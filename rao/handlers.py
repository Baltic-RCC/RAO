import uuid
import json
import itertools
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
import numpy as np
import pandas as pd
import pypowsybl
from pika import BasicProperties
from loguru import logger
import config
from integrations import rmq
from common.object_storage import ObjectStorage
from common.config_parser import parse_app_properties
from common.decorators import performance_counter
from rao.crac.builder import CracBuilder
from rao.crac.context import CracWorkaroundContext
from rao.find_low_impedance_gap import find_threshold_gap_branches, zero_out
from rao.parameters.manager import LoadflowSettingsManager

parse_app_properties(caller_globals=globals(), path=config.paths.object_storage.object_storage)
parse_app_properties(caller_globals=globals(),
                     path=str(Path(__file__).parent.joinpath("config.properties")),
                     section="HANDLER",
                     eval_types=True)


class HandlerVirtualOperator:

    def __init__(self, current_violations_only: bool = OPTIMIZE_ONLY_CURRENT_VIOLATIONS, debug: bool = DEBUG):

        self.current_violations_only = current_violations_only
        self.debug = debug
        self.network = None

        # Services initialization
        try:
            self.object_storage = ObjectStorage()
        except Exception as e:
            logger.error(f"Failed to initialize ObjectStorage service: {e}")

        # Metadata
        self.scenario_timestamp = None
        self.network_model_meta = None
        self.crac = None

    def get_input_profiles(self):
        requested_profile_types = ["CO", "AE", "RA"]
        received_profiles = self.object_storage.get_input_data_for_timestamp(type_keyword=requested_profile_types,
                                                                             scenario_timestamp=self.scenario_timestamp)

        if not received_profiles:
            logger.warning(f"[FALLBACK] Requesting all latest available input data files")
            received_profiles = self.object_storage.get_latest_available_input_data(
                type_keyword=requested_profile_types,
                scenario_timestamp=self.scenario_timestamp,
            )

        # Validate received profiles
        df = pd.DataFrame(received_profiles)
        if 'entity' in df.columns:
            expected = set(itertools.product(set(requested_profile_types), set(self.network_model_meta['included'])))
            received = set(zip(df['keyword'], df['entity']))
            missing = expected - received

            # Apply fallback
            for element in missing:
                logger.warning(f"[FALLBACK] Requesting latest available input data for: {element}")
                fallback_profile = self.object_storage.get_latest_available_input_data(
                    type_keyword=[element[0]],
                    scenario_timestamp=self.scenario_timestamp,
                    entity=[element[1]],
                )
                received_profiles.extend(fallback_profile)
        else:
            # Completeness is defined per TSO, so it cannot be established without
            # the entity. Proceed with whatever the object storage returned.
            logger.warning(f"Input profiles carry no entity, skipping per-TSO completeness check. "
                           f"Received profile types: {sorted(set(df['keyword']))}")

        return [profile['content'] for profile in received_profiles]

    def get_network_model(self, content_reference: str):
        # Query merge reports
        metadata = {'content_reference': content_reference}
        self.network_model_meta = self.object_storage.query(metadata_query=metadata, index=ELASTIC_MODELS_INDEX)[0]
        content = self.object_storage.get_content(metadata=self.network_model_meta, bucket_name=S3_BUCKET_IN_MODELS)

        return content

    def perform_low_impedance_workaround(self):
        """ WORKAROUND FOR LOW IMPEDANCE LINES"""
        # Threshold used to identify problematic lines with low impedance
        low_impedance_threshold = float(3.0E-5)  # From LF provider parameters lowImpedanceThreshold: '3.0E-5'
        # olf_default_low_impedance_threshold = float(1.0E-8) From default OLF parameters lowImpedanceThreshold: '1.0E-8'

        logger.info("[WORKAROUND] Performing lowImpedanceThreshold workaround for sensitivity analysis convergence")

        # Because branch impedance is calculated on PU mode, and current network variant is in normal unit mode, we must set network to PU mode temporarily to retrieve relevant r,x values
        self.network.per_unit = True
        all_lines_pu = self.network.get_lines(all_attributes=False,
                                              attributes=['r', 'x', 'connected1', 'connected2', 'fictitious'])
        all_2w_trafos_pu = self.network.get_2_windings_transformers(all_attributes=False,
                                                                    attributes=['r', 'x', 'connected1', 'connected2',
                                                                                'fictitious'])  # TODO NB! only side2 r,x values are retrieved using this function. Assume that they are problematic.
        # TODO 3w trafos would involve more work to calculate Z magnitude meeting low impedance threshold, as all legs need to be taken into account. For now not taken into account.
        # all_3w_trafos_pu = self.network.get_3_windings_transformers(all_attributes=True)

        # Filter relevant dfs for low impedance calculation threshold that are in service and not fictitious
        relevant_lines = all_lines_pu[
            (all_lines_pu['connected1']) & (all_lines_pu['connected2']) & (~all_lines_pu['fictitious'])].copy()
        relevant_2w_trafos = all_2w_trafos_pu[(all_2w_trafos_pu['connected1']) & (all_2w_trafos_pu['connected2']) & (
            ~all_2w_trafos_pu['fictitious'])].copy()

        # Calculate impedance magnitude |Z| = sqrt(r^2 + x^2)
        relevant_lines["z_abs"] = np.sqrt(relevant_lines["r"] ** 2 + relevant_lines["x"] ** 2)
        relevant_2w_trafos["z_abs"] = np.sqrt(relevant_2w_trafos["r"] ** 2 + relevant_2w_trafos["x"] ** 2)

        # For lines/2w trafos below the low impedance threshold, replace r and x values so that |Z| is around the low impedance threshold value
        low_impedance_lines = relevant_lines.index[relevant_lines["z_abs"] < low_impedance_threshold]
        low_impedance_2w_trafos = relevant_2w_trafos.index[relevant_2w_trafos["z_abs"] < low_impedance_threshold]

        if len(low_impedance_lines) > 0:
            # For short AC line segments typical X/R is ~10. We replace r and x values accordingly to match this logic for adequate P and Q distribution on relevant branches
            self.network.update_lines(id=low_impedance_lines, r=[2.8856078516e-6] * len(low_impedance_lines),
                                      x=[2.8856078516e-5] * len(low_impedance_lines))
            logger.info(f"[WORKAROUND] Replaced {len(low_impedance_lines)} low impedance line segment r/x values")

        if len(low_impedance_2w_trafos) > 0:
            # For transformers typical X/R is ~20. We replace r and x values accordingly to match this logic for adequate P and Q distribution on relevant branches
            self.network.update_2_windings_transformers(id=low_impedance_2w_trafos,
                                                        r=[4.9938e-6] * len(low_impedance_2w_trafos),
                                                        x=[9.9875e-5] * len(low_impedance_2w_trafos))
            logger.info(f"[WORKAROUND] Replaced {len(low_impedance_2w_trafos)} low impedance 2w transformer r/x values")

        # Reset network variant to non-PU mode
        self.network.per_unit = False

    @performance_counter(units='seconds')
    def handle(self, message: bytes, properties: object, **kwargs):
        """
        Process received SAR profile: prepare network model and CRACs, then fan out
        one optimization task per contingency to the optimizer workers
        """
        # Get unique x-message-id from headers, if not there - create
        message_id = properties.headers.get('message-id', str(uuid.uuid4()))
        if getattr(config.initialize_logging, 'elastic_handler', None):
            config.initialize_logging.elastic_handler.extra.update({
                'message-id': message_id,
                'source-module': properties.headers.get('source-module', 'unknown')
            })
        logger.info(f"Handling message with id: {message_id}")

        # Get metadata from properties
        self.scenario_timestamp = getattr(properties, 'headers').get('scenario-time', datetime.now(timezone.utc))
        if isinstance(self.scenario_timestamp, str):
            self.scenario_timestamp = datetime.fromisoformat(self.scenario_timestamp)

        # Store SAR to BytesIO object and load to triplets to scan violations
        sar = BytesIO(message)
        sar.name = f"{getattr(properties, 'headers').get('project-name', 'undefined')}.xml"
        logger.info(f"Loading received SAR profile")
        sar_data = pd.read_RDF([sar])
        for key, value in sar_data.types_dict().items():
            logger.debug(f"Loaded objects: {value} {key}")

        # Get all violations from SAR profile
        violations = sar_data.key_tableview("PowerFlowResult.isViolation")
        violations = violations[violations["PowerFlowResult.isViolation"] == "true"]

        # Filter by violation deadband
        violations = violations[violations["PowerFlowResult.value"] >= VIOLATION_THRESHOLD_PERCENT]

        # Filter to current violations only if defined by configuration
        if self.current_violations_only:
            if 'PowerFlowResult.valueA' in violations.columns:
                violations = violations[violations['PowerFlowResult.valueA'].notna()]
            else:
                violations = pd.DataFrame()

        # Exit if there is no relevant violations
        if violations.empty:
            logger.info("No violations found in SAR profile, exiting VirtualOperator process")
            return message, properties
        else:
            logger.info(f"SAR profile contains number of relevant violations: {len(violations)}")

        # Exit if there are more unique contingencies to be optimized than configured limit
        if 'ContingencyPowerFlowResult.Contingency' in violations.columns:
            _contingencies_count = len(violations['ContingencyPowerFlowResult.Contingency'].unique())
            logger.info(f"SAR profile contains number of unique contingencies: {_contingencies_count}")
            if _contingencies_count > CONTINGENCIES_COUNT_THRESHOLD:
                logger.error("Number of unique contingencies is above threshold and message can not be processed")
                return message, properties

        # Get network model from object storage
        content_reference = properties.headers.get('content-reference', None)
        if not content_reference:
            logger.error(f"RMQ message does not have content reference in headers")
            return message, properties
        network_object = self.get_network_model(content_reference=content_reference)

        # Determine used loadflow settings during model merge
        try:
            _loadflow_settings_key = self.network_model_meta["loadflow_settings"]
            logger.info(f"Loadflow settings defined by received merged model: {_loadflow_settings_key}")
        except KeyError:
            logger.warning(f"Loadflow settings not defined by received merged model, using default: BA_DEFAULT")
            _loadflow_settings_key = "BA_DEFAULT"

        lf_settings_manager = LoadflowSettingsManager(
            elastic_server=self.object_storage.elastic_service.server,
            elastic_api_key=self.object_storage.elastic_service.api_key,
            settings_keyword=_loadflow_settings_key,
        )

        logger.info(f"Loading network model to pypowsybl")
        self.network = pypowsybl.network.load_from_binary_buffer(
            buffer=network_object,
            parameters=lf_settings_manager.config['CGMES_IMPORT_PARAMETERS'])

        # TODO Temporary: Replace 3w transformers with 3 x 2w transformers in the network
        three_w_trafos = self.network.get_3_windings_transformers()
        # Replace only 3w transformers that are XNEs
        three_w_trafos_to_replace = three_w_trafos.index[ three_w_trafos[ "rated_u1" ] >= 330 ].tolist()
        for tid in three_w_trafos_to_replace:
            if tid not in self.network.get_3_windings_transformers().index:
                logger.warning(f"Skipping {tid}: deleted from network [removed eq error not solved]")
                continue
            try:
                pypowsybl.network.replace_3_windings_transformers_with_3_2_windings_transformers(self.network, [tid])
            except Exception as e:
                logger.error(f"Failed to replace {tid}: {e}")
        logger.info("[TEMPORARY] Replaced 3w transformers with 3 x 2w transformers in the network")
        two_w_trafos = self.network.get_2_windings_transformers()
        replaced_ids = set(three_w_trafos_to_replace)
        replaced_3w_trafos = two_w_trafos[ two_w_trafos.index.str.split("-Leg", n=1).str[ 0 ].isin(replaced_ids) ]

        workaround_ctx = CracWorkaroundContext(
            enable_3w_trafo_replacement=True,
            replaced_3w_trafos=replaced_3w_trafos
        )

        # Solve initial loadflow on retrieved model
        logger.info(f"Solve initial loadflow analysis")
        lf_result = pypowsybl.loadflow.run_ac(
            network=self.network,
            parameters=lf_settings_manager.build_pypowsybl_parameters())
        logger.info(f"Loadflow status: {lf_result[0]}")
        if lf_result[0].status.value:
            logger.error(f"Initial load flow computation failed, exiting message handling")
            return message, properties

        # Scan for low impedance gaps in the model (for lines, trafos) and zero them out.
        # Network-wide, so done once here instead of per contingency
        gap, neg_x, _ = find_threshold_gap_branches(self.network, lo=1e-8, hi=3e-5)
        logger.info(f"LOW-Z {len(gap)} elements in [1e-8 - 3e-5] pu: {gap.groupby('kind').size().to_dict()}")
        for eid, row in gap.iterrows():
            logger.info(f"LOW-Z {row.kind} {eid}: r={row.r:.3e} x={row.x:.3e} |z|={row.z_pu:.3e}")
        zero_out(self.network, gap)

        # Get other input data from object storage
        input_file_objects = self.get_input_profiles()

        # Load input files and SAR to triplets
        logger.info(f"Loading additional input data")
        input_files_data = pd.read_RDF(input_file_objects)
        for key, value in input_files_data.types_dict().items():
            logger.debug(f"Loaded objects: {value} {key}")

        # Create CRAC service
        logger.info(f"Loading network to triplets for CRAC service")
        network_triplets = pd.read_RDF(network_object)
        crac_service = CracBuilder(data=input_files_data, network=network_triplets, workaround=workaround_ctx)
        crac_service.get_limits()  # get limits from model and store in CRAC service object

        # Store prepared network (with workarounds applied) so optimizer workers do not repeat them
        batch_id = str(uuid.uuid4())
        file_prefix = f"{properties.headers['time-horizon']}_{self.scenario_timestamp:%Y%m%dT%H%M}"
        network_prepared = self.network.save_to_binary_buffer(format="XIIDM")
        network_prepared.name = f"{S3_PREFIX_PREPARED}/NETWORK_{file_prefix}_{batch_id}.zip"
        self.object_storage.s3_service.upload_object(file_path_or_file_object=network_prepared,
                                                     bucket_name=S3_BUCKET_RESULTS,
                                                     metadata=properties.headers)

        # Build CRAC per contingency and publish one optimization task for each
        contingencies = violations.groupby("ContingencyPowerFlowResult.Contingency")
        publisher = rmq.BlockingClient()
        try:
            for mrid, data in contingencies:
                logger.info(f"Processing contingency: {mrid} with {len(data)} violations")
                logger.info(f"Violations on network elements: {data['PowerFlowResult.EquipmentName'].to_list()}")

                crac = crac_service.build_crac(contingency_ids=[mrid])
                crac_object = BytesIO(json.dumps(crac, ensure_ascii=False, indent=2).encode('utf-8'))
                crac_object.name = f"RAO/CRAC_{file_prefix}_CO_{mrid}.json"
                self.object_storage.s3_service.upload_object(file_path_or_file_object=crac_object,
                                                             bucket_name=S3_BUCKET_RESULTS,
                                                             metadata=properties.headers)

                task = {
                    'contingency_id': mrid,
                    'bucket': S3_BUCKET_RESULTS,
                    'network_reference': network_prepared.name,
                    'crac_reference': crac_object.name,
                    'loadflow_settings': _loadflow_settings_key,
                    'violation_terminals': data['PowerFlowResult.ACDCTerminal'].astype(str).tolist(),
                }
                headers = {
                    **properties.headers,
                    'contingency-id': mrid,
                    'batch-id': batch_id,
                    'batch-size': contingencies.ngroups,
                }
                publisher.publish(payload=json.dumps(task),
                                  exchange_name=RMQ_EXCHANGE_OPTIMIZATION,
                                  headers=headers,
                                  delivery_mode=2)
                logger.info(f"Optimization task published for contingency: {mrid}")
        finally:
            publisher.close()

        logger.success(f"Message handling completed, published {contingencies.ngroups} optimization tasks")

        return message, properties


if __name__ == '__main__':
    # Define RMQ test message
    headers = {
        "baCorrelationID": f"{uuid.uuid4()}",
        "baMessageID": f"{uuid.uuid4()}",
        "businessType": "CSA-INPUT",
        "messageID": f"{uuid.uuid4()}",
        "sendTimestamp": datetime.utcnow().isoformat(),
        "sender": "TSOX",
        "senderApplication": "APPX",
        "service": "INPUT-DATA",
        "scenario-time": datetime(2026, 9, 23, 22, 30),
        "time-horizon": "ID",
        "content-reference": "EMFOS/RMM/ID/RMM_15_002_20260923T2030Z_BA_44a93f61-0158-483d-8cfc-dfe8959099ec.zip",
    }
    properties = BasicProperties(
        content_type='application/octet-stream',
        delivery_mode=2,
        priority=4,
        message_id=f"{uuid.uuid4()}",
        timestamp=1747208205,
        headers=headers,
    )
    with open(r"C:\Users\lukas.navickas\Documents\test_data_rao\SAR_20260923T2230_ID.xml", "rb") as file:
        file_bytes = file.read()

    # Create instance
    service = HandlerVirtualOperator()
    result = service.handle(message=file_bytes, properties=properties)

    # Test input data
    # contingencies = r"../test-data/TC1_contingencies.xml"
    # assessed_elements = r"../test-data/TC1_assessed_elements.xml"
    # remedial_actions = r"../test-data/TC1_remedial_actions.xml"
