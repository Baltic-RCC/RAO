import os
import uuid
from datetime import datetime, timezone
from pika import BasicProperties
from io import BytesIO
import pandas as pd
import numpy as np
import json
import pypowsybl
import itertools
import config
from pathlib import Path
from common.object_storage import ObjectStorage
from common.config_parser import parse_app_properties
from common.decorators import performance_counter
from rao.crac.builder import CracBuilder
from rao.parameters.manager import RaoSettingsManager
from rao.parameters.manager import LoadflowSettingsManager
from rao.optimizer import Optimizer
from loguru import logger


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

        return [profile['content'] for profile in received_profiles]

    def get_network_model(self, content_reference: str):
        # Query merge reports
        metadata = {'content_reference': content_reference}
        self.network_model_meta = self.object_storage.query(metadata_query=metadata, index=ELASTIC_MODELS_INDEX)[0]
        content = self.object_storage.get_content(metadata=self.network_model_meta, bucket_name=S3_BUCKET_IN_MODELS)

        return content

    @performance_counter(units='seconds')
    def post_process_results(self, results: pd.DataFrame):

        # Separate actions from CNEC results
        _cols_to_pop = ["networkActionResults", "rangeActionResults"]
        actions = results[_cols_to_pop]
        results = results.drop(columns=_cols_to_pop)

        # Transform dataframe from wide format to long by results type using melt
        _cols_to_melt = ["flowCnecResults"] # TODO to update when voltageCNEC and angleCNEC results are printed
        results = results.melt(id_vars=[col for col in results.columns if col not in _cols_to_melt],
                               value_vars=_cols_to_melt,
                               var_name='cnecResultsType',
                               value_name='cnecResults')

        # Drop CNEC result types where it is empty
        results = results.explode(column=["cnecResults"]).dropna(subset=["cnecResults"])
        results = pd.json_normalize(results.to_dict("records"))

        # Map CNEC data
        cnec_df = pd.DataFrame(self.crac['flowCnecs'])
        cnec_df.columns = [f"cnec.{col}" for col in cnec_df.columns]
        results = results.merge(cnec_df, how='left', left_on='cnecResults.flowCnecId', right_on='cnec.id').drop(columns='cnec.id')

        # Map contingency data
        contingency_df = pd.DataFrame(self.crac['contingencies'])
        if not contingency_df.empty:
            contingency_df.columns = [f"contingency.{col}" for col in contingency_df.columns]
            results = results.merge(contingency_df,
                                    how='left',
                                    left_on='cnec.contingencyId',
                                    right_on='contingency.id').drop(columns='contingency.id')

        # Normalize thresholds
        results = pd.json_normalize(results.explode("cnec.thresholds").to_dict('records'))

        # Explode and flatten network actions
        ## Check if there are any actions received from optimizer
        _optimized_actions_flag = bool(actions.apply(lambda col: col.map(lambda x: x != [])).values.any())
        if _optimized_actions_flag:
            actions = pd.json_normalize(actions['networkActionResults'].explode())
            actions = pd.json_normalize(actions.explode("activatedStates").to_dict("records"))

            # Combine dataframes
            results = results.merge(actions,
                                    how='left',
                                    left_on=["cnec.instant", "cnec.contingencyId"],
                                    right_on=["activatedStates.instant", "activatedStates.contingency"])

            # Map network action data
            action_df = pd.DataFrame(self.crac['networkActions'])
            if not action_df.empty:
                action_df.columns = [f"action.{col}" for col in action_df.columns]
                results = results.merge(action_df,
                                        how='left',
                                        left_on='networkActionId',
                                        right_on='action.id').drop(columns='action.id')

        # TODO - explode by optimized network actions
        # results = results.explode("action.terminalsConnectionActions")

        # Calculate CNEC loading percentages
        _min_thresholds = results['cnec.thresholds.min']
        _max_thresholds = results['cnec.thresholds.max']
        _unit = results['cnec.thresholds.unit']
        for col in results.filter(regex=r'^cnecResults\.[^.]+\.[^.]+\.side1\.flow$').columns:
            _unit_key = col.split(".")[2]  # -> "ampere" / "megawatt"
            _loading_col_name = col.replace("flow", "loading")
            # choose + or - limit based on sign of the flow
            _denominator = np.where(results[col].ge(0), _max_thresholds, _min_thresholds)
            # compute only for matching unit and non-zero limit
            _mask = (_unit == _unit_key) & (_denominator != 0)
            results[_loading_col_name] = np.where(_mask, results[col] / _denominator, np.nan)

        return results

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
        Process received SAR profile
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

        # Solve initial loadflow on retrieved model
        logger.info(f"Solve initial loadflow analysis")
        lf_result = pypowsybl.loadflow.run_ac(
            network=self.network,
            parameters=lf_settings_manager.build_pypowsybl_parameters())
        logger.info(f"Loadflow status: {lf_result[0]}")
        if lf_result[0].status.value:
            logger.error(f"Initial load flow computation failed, exiting message handling")
            return message, properties

        # Get other input data from object storage
        input_file_objects = self.get_input_profiles()

        # Load input files and SAR to triplets
        logger.info(f"Loading additional input data")
        input_files_data = pd.read_RDF(input_file_objects)
        for key, value in input_files_data.types_dict().items():
            logger.debug(f"Loaded objects: {value} {key}")

        # Get default optimization parameters
        optimizer_settings = RaoSettingsManager()

        # Create CRAC service
        logger.info(f"Loading network to triplets for CRAC service")
        network_triplets = pd.read_RDF(network_object)
        crac_service = CracBuilder(data=input_files_data, network=network_triplets)
        crac_service.get_limits()  # get limits from model and store in CRAC service object

        # Group by contingency id
        # TODO assess performance and consider to avoid groupby and only iterator over unique contingencies
        for mrid, data in violations.groupby("ContingencyPowerFlowResult.Contingency"):

            logger.info(f"Processing contingency: {mrid} with {len(data)} violations")
            logger.info(f"Violations on network elements: {data['PowerFlowResult.EquipmentName'].to_list()}")

            # Build CRAC for each contingency
            self.crac = crac_service.build_crac(contingency_ids=[mrid])

            # For debugging
            with open("test-crac.json", "w") as f:
                json.dump(self.crac, f, ensure_ascii=False, indent=4)

            # Store built CRAC files in S3 storage
            crac_object = BytesIO(json.dumps(self.crac).encode('utf-8'))
            crac_object.name = f"RAO/CRAC_{properties.headers['time-horizon']}_{self.scenario_timestamp:%Y%m%dT%H%M}_CO_{mrid}.json"
            self.object_storage.s3_service.upload_object(file_path_or_file_object=crac_object,
                                                         bucket_name=S3_BUCKET_RESULTS,
                                                         metadata=properties.headers)

            # Perform lowImpedanceThreshold parameter workaround
            self.perform_low_impedance_workaround()

            # Start the optimization
            optimizer = Optimizer(network=self.network,
                                  crac=crac_object,
                                  parameters_source=optimizer_settings.to_bytesio(),
                                  debug=self.debug)
            optimizer.run()
            logger.info(f"Optimization finished for contingency: {mrid}")

            # Check optimizer results
            if optimizer.results is None:
                logger.warning("Optimizer has no results to be processed")
                continue

            # Serialize results to json
            results = optimizer.results.to_json()
            if results['computationStatus'] == 'failure':
                logger.error(f"Optimizer failed computation: {results}")
                logger.error(f"Enable pypowsybl logs for more information")
                continue

            # Check if there are any optimized remedial actions
            if not results['networkActionResults'] and not results['rangeActionResults']:
                logger.warning(f"No possible actions proposed by optimizer")
            else:
                for optimized_action in results['networkActionResults']:
                    logger.info(f"Optimized network action: {optimized_action}")
                    _details = [x for x in self.crac['networkActions'] if x['id'] == optimized_action['networkActionId']]
                    logger.info(f"Action details: {_details[0]}")
                for optimized_action in results['rangeActionResults']:
                    # TODO print out action details
                    logger.info(f"Optimized range action: {optimized_action}")

            # Post-process optimizer results
            logger.info(f"Post-processing results")
            results = self.post_process_results(results=pd.json_normalize(results))

            # Flag CNECs which were identified as violations from received SAR profile
            results['cnec.sourceViolation'] = results['cnec.networkElementId'].isin(data['PowerFlowResult.ACDCTerminal'].apply(lambda x: f"_{x}"))

            # Logging status of successful optimization process for contingency
            logger.success(f"Optimization successful for contingency: {mrid}")

            # Include message properties as meta
            results['rmq'] = [properties.headers] * len(results)

            # Include optimization main relevant settings
            results['settings'] = [{"objective-function": optimizer_settings.get("objective-function.type")}] * len(results)

            # Send results to Elastic
            data_to_send = results.astype(object).where(pd.notna(results), None).to_dict("records")
            logger.info(f"Sending optimization results to Elastic index: {ELASTIC_RESULTS_INDEX}")
            self.object_storage.elastic_service.send_to_elastic_bulk(
                index=ELASTIC_RESULTS_INDEX,
                json_message_list=data_to_send,
            )

        logger.success(f"Message handling completed successfully")

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
        "scenario-time": datetime(2026, 4, 26, 18, 30),
        "time-horizon": "ID",
        "content-reference": "EMFOS/RMM/ID/RMM_12_002_20260417T1730Z_BA_547bf8be-49a1-4eb1-a1f4-2ea10c841722.zip",
    }
    properties = BasicProperties(
        content_type='application/octet-stream',
        delivery_mode=2,
        priority=4,
        message_id=f"{uuid.uuid4()}",
        timestamp=1747208205,
        headers=headers,
    )
    with open(r"C:\Users\lukas.navickas\Documents\test_data_rao\test_litgrid_prod_model\SAR_20260504T1430_ID_1_637ec819-3e81-426b-bd67-a6fa813662c3.xml", "rb") as file:
        file_bytes = file.read()

    # Create instance
    service = HandlerVirtualOperator()
    result = service.handle(message=file_bytes, properties=properties)

    # Test input data
    # contingencies = r"../test-data/TC1_contingencies.xml"
    # assessed_elements = r"../test-data/TC1_assessed_elements.xml"
    # remedial_actions = r"../test-data/TC1_remedial_actions.xml"
