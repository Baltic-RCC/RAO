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
from typing import Any
from common.object_storage import ObjectStorage
from common.config_parser import parse_app_properties
from common.decorators import performance_counter
from rao.crac.builder import CracBuilder
from rao.crac.context import CracWorkaroundContext
from rao.parameters.manager import RaoSettingsManager
from rao.parameters.manager import LoadflowSettingsManager
from rao.optimizer import Optimizer
from rao.find_low_impedance_gap import find_threshold_gap_branches
from rao.voltage_monitoring import build_voltage_documents
from loguru import logger

parse_app_properties(caller_globals=globals(), path=config.paths.object_storage.object_storage)
parse_app_properties(caller_globals=globals(),
                     path=str(Path(__file__).parent.joinpath("config.properties")),
                     section="HANDLER_OPERATOR",
                     eval_types=True)
parse_app_properties(caller_globals=globals(),
                     path=str(Path(__file__).parent.joinpath("config.properties")),
                     section="HANDLER_OPTIMIZER",
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

        # Get default optimization parameters
        optimizer_settings = RaoSettingsManager()

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



    # Group by contingency id
    # TODO assess performance and consider to avoid groupby and only iterator over unique contingencies
    for mrid, data in violations.groupby("ContingencyPowerFlowResult.Contingency"):

        # Start the optimization
        optimizer = Optimizer(network=self.network,
                              crac_source=crac_object,
                              parameters_source=optimizer_settings.to_bytesio(),
                              loadflow_parameters=lf_settings_manager.build_pypowsybl_parameters(),
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
            logger.warning("Attempting to relax RAO parameters and re-optimize")

            rao_params_relaxed = json.load(optimizer_settings.to_bytesio())
            rao_params_relaxed["extensions"]["open-rao-search-tree-parameters"]["load-flow-and-sensitivity-computation"]["sensitivity-parameters"]["load-flow-parameters"]["useReactiveLimits"] = False

            if hasattr(crac_object, "seek"):
                crac_object.seek(0)
            optimizer = Optimizer(network=self.network,
                                  crac_source=crac_object,
                                  parameters_source=BytesIO(json.dumps(rao_params_relaxed).encode('utf-8')),
                                  debug=self.debug)
            optimizer.run()
            results = optimizer.results.to_json() if optimizer.results else None


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

        applied_actions = build_applied_actions(rao_results=results, crac=self.crac)
        logger.info(f"Post-processing results")
        results = self.post_process_results(results=pd.json_normalize(results))

        # Strip -Leg1/Leg2 from the 2w trafo CNECs that were replaced with the 3w-2w workaround
        violation_ids = set("_" + data['PowerFlowResult.ACDCTerminal'].astype(str))
        base_element_ids = (results["cnec.networkElementId"].astype(str).str.replace(r'-Leg\d+$', '', regex=True))

        # Flag CNECs which were identified as violations from received SAR profile
        results['cnec.sourceViolation'] = base_element_ids.isin(violation_ids)

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
        # Voltage monitoring results to Elastic
        if optimizer.voltage_monitoring_results is not None:
            voltage_results = optimizer.voltage_monitoring_results.get_voltage_cnec_results()
            if not voltage_results.empty:
                voltage_docs = build_voltage_documents(voltage_results=voltage_results,
                                                       crac=self.crac,
                                                       network=self.network,
                                                       headers=properties.headers,
                                                       applied_actions=applied_actions)

                logger.info(f"Sending {len(voltage_docs)} voltage monitoring results to Elastic")
                # Derive the document id so re-running a scenario overwrites its
                # results instead of adding a second copy of every document.
                self.object_storage.elastic_service.send_to_elastic_bulk(
                    index=ELASTIC_VOLTAGE_RESULTS_INDEX,
                    json_message_list=voltage_docs,
                    id_from_metadata=True,
                    id_metadata_list=['@time_horizon', '@scenario_timestamp', 'cnec_id', 'limit_type'],
                    hashing=True,
                )
    logger.success(f"Message handling completed successfully")

    return message, properties


class HandlerOptimizer:
    """Run the RAO for a single contingency task published by HandlerVirtualOperator."""

    def __init__(self, debug: bool = DEBUG):
        self.debug = debug
        self.object_storage = ObjectStorage()

    def _download(self, bucket: str, reference: str) -> BytesIO:
        content = self.object_storage.s3_service.download_object(bucket_name=bucket, object_name=reference)
        if content is None:
            raise FileNotFoundError(f"Object not found in bucket {bucket}: {reference}")
        buffer = BytesIO(content)
        buffer.name = reference
        return buffer

    def optimize(self, network, crac_object: BytesIO, loadflow_parameters):
        """Run RAO with each profile in order and return the first successful attempt.

        If all profiles fail, the last attempt is returned so its failure status still reaches Elastic.
        """
        optimizer, profile = None, None
        for profile in RAO_PROFILES:
            settings = RaoSettingsManager()
            settings.set(profile["overrides"])
            logger.info(f"Running optimization with RAO profile: {profile['name']}")

            optimizer = Optimizer(network=network,
                                  crac_source=crac_object,
                                  parameters_source=settings.to_bytesio(),
                                  loadflow_parameters=loadflow_parameters,
                                  debug=self.debug)
            optimizer.run()

            if optimizer.results is not None and optimizer.results.to_json()['computationStatus'] != 'failure':
                logger.info(f"Optimization succeeded with RAO profile: {profile['name']}")
                return optimizer, profile, settings
            logger.warning(f"Optimization failed with RAO profile: {profile['name']}")

        return optimizer, profile, settings

    @performance_counter(units='seconds')
    def post_process_results(self, results: pd.DataFrame):

        # Separate actions from CNEC results
        _cols_to_pop = ["networkActionResults", "rangeActionResults"]
        actions = results[_cols_to_pop]
        results = results.drop(columns=_cols_to_pop)

        # Transform dataframe from wide format to long by results type using melt
        # Voltage monitoring is logged after RAO and deliberately does not alter
        # the persisted flow-CNEC result schema.
        _cols_to_melt = ["flowCnecResults"]
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

    def publish_flow_results_to_elastic(self, results: pd.DataFrame):
        data_to_send = results.astype(object).where(pd.notna(results), None).to_dict("records")
        logger.info(f"Sending optimization results to Elastic index: {ELASTIC_RESULTS_INDEX}")
        self.object_storage.elastic_service.send_to_elastic_bulk(index=ELASTIC_RESULTS_INDEX,
                                                                 json_message_list=data_to_send)

    def publish_voltage_results_to_elastic(self, voltage_results: pd.DataFrame, applied_actions: list[dict]):
        voltage_docs = build_voltage_documents(voltage_results=voltage_results,
                                               crac=self.crac,
                                               network=self.network,
                                               headers=properties.headers,
                                               applied_actions=applied_actions)

        logger.info(f"Sending {len(voltage_docs)} voltage monitoring results to Elastic")
        # Derive the document id so re-running a scenario overwrites its
        # results instead of adding a second copy of every document.
        self.object_storage.elastic_service.send_to_elastic_bulk(
            index=ELASTIC_VOLTAGE_RESULTS_INDEX,
            json_message_list=voltage_docs,
            id_from_metadata=True,
            id_metadata_list=['@time_horizon', '@scenario_timestamp', 'cnec_id', 'limit_type'],
            hashing=True,
        )

    @performance_counter(units='seconds')
    def handle(self, message: bytes, properties: object, **kwargs):
        headers = properties.headers
        task = json.loads(message)
        contingency_id = task['contingency_id']
        violation_terminals = task['violation_terminals']

        message_id = headers.get('message-id', str(uuid.uuid4()))
        if getattr(config.initialize_logging, 'elastic_handler', None):
            config.initialize_logging.elastic_handler.extra.update({
                'message-id': message_id,
                'batch-id': headers.get('batch-id'),
                'contingency-id': contingency_id,
            })
        logger.info(f"Optimizing contingency: {contingency_id} (batch {headers.get('batch-id')})")

        # Load prepared network and CRAC built by the virtual operator
        network = pypowsybl.network.load_from_binary_buffer(
            buffer=self._download(task['bucket'], task['network_reference']))
        crac_object = self._download(task['bucket'], task['crac_reference'])
        crac = json.loads(crac_object.getvalue())

        lf_settings_manager = LoadflowSettingsManager(
            elastic_server=self.object_storage.elastic_service.server,
            elastic_api_key=self.object_storage.elastic_service.api_key,
            settings_keyword=task['loadflow_settings'],
        )

        optimizer, profile, settings = self.optimize(
            network=network,
            crac_object=crac_object,
            loadflow_parameters=lf_settings_manager.build_pypowsybl_parameters())

        if optimizer.results is None:
            logger.error(f"Optimizer has no results for contingency: {contingency_id}")
            return message, properties

        rao_results = optimizer.results.to_json()
        if not rao_results['networkActionResults'] and not rao_results['rangeActionResults']:
            logger.warning(f"No possible actions proposed by optimizer")
        for optimized_action in rao_results['networkActionResults']:
            logger.info(f"Optimized network action: {optimized_action}")
        for optimized_action in rao_results['rangeActionResults']:
            logger.info(f"Optimized range action: {optimized_action}")

        from rao.voltage_monitoring import build_applied_actions
        applied_actions = build_applied_actions(rao_results=rao_results, crac=crac)

        # Post-process the RAO result of one contingency and send it to Elastic
        logger.info(f"Post-processing results")
        results = self.post_process_results(results=pd.json_normalize(rao_results), crac=crac)

        # Strip -Leg1/Leg2 from the 2w trafo CNECs that were replaced with the 3w-2w workaround
        violation_ids = {f"_{terminal}" for terminal in violation_terminals}
        base_element_ids = results["cnec.networkElementId"].astype(str).str.replace(r'-Leg\d+$', '', regex=True)

        # Flag CNECs which were identified as violations from received SAR profile
        results['cnec.sourceViolation'] = base_element_ids.isin(violation_ids)

        # Include message properties and optimization main relevant settings as meta
        results['rmq'] = [headers] * len(results)
        results['settings'] = [settings] * len(results)

        # Send results to Elastic
        self.publish_flow_results_to_elastic(results=results)

        # Voltage monitoring results to Elastic
        if optimizer.voltage_monitoring_results is not None:
            voltage_results = optimizer.voltage_monitoring_results.get_voltage_cnec_results()
            if not voltage_results.empty:
                self.publish_voltage_results_to_elastic(voltage_results=voltage_results, applied_actions=applied_actions)

        logger.success(f"Optimization handling completed for contingency: {contingency_id}")

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
