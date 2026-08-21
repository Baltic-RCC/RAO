import os
import pypowsybl
import pandas as pd
import logging
from common.decorators import performance_counter
from rao.parameters.manager import LoadflowSettingsManager
from common.helper import repackage_model_zip
from loguru import logger
from io import BytesIO


class Optimizer:

    def __init__(self,
                 network: pypowsybl.network.Network,
                 crac_source: str | BytesIO,
                 parameters_source: str | BytesIO | None = None,
                 debug: bool = False,
                 ):

        self.network = network
        self.crac_source = crac_source
        self.parameters_source = parameters_source
        self.debug = debug

        self.crac = None
        self.parameters = None
        self.results = None
        self.voltage_monitoring_results = None

        self.runner = pypowsybl.rao.create_rao()

    @property
    def cnec_results(self):
        return pd.json_normalize(self.results.to_json()['flowCnecResults'])

    @property
    def cost_results(self):
        return pd.json_normalize(self.results.to_json()['costResults'])

    def load_parameters(self):
        """
        Loads optimization parameters from:
            - BytesIO buffer via load_from_buffer_source() (default for ID)
            - File path via load_from_file_source() (default for 1D)
        """

        if not self.parameters_source:
            from rao.parameters.manager import RaoSettingsManager
            optimizer_settings = RaoSettingsManager()
            self.parameters_source = optimizer_settings.to_bytesio()

        if isinstance(self.parameters_source, BytesIO):
            self.parameters_source.seek(0)
            logger.info("Loading parameters from in-memory BytesIO buffer")
            self.parameters = pypowsybl.rao.Parameters.from_buffer_source(self.parameters_source)
            logger.info("Parameters loaded successfully from in-memory BytesIO buffer")
        elif isinstance(self.parameters_source, str) or isinstance(self.parameters_source, os.PathLike):
            logger.info(f"Loading parameters from file: {self.parameters_source}")
            self.parameters = pypowsybl.rao.Parameters.from_file_source(parameters_file=str(self.parameters_source))
            logger.info(f"Parameters loaded successfully from: {self.parameters_source}")
        else:
            raise TypeError("Unsupported parameter source for load_parameters(): expected str or BytesIO")

    def load_crac(self):
        if isinstance(self.crac_source, (str, os.PathLike)):
            self.crac = pypowsybl.rao.Crac.from_file_source(network=self.network, crac_file=self.crac_source)
        elif isinstance(self.crac_source, BytesIO):
            self.crac_source.seek(0)
            self.crac = pypowsybl.rao.Crac.from_buffer_source(network=self.network, crac_source=self.crac_source)
        logger.debug(f"CRAC loaded from: {self.crac if isinstance(self.crac, str) else 'buffer'}")

    def clean_network_variants(self):
        self.network.set_working_variant("InitialState")
        variant_ids = self.network.get_variant_ids()
        for var in variant_ids[1:]:
            self.network.remove_variant(var)
            logger.debug(f"Removed network variant: {var}")

    def run_voltage_monitoring(self):
        """Run voltage monitoring on the network state produced by the base RAO.

        Voltage CNECs are monitoring-only constraints, so they are evaluated only
        after the optimizer has selected the remedial actions. Keep the monitoring
        result separate from ``self.results`` to preserve the existing downstream
        flow-CNEC result contract.
        """
        voltage_cnecs = self.crac.get_voltage_cnecs()
        if voltage_cnecs.empty:
            logger.info("No voltage CNECs in CRAC; skipping voltage monitoring")
            return

        logger.info(f"Running voltage monitoring for {len(voltage_cnecs)} voltage CNECs after RAO")
        self.voltage_monitoring_results = self.runner.run_voltage_monitoring(
            crac=self.crac,
            network=self.network,
            rao_result=self.results,
        )

        voltage_results = self.voltage_monitoring_results.get_voltage_cnec_results()
        if voltage_results.empty:
            logger.warning("Voltage monitoring completed without voltage CNEC results")
            return

        for cnec_id, result in voltage_results.iterrows():
            cnec = voltage_cnecs.loc[cnec_id] if cnec_id in voltage_cnecs.index else {}
            voltage_level_id = cnec.get("network_element_id", "unknown")
            voltage_level_name = cnec.get("name")
            voltage_level = (
                f"{voltage_level_name} ({voltage_level_id})"
                if pd.notna(voltage_level_name) else str(voltage_level_id)
            )
            tso = cnec.get("operator", "")
            tso_context = tso if pd.notna(tso) and str(tso).strip() else "unknown"
            state = result.get("optimized_instant", "unknown")
            contingency = result.get("contingency")
            state_context = f"{state}, contingency={contingency}" if pd.notna(contingency) else str(state)
            min_voltage = result.get("min_voltage")
            max_voltage = result.get("max_voltage")
            margin = result.get("margin")
            logger.info(
                f"Voltage after RAO for CNEC {cnec_id} at VoltageLevel {voltage_level} "
                f"[TSO={tso_context}; {state_context}]: "
                f"min={min_voltage} kV, max={max_voltage} kV, margin={margin} kV"
            )

    def solve_loadflow(self, elastic_server: str = None, settings_keyword: str = "BA_DEFAULT"):
        settings_manager = LoadflowSettingsManager(elastic_server=elastic_server, settings_keyword=settings_keyword)
        result = pypowsybl.loadflow.run_ac(network=self.network,
                                           parameters=settings_manager.build_pypowsybl_parameters())
        logger.info(f"Loadflow status: {result[0].status_text}")

        return result

    @performance_counter(units='seconds')
    def run(self):
        logger.debug(f"Starting the RAO, loading the parameters")
        self.load_parameters()
        self.load_crac()
        logger.info(f"Starting optimization")
        self.results = self.runner.run(crac=self.crac, network=self.network, parameters=self.parameters)
        self.run_voltage_monitoring()
        self.clean_network_variants()


if __name__ == '__main__':
    # Testing
    logging.getLogger('powsybl').setLevel(20)
    logging.getLogger('pypowsybl').setLevel(20)

    # Define the network
    network_path = r"../tests/RAO_TEST_CASE_OCO_LN425.zip"
    try:
        logger.info(f"Loading model from local directory: {network_path}")
        network = pypowsybl.network.load(network_path, parameters=CGMES_IMPORT_PARAMETERS)
        logger.info(f"Network model scenario time: {network.case_date}")
    except Exception as e:
        logger.warning(f"Loading failed with error: {e}, trying to re-package")
        network = pypowsybl.network.load_from_binary_buffer(buffer=repackage_model_zip(network_path),
                                                            parameters=CGMES_IMPORT_PARAMETERS)

    # Run RAO
    rao = Optimizer(network=network)
    lf_results = rao.solve_loadflow()
    rao.run()
    print(rao.results)
    rao.results.serialize(r"test_output.json")

    # Clean network variants
    # rao.clean_network_variants()

    # Aggregate results
    cnec_results = rao.cnec_results
    cost_results = rao.cost_results
