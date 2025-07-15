import io
import pypowsybl
import pandas as pd
import logging
from common.decorators import performance_counter
from rao.loadflow_tool_settings import LF_PARAMETERS, CGMES_IMPORT_PARAMETERS
from common.helper import repackage_model_zip
from loguru import logger


class Optimizer:

    def __init__(self,
                 network: pypowsybl.network.Network,
                 crac: str | io.BytesIO,
                 debug: bool = False):

        self.network = network
        self.crac = crac
        self.debug = debug
        self.parameters = None
        self.results = None

        self.runner = pypowsybl.rao.create_rao()

    @property
    def cnec_results(self):
        return pd.json_normalize(self.results.to_json()['flowCnecResults'])

    @property
    def cost_results(self):
        return pd.json_normalize(self.results.to_json()['costResults'])

    def load_parameters(self, path: str = r"parameters_v30.json"):
        self.parameters = pypowsybl.rao.Parameters()
        logger.debug(f"Initalising with parameters: {path}")
        self.parameters.load_from_file_source(parameters_file=path)
        logger.debug(f"Parameters loaded from: {path}")

    def load_crac(self):
        if isinstance(self.crac, str):
            self.runner.set_crac_file_source(network=self.network, crac_file=self.crac)
        else:
            self.runner.set_crac_buffer_source(network=self.network, crac_source=self.crac)
        logger.debug(f"CRAC loaded from: {self.crac if isinstance(self.crac, str) else 'buffer'}")

    def clean_network_variants(self):
        self.network.set_working_variant("InitialState")
        variant_ids = self.network.get_variant_ids()
        for var in variant_ids[1:]:
            self.network.remove_variant(var)
            logger.debug(f"Removed network variant: {var}")

    def solve_loadflow(self):
        result = pypowsybl.loadflow.run_ac(network=self.network, parameters=LF_PARAMETERS)
        logger.info(f"Loadflow status: {result[0].status_text}")

        return result

    @performance_counter(units='seconds')
    def run(self):
        logger.debug(f"Starting the RAO, loading the parameters")
        self.load_parameters()
        self.load_crac()
        logger.info(f"Starting optimization")
        self.results = self.runner.run(self.network, parameters=self.parameters)
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
    # rao.results.serialize(r"test_output.json")

    # Clean network variants
    # rao.clean_network_variants()

    # Aggregate results
    cnec_results = rao.cnec_results
    cost_results = rao.cost_results
