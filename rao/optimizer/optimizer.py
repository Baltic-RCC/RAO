import pypowsybl as pp
from typing import Dict
import logging
import sys
import pandas as pd
from datetime import datetime
import time
from loadflow_settings import LF_PARAMETERS
from helper import repackage_model_zip


# Start logger
logger = logging.getLogger(__name__)


class Optimizer:

    CGMES_IMPORT_PARAMETERS: Dict[str, str] = {"iidm.import.cgmes.source-for-iidm-id": "rdfID",
                                               "iidm.import.cgmes.import-node-breaker-as-bus-breaker": "True"}

    def __init__(self,
                 network: pp.network.Network | str):

        self.network = network
        self.parameters = None
        self.results = None

        self.runner = pp.rao.create_rao()

        # Input argument 'network' can be pypowsybl network instance or path to local file
        if isinstance(self.network, str):
            try:
                logger.info(f"Loading model from local directory: {self.network}")
                self.network = pp.network.load(self.network, parameters=self.CGMES_IMPORT_PARAMETERS)
                logger.info(f"Network model scenario time: {self.network.case_date}")
            except Exception as e:
                logger.warning(f"Loading failed with error: {e}, trying to re-package")
                self.network = pp.network.load_from_binary_buffer(buffer=repackage_model_zip(self.network),
                                                                  parameters=self.CGMES_IMPORT_PARAMETERS)

    @property
    def cnec_results(self):
        return pd.json_normalize(self.results.to_json()['flowCnecResults'])

    @property
    def cost_results(self):
        return pd.json_normalize(self.results.to_json()['costResults'])

    def load_parameters(self, path: str = r"parameters.json"):
        self.parameters = pp.rao.Parameters()
        self.parameters.load_from_file_source(parameters_file=path)

    def load_crac(self, path: str = r"../crac/common_baltic_crac.json"):
        self.runner.set_crac_file_source(network=self.network, crac_file=path)

    def clean_network_variants(self):
        self.network.set_working_variant("InitialState")
        variant_ids = self.network.get_variant_ids()
        for var in variant_ids[1:]:
            self.network.remove_variant(var)

    def solve_loadflow(self):
        result = pp.loadflow.run_ac(network=self.network, parameters=LF_PARAMETERS)
        logger.info(f"Loadflow status: {result[0].status_text}")

        return result

    def run(self):
        self.load_parameters()
        _start_time = time.time()
        self.results = self.runner.run(self.network, parameters=self.parameters)
        logger.info(f"Optimization process duration: {time.time() - _start_time:.2f} seconds")


if __name__ == '__main__':
    # Testing
    logging.basicConfig(stream=sys.stdout,
                        format="%(levelname) -10s %(asctime) -10s %(name) -35s %(funcName) -35s %(lineno) -5d: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S",
                        level=20,
                        )
    logging.getLogger('powsybl').setLevel(20)

    # Define the network
    network_path = r"../tests/RAO_TEST_CASE_OCO_LN425.zip"

    # Run RAO
    rao = Optimizer(network=network_path)
    lf_results = rao.solve_loadflow()
    rao.load_crac()
    rao.run()
    print(rao.results)
    # rao.results.serialize(r"test_output.json")

    # Clean network variants
    # rao.clean_network_variants()

    # Aggregate results
    cnec_results = rao.cnec_results
    cost_results = rao.cost_results
