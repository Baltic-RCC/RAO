import io
import pypowsybl as pp
import pandas as pd
from datetime import datetime
import time
import logging
from rao.loadflow_tool_settings import LF_PARAMETERS, CGMES_IMPORT_PARAMETERS
from rao.helper import repackage_model_zip
from loguru import logger


class Optimizer:

    def __init__(self,
                 network: pp.network.Network,
                 crac: str | io.BytesIO):

        self.network = network
        self.crac = crac
        self.parameters = None
        self.results = None

        self.runner = pp.rao.create_rao()

    @property
    def cnec_results(self):
        return pd.json_normalize(self.results.to_json()['flowCnecResults'])

    @property
    def cost_results(self):
        return pd.json_normalize(self.results.to_json()['costResults'])

    def load_parameters(self, path: str = r"parameters_v30.json"):
        self.parameters = pp.rao.Parameters()
        self.parameters.load_from_file_source(parameters_file=path)

    def load_crac(self):
        if isinstance(self.crac, str):
            self.runner.set_crac_file_source(network=self.network, crac_file=self.crac)
        else:
            self.runner.set_crac_buffer_source(network=self.network, crac_source=self.crac)

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
        _start_time = time.time()
        self.load_parameters()
        self.load_crac()
        self.results = self.runner.run(self.network, parameters=self.parameters)
        logger.info(f"Optimization process duration: {time.time() - _start_time:.2f} seconds")


if __name__ == '__main__':
    # Testing
    logging.getLogger('powsybl').setLevel(20)

    # Define the network
    network_path = r"../tests/RAO_TEST_CASE_OCO_LN425.zip"
    try:
        logger.info(f"Loading model from local directory: {network_path}")
        network = pp.network.load(network_path, parameters=CGMES_IMPORT_PARAMETERS)
        logger.info(f"Network model scenario time: {network.case_date}")
    except Exception as e:
        logger.warning(f"Loading failed with error: {e}, trying to re-package")
        network = pp.network.load_from_binary_buffer(buffer=repackage_model_zip(network_path),
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
