from typing import Literal
from config.settings import AppSettings, ConsumerSettings


class RaoConsumerSettings(ConsumerSettings):
    consumer_type: Literal["SINGLE_MESSAGE", "LONG_LIVING"] = "SINGLE_MESSAGE"


class OperatorConsumerSettings(RaoConsumerSettings):
    rmq_queue_in: str = "rao.violations"


class OptimizerConsumerSettings(RaoConsumerSettings):
    rmq_queue_in: str = "rao.violations-optimization"


class RaoHandlerSettings(AppSettings):
    # Virtual operator
    optimize_only_current_violations: bool = True
    crac_use_limits_from_model: bool = True
    violation_threshold_percent: float = 100
    contingencies_count_threshold: int = 10
    s3_prepared_model_prefix: str = "RAO"
    s3_prepared_model_bucket: str = "opde-confidential-models"
    s3_crac_prefix: str = "RAO"
    s3_crac_bucket: str = "opde-confidential-analyses"
    rmq_queue_out: str = "rao.violations-optimization"
    # Optimizer
    elastic_results_index: str = "rao-results"
    elastic_voltage_results_index: str = "rao-voltage-results"
    debug: bool = False
