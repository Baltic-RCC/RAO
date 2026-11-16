from pathlib import Path
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
    # Remedial action costs (YAML or JSON), unset: built-in costs of rao.crac.costs
    crac_costs_path: Path | None = None
    # Topology actions leaving a substation on <= 1 line at or above crac_weak_substation_min_kv (kV)
    # get the 'topologyWeakSubstation' activation cost
    crac_weak_substation_check: bool = True
    crac_weak_substation_min_kv: float = 330.0
    # Optimizer
    # RAO objective function (rao.parameters.profiles.OBJECTIVE_OVERRIDES), only MIN_COST uses the remedial action costs
    rao_objective: Literal["MIN_COST", "MAX_MIN_MARGIN"] = "MIN_COST"
    elastic_results_index: str = "rao-results"
    elastic_voltage_results_index: str = "rao-voltage-results"
    debug: bool = False
