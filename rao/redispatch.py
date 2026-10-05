"""
MIN_COST RAO run helpers for local redispatching (pypowsybl 1.16.1 / OpenRAO 7.3.0).

The CRAC is built with rao.crac.redispatch. Known OpenRAO 7.3.0 requirements for MIN_COST:
    - 'costly-min-margin-parameters' must be set, otherwise the run fails
    - 'pst-model' must be APPROXIMATED_INTEGERS
    - pypowsybl's Python Parameters() object cannot set the costly block, so the
      parameters are always loaded from the JSON file (see rao_v34_min_cost.json)
"""
from pathlib import Path
import pandas as pd
import pypowsybl
from loguru import logger
from rao.crac.redispatch.builder import import_crac
from rao.parameters.manager import RaoSettingsManager

MIN_COST_PARAMETERS_PATH = Path(__file__).parent.joinpath("parameters", "rao_v34_min_cost.json")
_DC_KEY = ("extensions.open-rao-search-tree-parameters.load-flow-and-sensitivity-computation."
           "sensitivity-parameters.load-flow-parameters.dc")

RESULT_COLUMNS = ["action_id", "generator_id", "instant", "contingency", "initial_p", "optimized_p", "delta"]


def load_min_cost_parameters(path: str | Path = MIN_COST_PARAMETERS_PATH,
                             dc: bool = True,
                             overrides: dict | None = None) -> pypowsybl.rao.Parameters:
    """
    Load MIN_COST RAO parameters from JSON.

    Args:
        path: parameters JSON (version 3.4)
        dc: run load flow and sensitivity computation in DC (True) or AC (False)
        overrides: optional {"dot.separated.key": value} overrides
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"RAO parameters file not found: {path}")
    settings = RaoSettingsManager(default_path=path, use_env_override=False)
    settings.set(_DC_KEY, dc)
    if overrides:
        settings.set(overrides)
    logger.info(f"Loading MIN_COST RAO parameters from {path} ({'DC' if dc else 'AC'})")
    return pypowsybl.rao.Parameters.from_buffer_source(settings.to_bytesio())


def run_rao(network: pypowsybl.network.Network,
            crac,
            parameters: pypowsybl.rao.Parameters | None = None) -> pypowsybl.rao.RaoResult:
    """
    Run the RAO. The crac may be an imported pypowsybl Crac or a CRAC dict; parameters
    default to the MIN_COST parameters in DC. The network is not modified.
    """
    if isinstance(crac, dict):
        crac = import_crac(network, crac)
    if parameters is None:
        parameters = load_min_cost_parameters()
    logger.info("Starting redispatch RAO")
    result = pypowsybl.rao.create_rao().run(crac=crac, network=network, parameters=parameters)
    logger.info(f"RAO finished with status: {result.status()}")
    return result


def redispatch_results(crac,
                       rao_result: pypowsybl.rao.RaoResult,
                       network: pypowsybl.network.Network,
                       balance_tolerance: float = 1.0) -> pd.DataFrame:
    """
    Optimized set-points of the injection range actions per state.

    The set-point is absolute MW because every action has a single element with key 1.0.
    Deltas per (instant, contingency) state should sum to ~0 (balanced redispatch); a
    warning is logged otherwise.
    """
    injection_actions = crac.get_injection_range_actions()
    elements = crac.get_network_element_ids_and_keys()
    elements = elements[elements.index.isin(injection_actions.index)]
    multi = elements.index[elements.index.duplicated()].unique()
    if len(multi) or not (elements["distribution_key"] == 1.0).all():
        raise ValueError(f"Only single-element injection range actions with key 1.0 are supported: "
                         f"{sorted(set(multi) | set(elements.index[elements['distribution_key'] != 1.0]))}")
    generator_ids = elements["network_element_id"]

    results = rao_result.get_range_action_results()
    results = results[results["remedial_action_id"].isin(injection_actions.index)]
    if results.empty:
        logger.info("No redispatch range action activated")
        return pd.DataFrame(columns=RESULT_COLUMNS)

    frame = pd.DataFrame({
        "action_id": results["remedial_action_id"].values,
        "generator_id": results["remedial_action_id"].map(generator_ids).values,
        "instant": results["optimized_instant"].values,
        "contingency": results["contingency"].values,
        "initial_p": results["remedial_action_id"].map(injection_actions["initial_set_point"]).values,
        "optimized_p": results["optimized_set_point"].values,
    })
    frame["delta"] = frame["optimized_p"] - frame["initial_p"]

    # The RaoResult never modifies the network, so target_p should still be the initial set-point
    target_p = network.get_generators(attributes=["target_p"])["target_p"]
    changed = frame[(frame["generator_id"].map(target_p) - frame["initial_p"]).abs() > 1e-6]
    if not changed.empty:
        logger.warning(f"Network target_p differs from the CRAC initial set-point for "
                       f"{sorted(changed['generator_id'].unique())}, was redispatch already applied to this network?")

    for (instant, contingency), state in frame.groupby(["instant", "contingency"], dropna=False):
        imbalance = state["delta"].sum()
        if abs(imbalance) > balance_tolerance:
            logger.warning(f"Redispatch at state {instant}/{contingency or '-'} is not balanced: "
                           f"sum of deltas = {imbalance:.3f} MW")

    return frame.sort_values(["instant", "contingency", "action_id"]).reset_index(drop=True)[RESULT_COLUMNS]


def apply_redispatch(network: pypowsybl.network.Network,
                     results: pd.DataFrame,
                     instant: str,
                     contingency: str = "") -> pd.DataFrame:
    """
    Apply the optimized set-points of one state to the network generators.

    The RaoResult never modifies the network. For a curative state the contingency itself
    is not applied here, only the generator set-points. Returns the applied rows.
    """
    state = results[(results["instant"] == instant) & (results["contingency"].fillna("") == (contingency or ""))]
    if state.empty:
        logger.info(f"No redispatch to apply at state {instant}/{contingency or '-'}")
        return state
    network.update_generators(id=state["generator_id"].tolist(), target_p=state["optimized_p"].tolist())
    logger.info(f"Applied redispatch of {len(state)} generators at state {instant}/{contingency or '-'}")
    return state
