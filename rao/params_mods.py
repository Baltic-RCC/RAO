from rao.params_utils import ParameterOverride
from pathlib import Path

def get_parameter_override_stream(time_horizon: str) -> ParameterOverride | None:

    """
    Parameters modification function, used to define overrides in the parameters for specific business cases.
    If ID time horizon, maximum curative search tree depth is limited to 1 (default is 2)
    """

    if time_horizon != "ID":
        return None

    original_path = Path(__file__).parent / "parameters_v30.json"
    keys_path = [
        "extensions",
        "open-rao-search-tree-parameters",
        "topological-actions-optimization",
        "max-curative-search-tree-depth"
    ]
    new_value = 1

    return ParameterOverride(original_path, keys_path, new_value)