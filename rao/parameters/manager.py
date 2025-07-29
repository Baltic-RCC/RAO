import json
import os
from io import BytesIO
from pathlib import Path
from copy import deepcopy
import pypowsybl
from loguru import logger

class RaoSettingsManager:

    RAO_PARAMETERS_VERSION_MAP = {
        "1.11.0": f"{Path(__file__).parent.joinpath('rao_v30.json')}",
        "1.12.0": f"{Path(__file__).parent.joinpath('rao_v31.json')}",
    }

    def __init__(self):
        self.default_path = Path(self.RAO_PARAMETERS_VERSION_MAP.get(pypowsybl.__version__, None))
        if not self.default_path:
            raise ValueError(f"Unsupported version to get parameters: {pypowsybl.__version__}")
        override_env = os.environ.get("RAO_CONFIG_OVERRIDE_PATH")
        self.override_path = Path(override_env) if override_env else None
        self.config = self._load_and_merge()

    def _load_json(self, path: Path) -> dict:
        if path and path.exists():
            logger.debug(f"Loading JSON config from: {path}")
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)

        return {}

    def _load_and_merge(self) -> dict:
        base = self._load_json(path=self.default_path)
        override = self._load_json(self.override_path) if self.override_path else {}

        return self._deep_merge(base, override)

    def _deep_merge(self, a: dict, b: dict) -> dict:
        result = deepcopy(a)
        for k, v in b.items():
            if isinstance(v, dict) and isinstance(result.get(k), dict):
                result[k] = self._deep_merge(result[k], v)
            else:
                result[k] = deepcopy(v)

        return result

    def _set_single(self, path: str, value):
        keys = path.split(".")
        d = self.config
        for k in keys[:-1]:
            d = d.setdefault(k, {})
        d[keys[-1]] = value


    def get(self, path: str, default = None):
        keys = path.split(".")
        val = self.config
        for k in keys:
            val = val.get(k, default) if isinstance(val, dict) else default

        return val

    def set(self, path_or_dict, value = None):
        """
        Set a single nested config value or multiple values at once.
        - Single: set("api.timeout", 30)
        - Multiple: set({"api.timeout": 30, "features.login": False})
        """
        if isinstance(path_or_dict, dict):
            for path, val in path_or_dict.items():
                self._set_single(path, val)
        else:
            self._set_single(path_or_dict, value)

    def to_bytesio(self) -> BytesIO:
        """Return BytesIO object containing the updated JSON config."""
        json_str = json.dumps(self.config, indent=4)
        buffer = BytesIO(json_str.encode("utf-8"))
        buffer.name = "rao-parameters.json"
        buffer.seek(0)

        return buffer


if __name__ == "__main__":
    settings = RaoSettingsManager()
    print(settings.get("extensions.open-rao-search-tree-parameters.topological-actions-optimization.max-curative-search-tree-depth", 2))
    settings.set("extensions.open-rao-search-tree-parameters.topological-actions-optimization.max-curative-search-tree-depth", 1)
    buffer = settings.to_bytesio()
    print(buffer.read().decode("utf-8"))
    buffer.close()
