import os
import json
from io import BytesIO
from pathlib import Path
from copy import deepcopy
from typing import Optional
import pypowsybl
from enum import Enum as _PyEnum
from elasticsearch import Elasticsearch
from rao.parameters.loadflow import CGMES_IMPORT_PARAMETERS, LF_PROVIDER, LF_PARAMETERS
from loguru import logger

try:
    import yaml  # type: ignore
except Exception:
    yaml = None


class LoadflowSettingsManager:
    """Class-based settings manager for pypowsybl load flow, similar to RaoSettingsManager.

    - Defaults are queried from Elasticsearch and fallback to loadflow.py file.
    - Optional override file path is read from env:
        * LOADFLOW_CONFIG_OVERRIDE_PATH
      The file may be JSON or YAML.
    - Deep-merge override into defaults.
    - No type coercion: values are kept as-is (strings remain strings).
    - Provides get/set helpers and export to BytesIO (JSON/YAML).

    Note:
    - LF_PARAMETERS in defaults is a pypowsybl Parameters object.
      We snapshot a dict of known public constructor-like attributes at init.
      If you add new fields to your default Parameters in the future, you can extend
      _extract_params_dict accordingly or switch to a custom serialization hook.
    """

    # Minimal set of known attributes often used in pypowsybl.loadflow.Parameters
    _KNOWN_PARAM_FIELDS = list(pypowsybl.loadflow.Parameters().__dict__.keys())
    _KNOWN_PARAM_FIELDS = [f for f in _KNOWN_PARAM_FIELDS if f != "provider_parameters"]

    def __init__(self,
                 elastic_server: Optional[str] = None,
                 elastic_username: Optional[str] = None,
                 elastic_password: Optional[str] = None,
                 elastic_index: str = 'config-lf-parameters',
                 settings_keyword: str = 'BA_DEFAULT',
                 override_path: Optional[str] = None):

        self.elastic_server = elastic_server
        self.elastic_username = elastic_username
        self.elastic_password = elastic_password
        self.elastic_index = elastic_index
        self.settings_keyword = settings_keyword

        # Decide override path from arg or env
        env_path = os.environ.get('LOADFLOW_CONFIG_OVERRIDE_PATH')
        self.override_path = Path(override_path or env_path) if (override_path or env_path) else None
        if self.override_path:
            logger.info(f"Loadflow settings override path: {self.override_path}")

        # Firstly try to get loadflow parameters from Elastic as primary source, otherwise - fallback to repository
        try:
            base = self._get_defaults_from_elastic()
            # overwrite CGMES_IMPORT_PARAMETERS from local repository
            base['CGMES_IMPORT_PARAMETERS'] = deepcopy(CGMES_IMPORT_PARAMETERS)
        except Exception as err:
            logger.warning(f"Loadflow settings retrieving failed from Elastic: {err}")
            logger.warning(f"Using default settings from repository")
            base = {
                'CGMES_IMPORT_PARAMETERS': deepcopy(CGMES_IMPORT_PARAMETERS),
                'LF_PROVIDER': deepcopy(LF_PROVIDER),
                'LF_PARAMETERS': self._extract_params_dict(LF_PARAMETERS),
            }

        # Handle overrides if defined
        overrides = self._load_override_file(self.override_path) if self.override_path else {}
        self.config = self._deep_merge(base, overrides)

    # ----------------- I/O -----------------
    def _get_defaults_from_elastic(self) -> dict:
        if not self.elastic_server:
            raise Exception("Elasticsearch server not defined")
        client = Elasticsearch(self.elastic_server)
        logger.info(f"Retrieving base loadflow settings fromm Elasticsearch with key: {self.settings_keyword}")
        response = client.get(index=self.elastic_index, id=self.settings_keyword)

        return response.raw["_source"]

    @staticmethod
    def _load_override_file(self, path: Path | None) -> dict:
        if not path:
            return {}
        if not path.exists():
            raise FileNotFoundError(f"Override config not found: {path}")
        text = path.read_text(encoding='utf-8')
        lower = path.name.lower()
        if lower.endswith(('.yaml', '.yml')) and yaml is not None:
            data = yaml.safe_load(text) or {}
        else:
            # Try JSON first, then YAML if available
            try:
                data = json.loads(text)
            except Exception:
                if yaml is None:
                    raise RuntimeError("Install PyYAML to read YAML overrides or provide valid JSON")
                data = yaml.safe_load(text) or {}
        if not isinstance(data, dict):
            raise ValueError("Override file must contain a mapping/dict at the top level")
        return data

    # --- Plainification helpers (for export only) ---
    def _to_plain(self, obj, enum_repr: str = 'name'):
        """Recursively convert non-JSON-serializable types:
        - Enum -> name|value|str
        - set/tuple -> list
        - Path -> str
        Does NOT mutate self.config; only used for export.
        """
        # Enums
        if isinstance(obj, _PyEnum):
            if enum_repr == 'name':
                return obj.name
            if enum_repr == 'value':
                return obj.value
            return str(obj)

        # Common simple containers
        if isinstance(obj, dict):
            return {k: self._to_plain(v, enum_repr) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [self._to_plain(v, enum_repr) for v in obj]
        if isinstance(obj, Path):
            return str(obj)

        # Last resort: try JSON encoding test and fallback to str if needed
        try:
            json.dumps(obj)
            return obj
        except TypeError:
            return str(obj)

    def to_bytesio(self, fmt: str = 'json', enum_repr: str = 'name') -> BytesIO:
        """Return BytesIO of merged config in JSON/YAML.
        enum_repr: how to represent Enum values ('name'|'value'|'str').
        """
        if fmt not in ('json', 'yaml'):
            raise ValueError("fmt must be 'json' or 'yaml'")
        plain = self._to_plain(self.config, enum_repr=enum_repr)

        if fmt == 'yaml':
            if yaml is None:
                raise RuntimeError("PyYAML not installed; cannot write YAML")
            payload = yaml.safe_dump(plain, sort_keys=False, allow_unicode=True)
            name = 'loadflow-settings.yaml'
        else:
            payload = json.dumps(plain, indent=4, ensure_ascii=False)
            name = 'loadflow-settings.json'

        buf = BytesIO(payload.encode('utf-8'))
        buf.name = name
        buf.seek(0)
        return buf

    # ----------------- Accessors -----------------
    def get(self, path: str, default=None):
        """Get nested value by dot path, e.g. 'LF_PROVIDER.maxVoltageMismatch'"""
        keys = path.split('.')
        val = self.config
        for k in keys:
            if isinstance(val, dict):
                val = val.get(k, default)
            else:
                return default
        return val

    def set(self, path_or_dict, value=None):
        """Set a single nested value or multiple values:
            set('LF_PARAMETERS.write_slack_bus', True)
            set({'LF_PROVIDER.slackBusCountryFilter': 'LT', 'LF_PARAMETERS.read_slack_bus': False})
        """
        if isinstance(path_or_dict, dict):
            for p, v in path_or_dict.items():
                self._set_single(p, v)
        else:
            self._set_single(path_or_dict, value)

    # ----------------- Helpers -----------------
    def _set_single(self, path: str, value):
        keys = path.split('.')
        d = self.config
        for k in keys[:-1]:
            d = d.setdefault(k, {})
        d[keys[-1]] = value

    def _deep_merge(self, a: dict, b: dict) -> dict:
        res = deepcopy(a)
        for k, v in (b or {}).items():
            if isinstance(v, dict) and isinstance(res.get(k), dict):
                res[k] = self._deep_merge(res[k], v)
            else:
                res[k] = deepcopy(v)
        return res

    def _extract_params_dict(self, params_obj) -> dict:
        """Extract a dict snapshot from a pypowsybl Parameters object without coercion."""
        out = {}
        for name in self._KNOWN_PARAM_FIELDS:
            if hasattr(params_obj, name):
                out[name] = getattr(params_obj, name)
        return out

    # --- Enum resolution (build-time only) ---
    def _resolve_enums(self, lf_params: dict):
        """Map enum-looking strings to real pypowsybl enums for known fields.
        Does NOT mutate self.config; operates on the provided dict.
        Accepts:
          - 'UNIFORM_VALUES' (name)
          - 'VoltageInitMode.UNIFORM_VALUES' (qualified)
          - value equality with member.value
        Case-insensitive on names.
        """
        enum_map = {
            'voltage_init_mode': pypowsybl.loadflow.VoltageInitMode,
            'balance_type': pypowsybl.loadflow.BalanceType,
            'connected_component_mode': pypowsybl.loadflow.ConnectedComponentMode,
        }

        out = deepcopy(lf_params)

        for key, enum_cls in enum_map.items():
            val = out.get(key)
            if isinstance(val, _PyEnum):
                continue
            if isinstance(val, str):
                s = val.strip()
                # strip optional qualifier "EnumClass."
                if "." in s:
                    s = s.split(".")[-1]

                # 1) direct attribute lookup (handles typical Python Enums)
                for candidate in (s, s.upper(), s.lower()):
                    if hasattr(enum_cls, candidate):
                        out[key] = getattr(enum_cls, candidate)
                        break
                else:
                    # 2) iterate members and match by name (case-insensitive)
                    try:
                        for member in enum_cls:
                            if getattr(member, "name", str(member)).upper() == s.upper():
                                out[key] = member
                                break
                        else:
                            # 3) fallback: match by value string
                            for member in enum_cls:
                                if str(getattr(member, "value", "")) == val:
                                    out[key] = member
                                    break
                    except TypeError:
                        # some foreign enums might not be iterable
                        pass
        return out

    # -------- Optional: build pypowsybl object --------
    def build_pypowsybl_parameters(self):
        import pypowsybl
        lf_params = deepcopy(self.config.get('LF_PARAMETERS', {}))
        lf_params['provider_parameters'] = deepcopy(self.config.get('LF_PROVIDER', {}))
        # Convert enum strings to actual enums for known fields (build-time only)
        lf_params = self._resolve_enums(lf_params)
        return pypowsybl.loadflow.Parameters(**lf_params)

    def export_config(self, plain: bool = True, enum_repr: str = 'name') -> dict:
        cfg = deepcopy(self.config)
        return self._to_plain(cfg, enum_repr=enum_repr) if plain else cfg


class RaoSettingsManager:

    RAO_PARAMETERS_VERSION_MAP = {
        "1.11.0": f"{Path(__file__).parent.joinpath('rao_v30.json')}",
        "1.11.1": f"{Path(__file__).parent.joinpath('rao_v30.json')}",
        "1.11.2": f"{Path(__file__).parent.joinpath('rao_v30.json')}",
        "1.12.0": f"{Path(__file__).parent.joinpath('rao_v31.json')}",
        "1.13.0": f"{Path(__file__).parent.joinpath('rao_v32.json')}",
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
