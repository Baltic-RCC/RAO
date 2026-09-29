"""Settings managers (rao/parameters/manager.py): Elastic source, overrides, accessors, RAO parameter files."""
import json
from io import BytesIO
from unittest.mock import MagicMock

import pypowsybl
import pytest

import rao.parameters.manager as manager_module
from rao.parameters.loadflow import CGMES_IMPORT_PARAMETERS
from rao.parameters.manager import LoadflowSettingsManager, RaoSettingsManager

RELAXATION_PATH = ["extensions", "open-rao-search-tree-parameters", "load-flow-and-sensitivity-computation",
                   "sensitivity-parameters", "load-flow-parameters", "useReactiveLimits"]


@pytest.fixture
def elastic_client(monkeypatch):
    client_class = MagicMock(name="Elasticsearch")
    monkeypatch.setattr(manager_module, "Elasticsearch", client_class)
    return client_class


class TestLoadflowSettingsSource:
    def test_elastic_settings_used_with_local_cgmes_import_parameters(self, elastic_client):
        elastic_client.return_value.get.return_value.raw = {"_source": {
            "CGMES_IMPORT_PARAMETERS": {"iidm.import.cgmes.source-for-iidm-id": "mRID"},
            "LF_PROVIDER": {"slackBusCountryFilter": "LT"},
            "LF_PARAMETERS": {"write_slack_bus": False},
        }}
        manager = LoadflowSettingsManager(elastic_server="https://elastic:9200", elastic_api_key="key",
                                          settings_keyword="BA_ID")
        elastic_client.assert_called_once_with("https://elastic:9200", api_key="key")
        elastic_client.return_value.get.assert_called_once_with(index="config-lf-parameters", id="BA_ID")
        assert manager.config["LF_PROVIDER"] == {"slackBusCountryFilter": "LT"}
        assert manager.config["CGMES_IMPORT_PARAMETERS"] == CGMES_IMPORT_PARAMETERS

    def test_elastic_failure_falls_back_to_repository(self, elastic_client, loguru_messages):
        elastic_client.return_value.get.side_effect = ConnectionError("unreachable")
        manager = LoadflowSettingsManager(elastic_server="https://elastic:9200")
        assert manager.get("LF_PROVIDER.slackBusSelectionMode") == "LARGEST_GENERATOR"
        assert any("unreachable" in r["message"] for r in loguru_messages if r["level"].name == "WARNING")


class TestLoadflowOverrides:
    def test_argument_takes_precedence_over_environment(self, tmp_path, monkeypatch):
        env_file, arg_file = tmp_path / "env.json", tmp_path / "arg.json"
        env_file.write_text(json.dumps({"LF_PROVIDER": {"slackBusCountryFilter": "EE"}}))
        arg_file.write_text(json.dumps({"LF_PROVIDER": {"slackBusCountryFilter": "LV"}}))
        monkeypatch.setenv("LOADFLOW_CONFIG_OVERRIDE_PATH", str(env_file))
        assert LoadflowSettingsManager(override_path=str(arg_file)).get("LF_PROVIDER.slackBusCountryFilter") == "LV"

    def test_deep_merge_replaces_lists_and_scalars(self, tmp_path):
        override = tmp_path / "o.json"
        override.write_text(json.dumps({"LF_PARAMETERS": {"countries_to_balance": ["LT"]}, "EXTRA": {"a": 1}}))
        manager = LoadflowSettingsManager(override_path=str(override))
        assert manager.get("LF_PARAMETERS.countries_to_balance") == ["LT"]
        assert manager.get("LF_PARAMETERS.write_slack_bus") is True
        assert manager.get("EXTRA.a") == 1

    def test_missing_override_file(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            LoadflowSettingsManager(override_path=str(tmp_path / "missing.json"))

    @pytest.mark.parametrize("content", ["[1, 2]", "null"])
    def test_override_must_be_a_mapping(self, tmp_path, content):
        override = tmp_path / "o.json"
        override.write_text(content)
        with pytest.raises(ValueError, match="mapping/dict"):
            LoadflowSettingsManager(override_path=str(override))

    def test_invalid_json_without_pyyaml(self, tmp_path, monkeypatch):
        monkeypatch.setattr(manager_module, "yaml", None)
        override = tmp_path / "o.yaml"
        override.write_text("LF_PROVIDER:\n  a: 1\n")
        with pytest.raises(RuntimeError, match="PyYAML"):
            LoadflowSettingsManager(override_path=str(override))

    def test_yaml_export_without_pyyaml(self, monkeypatch):
        monkeypatch.setattr(manager_module, "yaml", None)
        with pytest.raises(RuntimeError, match="PyYAML"):
            LoadflowSettingsManager().to_bytesio("yaml")

    def test_unknown_export_format(self):
        with pytest.raises(ValueError, match="fmt must be"):
            LoadflowSettingsManager().to_bytesio("xml")


class TestLoadflowAccessors:
    def test_keys_containing_dots_cannot_be_addressed(self):
        # CGMES import parameter names contain dots, so dotted-path access splits them
        manager = LoadflowSettingsManager()
        assert manager.get("CGMES_IMPORT_PARAMETERS.iidm.import.cgmes.source-for-iidm-id", "n/a") == "n/a"

    def test_set_through_a_scalar_node_fails(self):
        manager = LoadflowSettingsManager()
        with pytest.raises(TypeError):
            manager.set("LF_PROVIDER.slackBusSelectionMode.nested", "x")

    def test_get_returns_live_reference(self):
        manager = LoadflowSettingsManager()
        manager.get("LF_PROVIDER")["added"] = "yes"
        assert manager.get("LF_PROVIDER.added") == "yes"

    def test_export_config_is_a_copy(self):
        manager = LoadflowSettingsManager()
        exported = manager.export_config()
        exported["LF_PROVIDER"]["slackBusSelectionMode"] = "changed"
        assert manager.get("LF_PROVIDER.slackBusSelectionMode") == "LARGEST_GENERATOR"


class TestLoadflowParametersBuild:
    @pytest.mark.parametrize("raw, expected", [
        ("DC_VALUES", pypowsybl.loadflow.VoltageInitMode.DC_VALUES),
        ("VoltageInitMode.DC_VALUES", pypowsybl.loadflow.VoltageInitMode.DC_VALUES),
        ("dc_values", pypowsybl.loadflow.VoltageInitMode.DC_VALUES),
        (" UNIFORM_VALUES ", pypowsybl.loadflow.VoltageInitMode.UNIFORM_VALUES),
    ])
    def test_enum_strings_resolved(self, raw, expected):
        manager = LoadflowSettingsManager()
        manager.set("LF_PARAMETERS.voltage_init_mode", raw)
        assert manager.build_pypowsybl_parameters().voltage_init_mode == expected

    def test_exported_json_round_trips_to_parameters(self, tmp_path):
        override = tmp_path / "exported.json"
        override.write_bytes(LoadflowSettingsManager().to_bytesio().getvalue())
        parameters = LoadflowSettingsManager(override_path=str(override)).build_pypowsybl_parameters()
        assert parameters.balance_type == pypowsybl.loadflow.BalanceType.PROPORTIONAL_TO_GENERATION_P_MAX

    def test_unknown_parameter_rejected(self):
        manager = LoadflowSettingsManager()
        manager.set("LF_PARAMETERS.not_a_parameter", True)
        with pytest.raises(TypeError):
            manager.build_pypowsybl_parameters()

    def test_build_does_not_mutate_config(self):
        manager = LoadflowSettingsManager()
        manager.set("LF_PARAMETERS.voltage_init_mode", "DC_VALUES")
        manager.build_pypowsybl_parameters()
        assert manager.get("LF_PARAMETERS.voltage_init_mode") == "DC_VALUES"
        assert "provider_parameters" not in manager.config["LF_PARAMETERS"]


class TestRaoSettingsManager:
    def test_parameter_file_matches_installed_pypowsybl(self):
        manager = RaoSettingsManager()
        assert str(manager.default_path) == RaoSettingsManager.RAO_PARAMETERS_VERSION_MAP[pypowsybl.__version__]
        assert manager.get("objective-function.type") == "MAX_MIN_MARGIN"

    def test_every_mapped_file_exists_and_is_json(self):
        for version, path in RaoSettingsManager.RAO_PARAMETERS_VERSION_MAP.items():
            assert isinstance(json.loads(manager_module.Path(path).read_text(encoding="utf-8")), dict), version

    @pytest.mark.xfail(strict=True, raises=TypeError,
                       reason="BUG: Path(None) raises TypeError before the intended "
                              "'Unsupported version to get parameters' ValueError can be raised")
    def test_unsupported_pypowsybl_version(self, monkeypatch):
        monkeypatch.setattr(manager_module.pypowsybl, "__version__", "0.0.0")
        with pytest.raises(ValueError, match="Unsupported version"):
            RaoSettingsManager()

    def test_environment_override_merged(self, tmp_path, monkeypatch):
        override = tmp_path / "rao.json"
        override.write_text(json.dumps({"objective-function": {"type": "MIN_COST"}}))
        monkeypatch.setenv("RAO_CONFIG_OVERRIDE_PATH", str(override))
        manager = RaoSettingsManager()
        assert manager.get("objective-function.type") == "MIN_COST"
        assert manager.get("objective-function.enforce-curative-security") is True

    def test_missing_override_file_silently_ignored(self, tmp_path, monkeypatch):
        # Edge: unlike LoadflowSettingsManager, a missing RAO override is not an error
        baseline = RaoSettingsManager().config
        monkeypatch.setenv("RAO_CONFIG_OVERRIDE_PATH", str(tmp_path / "missing.json"))
        assert RaoSettingsManager().config == baseline

    def test_invalid_override_json(self, tmp_path, monkeypatch):
        override = tmp_path / "rao.json"
        override.write_text("{not json")
        monkeypatch.setenv("RAO_CONFIG_OVERRIDE_PATH", str(override))
        with pytest.raises(json.JSONDecodeError):
            RaoSettingsManager()

    def test_set_get_and_fresh_buffers(self):
        manager = RaoSettingsManager()
        key = "extensions.open-rao-search-tree-parameters.topological-actions-optimization.max-curative-search-tree-depth"
        manager.set(key, 1)
        manager.set({"new.key": "v"})
        assert manager.get(key) == 1 and manager.get("new.key") == "v"
        assert manager.get("missing.path", "d") == "d"
        first, second = manager.to_bytesio(), manager.to_bytesio()
        assert first is not second and first.name == "rao-parameters.json"
        assert json.load(first)["new"] == {"key": "v"}

    def test_relaxation_path_used_by_handler_exists(self):
        node = RaoSettingsManager().config
        for key in RELAXATION_PATH[:-1]:
            node = node[key]
        assert node[RELAXATION_PATH[-1]] is True

    def test_legacy_v24_parameters_lack_relaxation_path(self):
        # rao_v24.json is not mapped to any pypowsybl version; the handler's retry would KeyError on it
        legacy = json.loads((manager_module.Path(manager_module.__file__).parent / "rao_v24.json").read_text())
        assert "extensions" not in legacy

    @pytest.mark.integration
    def test_parameters_and_relaxed_parameters_load_in_openrao(self):
        manager = RaoSettingsManager()
        assert isinstance(pypowsybl.rao.Parameters.from_buffer_source(manager.to_bytesio()), pypowsybl.rao.Parameters)
        relaxed = json.load(manager.to_bytesio())
        node = relaxed
        for key in RELAXATION_PATH[:-1]:
            node = node[key]
        node[RELAXATION_PATH[-1]] = False
        pypowsybl.rao.Parameters.from_buffer_source(BytesIO(json.dumps(relaxed).encode()))
