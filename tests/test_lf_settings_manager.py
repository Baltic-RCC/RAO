"""LoadflowSettingsManager with the repository defaults and the lf_settings_override.json fixture.

Replaces the former print-only script; the scenarios (defaults, accessors, export,
environment override) are the same, now as assertions.
"""
import json
from pathlib import Path

import pypowsybl
import pytest

from rao.parameters.loadflow import CGMES_IMPORT_PARAMETERS
from rao.parameters.manager import LoadflowSettingsManager

OVERRIDE_FILE = Path(__file__).resolve().parent / "lf_settings_override.json"


def test_repository_defaults():
    manager = LoadflowSettingsManager()
    assert set(manager.config) == {"CGMES_IMPORT_PARAMETERS", "LF_PROVIDER", "LF_PARAMETERS"}
    assert manager.config["CGMES_IMPORT_PARAMETERS"] == CGMES_IMPORT_PARAMETERS
    assert manager.get("LF_PARAMETERS.write_slack_bus") is True
    assert manager.get("LF_PROVIDER.lowImpedanceThreshold") == "3.0E-5"


def test_build_pypowsybl_parameters_from_defaults():
    parameters = LoadflowSettingsManager().build_pypowsybl_parameters()
    assert isinstance(parameters, pypowsybl.loadflow.Parameters)
    assert parameters.voltage_init_mode == pypowsybl.loadflow.VoltageInitMode.UNIFORM_VALUES
    assert parameters.connected_component_mode == pypowsybl.loadflow.ConnectedComponentMode.ALL
    assert parameters.provider_parameters["slackBusSelectionMode"] == "LARGEST_GENERATOR"


def test_set_and_get():
    manager = LoadflowSettingsManager()
    manager.set("LF_PROVIDER.maxNewtonRaphsonIterations", "25")
    manager.set({"LF_PARAMETERS.read_slack_bus": True, "NEW.section.key": 1})
    assert manager.get("LF_PROVIDER.maxNewtonRaphsonIterations") == "25"
    assert manager.get("LF_PARAMETERS.read_slack_bus") is True
    assert manager.get("NEW.section.key") == 1
    assert manager.get("LF_PROVIDER.unknown", "fallback") == "fallback"


def test_json_export():
    buffer = LoadflowSettingsManager().to_bytesio("json")
    assert buffer.name == "loadflow-settings.json" and buffer.tell() == 0
    exported = json.load(buffer)
    assert exported["LF_PARAMETERS"]["write_slack_bus"] is True
    # pypowsybl enums are pybind11 types, not enum.Enum: exported through str()
    assert exported["LF_PARAMETERS"]["voltage_init_mode"] == "VoltageInitMode.UNIFORM_VALUES"


def test_environment_override(monkeypatch):
    monkeypatch.setenv("LOADFLOW_CONFIG_OVERRIDE_PATH", str(OVERRIDE_FILE))
    manager = LoadflowSettingsManager()
    assert manager.override_path == OVERRIDE_FILE
    assert manager.get("LF_PROVIDER.slackBusCountryFilter") == "LT"
    assert manager.get("LF_PROVIDER.lowImpedanceThreshold") == "3.0E-5"  # untouched keys survive the merge
    assert manager.config["CGMES_IMPORT_PARAMETERS"]["iidm.import.cgmes.import-node-breaker-as-bus-breaker"] == "False"
    assert manager.get("LF_PARAMETERS.connected_component_mode") == "MAIN"
    assert manager.get("LF_PARAMETERS.countries_to_balance") == ["LT", "LV", "EE"]


@pytest.mark.xfail(strict=True, raises=pypowsybl.PyPowsyblError,
                   reason="BUG: the defaults snapshot carries component_mode (pypowsybl >= 1.16); overriding the "
                          "deprecated connected_component_mode - as lf_settings_override.json does and "
                          "_resolve_enums supports - sets both and pypowsybl rejects the Parameters")
def test_environment_override_builds_parameters(monkeypatch):
    monkeypatch.setenv("LOADFLOW_CONFIG_OVERRIDE_PATH", str(OVERRIDE_FILE))
    parameters = LoadflowSettingsManager().build_pypowsybl_parameters()
    assert parameters.connected_component_mode == pypowsybl.loadflow.ConnectedComponentMode.MAIN
    assert parameters.countries_to_balance == ["LT", "LV", "EE"]
    assert parameters.distributed_slack is False


def test_relative_override_path_resolves_against_cwd(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("LOADFLOW_CONFIG_OVERRIDE_PATH", "lf_settings_override.json")
    with pytest.raises(FileNotFoundError):
        LoadflowSettingsManager()
