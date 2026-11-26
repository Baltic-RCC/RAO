from pathlib import Path

import pytest
from pypowsybl._pypowsybl import PyPowsyblError

from rao.parameters.manager import LoadflowSettingsManager, RaoSettingsManager

OVERRIDE_FILE = Path(__file__).with_name("lf_settings_override.json")


@pytest.fixture(autouse=True)
def no_ambient_override(monkeypatch):
    monkeypatch.delenv("LOADFLOW_CONFIG_OVERRIDE_PATH", raising=False)


def test_default_config_builds_pypowsybl_parameters():
    mgr = LoadflowSettingsManager()

    assert mgr.config
    assert mgr.build_pypowsybl_parameters() is not None
    assert mgr.get("LF_PARAMETERS.write_slack_bus", None) is not None
    assert mgr.to_bytesio("json").getvalue()


def test_override_file_is_merged_into_defaults(monkeypatch):
    monkeypatch.setenv("LOADFLOW_CONFIG_OVERRIDE_PATH", str(OVERRIDE_FILE))

    mgr = LoadflowSettingsManager()

    assert mgr.get("LF_PROVIDER.maxNewtonRaphsonIterations") == "20"
    assert mgr.get("LF_PARAMETERS.countries_to_balance") == ["LT", "LV", "EE"]
    assert mgr.build_pypowsybl_parameters() is not None


@pytest.mark.xfail(
    raises=PyPowsyblError,
    reason="Known gap: the default snapshot already carries 'component_mode', and pypowsybl rejects it together with "
           "the deprecated 'connected_component_mode'. Overrides using the old key fail to build parameters.",
)
def test_deprecated_connected_component_mode_override(monkeypatch, tmp_path):
    override = tmp_path / "override.json"
    override.write_text('{"LF_PARAMETERS": {"connected_component_mode": "MAIN"}}', encoding="utf-8")
    monkeypatch.setenv("LOADFLOW_CONFIG_OVERRIDE_PATH", str(override))

    LoadflowSettingsManager().build_pypowsybl_parameters()


def test_missing_override_file_is_rejected(monkeypatch, tmp_path):
    monkeypatch.setenv("LOADFLOW_CONFIG_OVERRIDE_PATH", str(tmp_path / "missing.json"))

    with pytest.raises(FileNotFoundError):
        LoadflowSettingsManager()


def test_installed_pypowsybl_has_rao_parameters():
    """Fails when pypowsybl is bumped (e.g. by Dependabot) without adding it to RAO_PARAMETERS_VERSION_MAP."""
    mgr = RaoSettingsManager()

    assert mgr.default_path.exists()
