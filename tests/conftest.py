"""Shared pytest configuration for the RAO test-suite.

Unit tests use synthetic triplets and mocks. Tests marked ``integration`` run real
pypowsybl/OpenRAO on the TC1 test configuration from the ``test-data`` submodule
(``git submodule update --init test-data``) and are skipped when it is absent.
"""
import json
import sys
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd  # noqa: E402
from loguru import logger  # noqa: E402

import helpers  # noqa: E402

TC1_FILES = (helpers.TC1_CO, helpers.TC1_AE, helpers.TC1_RA, helpers.TC1_CGMES)


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: runs real pypowsybl/OpenRAO computations on TC1 data")


@pytest.fixture(autouse=True)
def _isolate_override_env(monkeypatch):
    """Settings managers read override paths from the environment."""
    monkeypatch.delenv("LOADFLOW_CONFIG_OVERRIDE_PATH", raising=False)
    monkeypatch.delenv("RAO_CONFIG_OVERRIDE_PATH", raising=False)


@pytest.fixture
def loguru_messages():
    """Collect loguru records (pytest's caplog does not see loguru)."""
    records = []
    sink_id = logger.add(lambda message: records.append(message.record), level="DEBUG", format="{message}")
    yield records
    logger.remove(sink_id)


# --- TC1 data -----------------------------------------------------------------------

@pytest.fixture(scope="session")
def tc1_dir() -> Path:
    missing = [name for name in TC1_FILES if not (helpers.TC1_DIR / name).exists()]
    if missing:
        pytest.skip(f"TC1 test data missing ({', '.join(missing)}); run `git submodule update --init test-data`")
    return helpers.TC1_DIR


@pytest.fixture(scope="session")
def tc1_profiles(tc1_dir) -> pd.DataFrame:
    """Raw TC1 CO/AE/RA NC profiles as triplets."""
    return pd.read_RDF([str(tc1_dir / name) for name in (helpers.TC1_CO, helpers.TC1_AE, helpers.TC1_RA)])


@pytest.fixture(scope="session")
def tc1_network_triplets(tc1_dir) -> pd.DataFrame:
    """Raw TC1 CGMES (3.0) grid model as triplets."""
    return pd.read_RDF([str(tc1_dir / helpers.TC1_CGMES)])


@pytest.fixture(scope="session")
def tc1_profiles_adapted(tc1_profiles) -> pd.DataFrame:
    return helpers.adapt_tc1_profiles(tc1_profiles)


@pytest.fixture(scope="session")
def tc1_network_triplets_adapted(tc1_network_triplets) -> pd.DataFrame:
    return helpers.adapt_tc1_network(tc1_network_triplets)


@pytest.fixture(scope="session")
def tc1_crac(tc1_profiles_adapted, tc1_network_triplets_adapted) -> dict:
    """CRAC built the way the handler does it: one contingency (OCO_BE-Line_1) per CRAC."""
    from rao.crac.builder import CracBuilder
    builder = CracBuilder(data=tc1_profiles_adapted, network=tc1_network_triplets_adapted)
    return builder.build_crac(contingency_ids=[helpers.TC1_CONTINGENCY_BE_LINE_1])


@pytest.fixture
def tc1_crac_buffer(tc1_crac) -> BytesIO:
    return helpers.named_buffer(json.dumps(tc1_crac).encode("utf-8"), "crac.json")


@pytest.fixture
def pypowsybl_module():
    return pytest.importorskip("pypowsybl")


@pytest.fixture
def lf_settings():
    from rao.parameters.manager import LoadflowSettingsManager
    return LoadflowSettingsManager()


@pytest.fixture
def tc1_network(tc1_dir, pypowsybl_module, lf_settings):
    """Fresh pypowsybl network per test: RAO, load flow and workarounds mutate it."""
    return pypowsybl_module.network.load(str(tc1_dir / helpers.TC1_CGMES),
                                         parameters=lf_settings.config["CGMES_IMPORT_PARAMETERS"])


# --- Service doubles ----------------------------------------------------------------

@pytest.fixture
def object_storage_mock():
    """ObjectStorage double: records S3 uploads and Elastic bulk sends."""
    storage = MagicMock(name="ObjectStorage")
    storage.elastic_service.server = None
    storage.elastic_service.api_key = None
    storage.get_input_data_for_timestamp.return_value = []
    storage.get_latest_available_input_data.return_value = []
    return storage
