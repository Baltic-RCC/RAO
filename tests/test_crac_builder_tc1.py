"""CracBuilder on the TC1 test configuration (BE/NL MicroGrid, CGMES 3.0 + NC 2.3 profiles).

Raw TC1 hits three builder defects (strict xfails below). Everything else uses the
minimal adaptation in ``helpers.adapt_tc1_profiles`` / ``helpers.adapt_tc1_network``.
"""
import json

import pytest

from helpers import REPO_ROOT, TC1_CONTINGENCY_BE_LINE_1
from rao.crac.builder import CracBuilder

TC1_CONTINGENCIES = {
    "288ac5c2-128e-40cd-b5b2-d3d79779694c": "OCO_BE-G1",
    "2f28d2b2-b37f-444b-8661-cb7375299b61": "OCO_BE-Line_4",       # normalMustStudy=false, still built
    "844e70d2-b39d-4093-9edc-ef931bc435db": "OCO_NL-Line_1",
    "8fac3367-7b25-485d-80e9-1bde01a7c516": "EXCO_ANVERS_BRUSSELS",
    "bea8e271-4901-4ddf-9a3a-b1d13f2bf2e0": "OCO_BE-Line_3",
    "cc276a2f-98f7-4927-8e85-1560af6e223d": "OCO_BE-TR2_2",
    "ea6a5055-3108-4f86-bfd7-e1320a9473da": "OOR_NL_BORDER",
    "f38b9192-6064-4f1a-b2c6-5459ceb514b0": "OCO_BE-Line_1",
}
MONITORED_LINES = {"BE-Line_1", "BE-Line_2", "BE-Line_3", "BE-Line_5", "BE-Line_6", "BE-Line_7", "NL-Line_1", "NL-Line_2"}
TC1_NETWORK_ACTIONS = {
    "RA_BE-TR2_1": ("preventive", "terminalsConnectionActions"),
    "RA_BE_S1": ("preventive", "shuntCompensatorPositionActions"),   # needs the adapted property range
    "RA_BE-Line_2": ("curative", "terminalsConnectionActions"),      # needs the adapted property range
    "RA_NL-G3": ("curative", "terminalsConnectionActions"),
    "RA_NL-Line_3": ("curative", "terminalsConnectionActions"),
}


def without_added_ranges(profiles):
    return profiles[~profiles["ID"].str.startswith("test-range-")]


class TestRawTc1Defects:
    @pytest.mark.xfail(strict=True, raises=KeyError,
                       reason="BUG: TC1 AEs carry no IdentifiedObject.description, process_cnecs indexes it directly")
    def test_assessed_elements_without_description(self, tc1_profiles, tc1_network_triplets_adapted):
        CracBuilder(data=tc1_profiles, network=tc1_network_triplets_adapted).build_crac()

    @pytest.mark.xfail(strict=True, raises=AttributeError,
                       reason="BUG: ALT_BE_S1 / ALT_BE-Line_2 have no StaticPropertyRange; "
                              "`directions.unique().item()` fails on the empty StringArray")
    def test_alterations_without_property_range(self, tc1_profiles_adapted, tc1_network_triplets_adapted):
        CracBuilder(data=without_added_ranges(tc1_profiles_adapted), network=tc1_network_triplets_adapted).build_crac()

    @pytest.mark.xfail(strict=True, raises=KeyError,
                       reason="BUG/LIMITATION: TC1 is CGMES 3.0 (OperationalLimitType.kind); limits are only read "
                              "from the CGMES 2.4.15 OperationalLimitType.limitType")
    def test_cgmes3_operational_limit_kind(self, tc1_profiles_adapted, tc1_network_triplets):
        CracBuilder(data=tc1_profiles_adapted, network=tc1_network_triplets).build_crac()

    @pytest.mark.xfail(strict=True, raises=AssertionError,
                       reason="BUG: build_crac() for all contingencies repeats '{mRID}-curative' CNEC ids")
    def test_full_crac_has_unique_cnec_ids(self, tc1_profiles_adapted, tc1_network_triplets_adapted):
        crac = CracBuilder(data=tc1_profiles_adapted, network=tc1_network_triplets_adapted).build_crac()
        ids = [c["id"] for c in crac["flowCnecs"]]
        assert len(ids) == len(set(ids))


@pytest.fixture(scope="module")
def tc1_builder(tc1_profiles_adapted, tc1_network_triplets_adapted):
    return CracBuilder(data=tc1_profiles_adapted, network=tc1_network_triplets_adapted)


class TestTc1FullCrac:
    @pytest.fixture(scope="class")
    def crac(self, tc1_builder):
        return tc1_builder.build_crac()

    def test_all_contingencies(self, crac):
        assert {c["id"]: c["name"] for c in crac["contingencies"]} == TC1_CONTINGENCIES

    def test_exceptional_contingency_has_two_elements(self, crac):
        exco = next(c for c in crac["contingencies"] if c["name"] == "EXCO_ANVERS_BRUSSELS")
        assert sorted(exco["networkElementsIds"]) == ["_17086487-56ba-4979-b8de-064025a6b4da",
                                                      "_ffbabc27-1ccd-4fdc-b037-e341706c8d29"]

    def test_disabled_assessed_element_not_monitored(self, crac):
        assert {c["name"] for c in crac["flowCnecs"]} == MONITORED_LINES  # BE-Line_4 has normalEnabled=false

    def test_network_actions(self, crac):
        actions = {a["name"]: a for a in crac["networkActions"]}
        assert set(actions) == set(TC1_NETWORK_ACTIONS)
        for name, (instant, kind) in TC1_NETWORK_ACTIONS.items():
            assert actions[name]["onInstantUsageRules"] == [{"instant": instant}]
            assert kind in actions[name]

    def test_unsupported_tc1_alterations_are_not_in_crac(self, crac):
        # TapPositionAction (RA_BE-TR2_3), RotatingMachineAction (RA_NL-G2), LoadAction,
        # RegulatingControlAction and the non-GSA countertrade/redispatch actions
        names = {a["name"] for a in crac["networkActions"]}
        assert not names & {"RA_BE-TR2_3", "RA_NL-G2", "COUNTERTRADE_BE-NL"}

    def test_no_voltage_cnecs_without_voltage_limits(self, crac):
        assert crac["voltageCnecs"] == []

    def test_consistent_with_example_crac(self, crac):
        example = json.loads((REPO_ROOT / "examples" / "TC1_example_crac.json").read_text())
        assert {c["id"]: sorted(c["networkElementsIds"]) for c in example["contingencies"]} == {
            c["id"]: sorted(e.lstrip("_") for e in c["networkElementsIds"]) for c in crac["contingencies"]}
        example_elements = {c["name"]: c["networkElementId"] for c in example["flowCnecs"]}
        built_elements = {c["name"]: c["networkElementId"].lstrip("_") for c in crac["flowCnecs"]}
        assert built_elements == {name: example_elements[name] for name in MONITORED_LINES}


@pytest.mark.parametrize("contingency_id", sorted(TC1_CONTINGENCIES))
def test_per_contingency_crac_as_built_by_handler(tc1_builder, tc1_network_triplets_adapted, contingency_id):
    crac = tc1_builder.build_crac(contingency_ids=[contingency_id])
    assert [c["id"] for c in crac["contingencies"]] == [contingency_id]

    cnecs = crac["flowCnecs"]
    assert len({c["id"] for c in cnecs}) == len(cnecs) == 2 * len(MONITORED_LINES)
    assert {c["contingencyId"] for c in cnecs if c["instant"] == "curative"} == {contingency_id}
    assert all(t["min"] == -t["max"] and t["unit"] == "ampere" for c in cnecs for t in c["thresholds"])

    network_ids = set(tc1_network_triplets_adapted["ID"])
    assert {c["networkElementId"].lstrip("_") for c in cnecs} <= network_ids
    assert {e.lstrip("_") for e in crac["contingencies"][0]["networkElementsIds"]} <= network_ids


def test_patl_and_tatl_thresholds(tc1_builder):
    crac = tc1_builder.build_crac(contingency_ids=[TC1_CONTINGENCY_BE_LINE_1])
    be_line_3 = {c["instant"]: c["thresholds"][0]["max"] for c in crac["flowCnecs"] if c["name"] == "BE-Line_3"}
    assert be_line_3 == {"preventive": 1233.9, "curative": 500.0}


@pytest.mark.integration
def test_thresholds_match_pypowsybl_operational_limits(tc1_crac, tc1_network):
    limits = tc1_network.get_operational_limits().reset_index()
    current = limits[limits["type"] == "CURRENT"]
    permanent = current[current["acceptable_duration"] == -1].groupby("element_id")["value"].min()
    temporary = current[current["acceptable_duration"] > 0].groupby("element_id")["value"].min()
    for cnec in tc1_crac["flowCnecs"]:
        expected = permanent if cnec["instant"] == "preventive" else temporary
        assert cnec["thresholds"][0]["max"] == pytest.approx(expected[cnec["networkElementId"]], abs=0.05), cnec["name"]
