import uuid
from json import dumps
from types import SimpleNamespace
from datetime import datetime, timezone, timedelta
import pytest
import requests
from pydantic import ValidationError
from rao import sar
from rao.sar import SarRequest, generate_sar

LINE, TRAFO, CO1, CO2 = (str(uuid.uuid4()) for _ in range(4))
ELASTIC = SimpleNamespace(client=None)  # scan is replaced, the client is only passed through
REQUEST = SarRequest(scenario_time="2026-09-23T22:30:00Z", time_horizon="ID", run_id="09")


def doc(element, flow, instant="curative", action="RA-1", contingency=CO1, batch="b2", timestamp="2026-11-05T11:00:00"):
    """RAO result document as stored by HandlerOptimizer: dotted column names, task headers under rmq."""
    return {
        "@timestamp": timestamp,
        "rmq": {"batch-id": batch, "contingency-id": contingency},
        "computationStatus": "default",
        "cnec.name": "Line A",
        "cnec.networkElementId": f"_{element}",
        "cnec.instant": instant,
        "cnec.operator": "https://energy.referencedata.eu/EIC/10XLT-LITGRID--Q",
        "cnec.substation": "Substation A",
        "cnec.thresholds.side": 1,
        "cnec.thresholds.min": -1000,
        "cnec.thresholds.max": 1000,
        f"cnecResults.{instant}.ampere.side1.flow": flow,
        f"cnecResults.{instant}.ampere.side1.loading": abs(flow) / 1000,
        f"cnecResults.{instant}.megawatt.side1.flow": flow / 2,
        "networkActionId": action,
        "activatedStates.contingency": contingency if instant == "curative" else None,
    }


HITS = [
    doc(LINE, 900),                                                   # 90% of the limit
    doc(f"{TRAFO}-Leg2", 1100, action="RA-2"),                        # 3w transformer leg, 110%
    doc(LINE, -1200, action="RA-3", contingency=CO2),                 # against the negative limit, 120%
    doc(LINE, 500, instant="preventive", action="RA-3", contingency=CO2),  # not activated for a contingency
    doc(LINE, 700, action=None),                                      # no network action applied, skipped
    doc(LINE, 2000, batch="b1", timestamp="2026-11-05T10:00:00"),     # older run of the contingency, ignored
]


class Response:
    def __init__(self, status_code: int = 200, content: bytes = b"<sar/>"):
        self.status_code, self.content, self.text = status_code, content, content.decode()

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error", response=self)


@pytest.fixture
def calls(monkeypatch):
    """Replace Elastic scan and the nc-profile-service request, what was sent is collected to the returned dict."""
    calls = {"hits": HITS, "status": 200}

    def fake_scan(client, index, query):
        calls.update(index=index, query=query)
        return [{"_source": hit} for hit in calls["hits"]]

    def fake_post(url, json, **kwargs):
        dumps(json, allow_nan=False)  # requests refuses NaN, so the payload must not have any
        calls.update(url=url, body=json)
        return Response(calls["status"])

    monkeypatch.setattr(sar, "scan", fake_scan)
    monkeypatch.setattr(sar.requests, "post", fake_post)
    return calls


def results_by_key(body):
    return {(result["ACDCTerminal"], result["contingency"]): result for result in body["results"]}


def test_request_scenario_time_is_utc():
    request = SarRequest(scenario_time="2026-09-23T22:30:00", time_horizon="ID", run_id="09")
    assert request.scenario_time.tzinfo == timezone.utc
    local = datetime(2026, 9, 24, 0, 30, tzinfo=timezone(timedelta(hours=2)))
    request = SarRequest(scenario_time=local, time_horizon="ID", run_id="09")
    assert request.scenario_time == datetime(2026, 9, 23, 22, 30, tzinfo=timezone.utc)


def test_request_requires_run_id():
    with pytest.raises(ValidationError):
        SarRequest(scenario_time="2026-09-23T22:30:00Z", time_horizon="ID")


def test_queries_requested_scenario_horizon_and_run(calls):
    generate_sar(REQUEST, elastic=ELASTIC)

    filters = calls["query"]["query"]["bool"]["filter"]
    scenario_time = "2026-09-23T22:30:00+00:00"
    assert {"range": {sar.SCENARIO_TIME_FIELD: {"gte": scenario_time, "lte": scenario_time}}} in filters
    assert {"term": {sar.TIME_HORIZON_FIELD: "ID"}} in filters
    assert {"term": {sar.RUN_ID_FIELD: "09"}} in filters
    assert calls["index"] == f"{sar.conf.elastic_results_index}*"


def test_request_sent_to_service_with_latest_optimized_results(calls):
    assert generate_sar(REQUEST, elastic=ELASTIC) == b"<sar/>"

    body = calls["body"]
    assert calls["url"] == f"{sar.conf.nc_profile_service_url.rstrip('/')}{sar.SAR_ENDPOINT}"
    assert {key: body[key] for key in ("scenario_time", "time_horizon", "run_id", "service")} == {
        "scenario_time": "2026-09-23T22:30:00Z", "time_horizon": "ID", "run_id": "09", "service": "RAO"}
    # Skipped: result without network action and the older run. 3w transformer leg is reported on its base element
    assert set(results_by_key(body)) == {(LINE, CO1), (TRAFO, CO1), (LINE, CO2), (LINE, None)}


def test_result_values_are_post_rao_values_at_cnec_instant(calls):
    generate_sar(REQUEST, elastic=ELASTIC)

    assert results_by_key(calls["body"])[(LINE, CO1)] == {
        "isViolation": False,
        "value": 90.0,
        "absoluteValue": 900.0,
        "valueA": 900.0,
        "valueW": 450.0,
        "ACDCTerminal": LINE,
        "OperationalLimitValue": 1000.0,
        "ReportedByRegion": "10XLT-LITGRID--Q",
        "EquipmentName": "Line A",
        "Substation": "Substation A",
        "contingency": CO1,
        "remedialAction": "RA-1",
    }


def test_violation_is_flagged_and_limit_follows_flow_direction(calls):
    generate_sar(REQUEST, elastic=ELASTIC)
    results = results_by_key(calls["body"])

    reverse = results[(LINE, CO2)]
    assert (reverse["isViolation"], reverse["value"]) == (True, 120.0)
    assert (reverse["valueA"], reverse["absoluteValue"], reverse["OperationalLimitValue"]) == (-1200.0, 1200.0, -1000.0)
    assert results[(TRAFO, CO1)]["isViolation"] is True
    assert results[(LINE, CO1)]["isViolation"] is False


def test_preventive_cnec_has_no_contingency(calls):
    generate_sar(REQUEST, elastic=ELASTIC)

    preventive = results_by_key(calls["body"])[(LINE, None)]
    assert (preventive["value"], preventive["remedialAction"]) == (50.0, "RA-3")


def test_no_results_returns_none_without_calling_service(calls):
    calls["hits"] = []

    assert generate_sar(REQUEST, elastic=ELASTIC) is None
    assert "url" not in calls


@pytest.mark.parametrize("keep_column", [True, False], ids=["action_empty", "action_column_missing"])
def test_no_network_actions_returns_none_without_calling_service(calls, keep_column):
    """The networkActionId column does not exist in the results if no network action was applied in any of them."""
    hits = [{key: value for key, value in hit.items() if key != "networkActionId"} for hit in HITS]
    calls["hits"] = [{**hit, "networkActionId": None} for hit in hits] if keep_column else hits

    assert generate_sar(REQUEST, elastic=ELASTIC) is None
    assert "url" not in calls


def test_service_error_is_raised(calls):
    calls["status"] = 422

    with pytest.raises(requests.HTTPError):
        generate_sar(REQUEST, elastic=ELASTIC)
