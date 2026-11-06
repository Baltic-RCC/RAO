import uuid
from types import SimpleNamespace
from datetime import datetime, timezone, timedelta
import pytest
from rao import sar
from rao.sar import SarRequest, generate_sar

LINE, TRAFO, CO = (str(uuid.uuid4()) for _ in range(3))
ELASTIC = SimpleNamespace(client=None)  # scan is replaced, the client is only passed through
REQUEST = SarRequest(scenario_time="2026-09-23T22:30:00Z", time_horizon="ID")


def doc(element, instant, flow, unit="ampere", batch="b2", timestamp="2026-11-05T11:00:00"):
    """RAO result document as stored by HandlerOptimizer: dotted column names, task headers under rmq."""
    return {
        "@timestamp": timestamp,
        "rmq": {"batch-id": batch, "contingency-id": CO},
        "computationStatus": "default",
        "cnec.name": element[:4],
        "cnec.networkElementId": f"_{element}",
        "cnec.contingencyId": CO if instant == "curative" else None,
        "cnec.instant": instant,
        "cnec.thresholds.unit": unit,
        "cnec.thresholds.min": -1000,
        "cnec.thresholds.max": 1000,
        f"cnecResults.initial.{unit}.side1.flow": 5000,
        f"cnecResults.{instant}.{unit}.side1.flow": flow,
        f"cnecResults.{instant}.{unit}.side1.loading": abs(flow) / 1000,
    }


HITS = [
    doc(LINE, "curative", 900), doc(LINE, "curative", 900),                      # repeated per network action
    doc(LINE, "curative", -450, unit="megawatt"),
    doc(f"{TRAFO}-Leg1", "curative", 800), doc(f"{TRAFO}-Leg2", "curative", 1100),
    doc(LINE, "preventive", 500),
    doc(LINE, "curative", 2000, batch="b1", timestamp="2026-11-05T10:00:00"),    # older run is ignored
    doc("EQUIPMENT-WITHOUT-MRID", "curative", 100),
]


class Response:
    status_code = 200
    content = b"<sar/>"
    text = ""

    def raise_for_status(self):
        pass


@pytest.fixture
def calls(monkeypatch):
    calls = {}

    def fake_scan(client, index, query):
        calls.update(index=index, query=query)
        return [{"_source": hit} for hit in calls.get("hits", HITS)]

    def fake_post(url, json, timeout):
        calls.update(url=url, body=json, timeout=timeout)
        return Response()

    monkeypatch.setattr(sar, "scan", fake_scan)
    monkeypatch.setattr(sar.requests, "post", fake_post)
    return calls


def test_request_scenario_time_is_utc():
    assert SarRequest(scenario_time="2026-09-23T22:30:00", time_horizon="ID").scenario_time.tzinfo == timezone.utc
    local = datetime(2026, 9, 24, 0, 30, tzinfo=timezone(timedelta(hours=2)))
    assert SarRequest(scenario_time=local, time_horizon="ID").scenario_time == datetime(2026, 9, 23, 22, 30, tzinfo=timezone.utc)


def test_queries_requested_scenario_and_horizon(calls):
    generate_sar(REQUEST, elastic=ELASTIC)

    filters = calls["query"]["query"]["bool"]["filter"]
    assert {"term": {"rmq.time-horizon.keyword": "ID"}} in filters
    assert {"range": {"rmq.scenario-time": {"gte": "2026-09-23T22:30:00+00:00", "lte": "2026-09-23T22:30:00+00:00"}}} in filters
    assert calls["index"] == "rao-results*"


def test_profile_has_latest_post_rao_value_per_element(calls):
    sar_file = generate_sar(REQUEST, elastic=ELASTIC)

    assert sar_file == b"<sar/>"
    assert calls["url"] == "http://nc-profile-service:8000/api/v1/profiles/security-analysis-result"
    body = calls["body"]
    assert body["header"]["scenarioTime"] == "2026-09-23T22:30:00Z"
    assert body["header"]["endDate"] == "2026-09-23T22:45:00Z"
    assert body["header"]["wasGeneratedBy"] == "RAO-ID-SAR"

    contingency_results = {result["ACDCTerminal"]: result for result in body["ContingencyPowerFlowResult"]}
    assert set(contingency_results) == {LINE, TRAFO}  # element without mRID is skipped
    assert {result["Contingency"] for result in contingency_results.values()} == {CO}
    assert (contingency_results[LINE]["value"], contingency_results[LINE]["isViolation"]) == (90, False)
    assert (contingency_results[LINE]["valueA"], contingency_results[LINE]["valueW"]) == (900, -450)
    assert contingency_results[LINE]["OperationalLimitValue"] == 1000
    assert (contingency_results[TRAFO]["value"], contingency_results[TRAFO]["isViolation"]) == (110, True)  # worst leg

    base_case = body["BaseCasePowerFlowResult"]
    assert [(result["ACDCTerminal"], result["value"]) for result in base_case] == [(LINE, 50)]
    assert "Contingency" not in base_case[0]


def test_no_results_returns_none_without_calling_service(calls):
    calls["hits"] = []

    assert generate_sar(REQUEST, elastic=ELASTIC) is None
    assert "url" not in calls
