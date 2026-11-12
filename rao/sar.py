"""Generate a SecurityAnalysisResult (SAR) profile from RAO results stored in Elastic.

Reads the RAO result documents of one scenario timestamp, time horizon and run id from the RAO results index, keeps
the latest optimization run of each contingency and the CNECs that have a remedial action applied, and sends their
post-RAO values to nc-profile-service, which generates the SAR profile.

    sar = generate_sar(SarRequest(scenario_time="2026-09-23T22:30:00Z", time_horizon="ID", run_id="09"))
"""
from datetime import datetime, timezone
import pandas as pd
import pytz
import requests
from elasticsearch.helpers import scan
from loguru import logger
from pydantic import BaseModel, Field
from pydantic import field_validator
from integrations.elastic import Elastic
from rao.settings import RaoHandlerSettings

conf = RaoHandlerSettings()

SAR_ENDPOINT = "/api/v1/security-analysis-result/generate"

# Fields of the RAO result documents identifying the requested run, keyword subfields are used for exact matching
SCENARIO_TIME_FIELD = "@scenario_timestamp"
TIME_HORIZON_FIELD = "@time_horizon.keyword"
RUN_ID_FIELD = "@run_id.keyword"


class SarRequest(BaseModel):
    """SAR to generate: scenario_time, time_horizon and run_id select the RAO results.

    The model is also the nc-profile-service request, generate_sar fills `results` with the post-RAO values.
    Naive timestamps are taken as UTC, strings must be ISO formatted.
    """
    scenario_time: datetime
    time_horizon: str
    run_id: str
    service: str = "RAO"
    results: list = Field(default_factory=list)

    @field_validator("scenario_time")
    @classmethod
    def as_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


def fetch_results(elastic: Elastic, request: SarRequest) -> pd.DataFrame:
    """Get all RAO result documents of the requested scenario timestamp, time horizon and run id, flattened to columns."""
    scenario_time = request.scenario_time.isoformat()
    query = {"bool": {"filter": [
        {"range": {SCENARIO_TIME_FIELD: {"gte": scenario_time, "lte": scenario_time}}},
        {"term": {TIME_HORIZON_FIELD: request.time_horizon}},
        {"term": {RUN_ID_FIELD: request.run_id}},
        {"exists": {"field": "rmq.batch-id"}},
    ]}}
    index = f"{conf.elastic_results_index}*"  # monthly rollover indices
    logger.info(f"Querying {index} for scenario {scenario_time} [{request.time_horizon}]")
    hits = scan(elastic.client, index=index, query={"query": query})
    results = pd.json_normalize([hit["_source"] for hit in hits])
    logger.info(f"Retrieved {len(results)} result documents")

    return results


def keep_latest_runs(results: pd.DataFrame) -> pd.DataFrame:
    """Keep only the latest optimization run of each contingency, a scenario can be optimized more than once."""
    results = results.assign(**{"@timestamp": pd.to_datetime(results["@timestamp"])})
    latest = (results.sort_values("@timestamp")
                     .drop_duplicates(subset="rmq.contingency-id", keep="last")[["rmq.contingency-id", "rmq.batch-id"]])

    return results.merge(latest, on=["rmq.contingency-id", "rmq.batch-id"])


def _number(value) -> float | None:
    return None if pd.isna(value) else round(float(value), 2)


def request_profile(payload) -> bytes:
    """Send the payload to nc-profile-service and return the generated SAR file content.

    Certificate verification of the service is disabled. Raises requests.HTTPError if the service responds with error.
    """
    url = f"{conf.nc_profile_service_url.rstrip('/')}{SAR_ENDPOINT}"
    response = requests.post(url,
                             json=payload,
                             timeout=conf.nc_profile_service_timeout,
                             verify=False)
    if response.status_code == 422:
        logger.error(f"nc-profile-service rejected SAR request: {response.text}")
    response.raise_for_status()

    return response.content


def generate_sar(request: SarRequest, elastic: Elastic | None = None) -> bytes | None:
    """Generate SAR profile from the latest RAO results of the requested scenario timestamp, time horizon and run id.

    Only CNECs with an applied remedial action are reported, one entry per result document. An entry has the
    post-RAO flow and loading (percent of the ampere limit) at the CNEC's own instant, the contingency and the
    remedial action it was activated for. Returns the profile file content or None if there is nothing to report:
    no RAO results for the request or no remedial action applied in them.
    """
    elastic = elastic or Elastic()

    results = fetch_results(elastic, request)
    if results.empty:
        logger.warning(f"No RAO results found for {request.scenario_time.isoformat()} [{request.time_horizon}-{request.run_id}]")
        return None

    # Keep the latest run of each contingency and warn about failed computations
    results = keep_latest_runs(results)
    failed = results.loc[results["computationStatus"] == "failure", "rmq.contingency-id"].unique()
    if len(failed):
        logger.warning(f"Optimization failed for contingencies, reported values are not optimized: {failed.tolist()}")

    # Filter to only optimized RA documents, the column does not exist if no network action was applied in any of them
    if "networkActionId" in results.columns:
        results = results[results["networkActionId"].notna()]
    else:
        results = results.iloc[0:0]
    if results.empty:
        logger.warning(f"No remedial actions applied in RAO results of {request.scenario_time.isoformat()} "
                       f"[{request.time_horizon}-{request.run_id}], nothing to report")
        return None

    # Remove transformers Leg definitions from CNEC identifiers
    results = results.assign(**{"cnec.networkElementId": results["cnec.networkElementId"].str.replace(r"-Leg\d+$", "", regex=True)})

    # Generate request payload
    payload = request.model_dump(mode="json")
    for row in results.to_dict("records"):
        _side = row["cnec.thresholds.side"]
        _instant = row["cnec.instant"]
        _loading = row[f"cnecResults.{_instant}.ampere.side{_side}.loading"] * 100
        _flow = row[f"cnecResults.{_instant}.ampere.side{_side}.flow"]
        _terminal = row["cnec.networkElementId"].lstrip("_")
        _operational_limit = row["cnec.thresholds.max"] if _flow > 0 else row["cnec.thresholds.min"]
        attributes = dict(
            isViolation=bool(_loading > 100),
            value=_number(_loading),
            absoluteValue=_number(abs(_flow)),
            valueA=_number(_flow),
            valueW=_number(row[f"cnecResults.{_instant}.megawatt.side{_side}.flow"]),
            ACDCTerminal=_terminal,
            OperationalLimitValue=_number(_operational_limit),
            ReportedByRegion=row["cnec.operator"].split("/")[-1],
            EquipmentName=row["cnec.name"],
            Substation=row["cnec.substation"],
            contingency=row["activatedStates.contingency"],
            remedialAction=row["networkActionId"],
        )
        payload["results"].append(attributes)

    return request_profile(payload)


if __name__ == "__main__":
    tz = pytz.timezone("Europe/Brussels")
    request = SarRequest(scenario_time=tz.localize(datetime(2026, 11, 12, 9, 30)),
                         time_horizon="ID", run_id="09")
    response = generate_sar(request=request)
    from lxml import etree
    root = etree.fromstring(response)
    print(etree.tostring(root, pretty_print=True, encoding='unicode'))

    # Write to file
    # etree.ElementTree(root).write("sar_output.xml", pretty_print=True, encoding='UTF-8')
