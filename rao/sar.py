"""Generate a SecurityAnalysisResult (SAR) profile from RAO results stored in Elastic.

Reads the post-RAO flow of every CNEC optimized for one scenario timestamp and time horizon from the RAO results
index and lets nc-profile-service serialize them as PowerFlowResults.

    sar = generate_sar(SarRequest(scenario_time="2026-09-23T22:30:00Z", time_horizon="ID"))
"""
from datetime import datetime, timedelta, timezone
import numpy as np
import pandas as pd
import requests
from elasticsearch.helpers import scan
from loguru import logger
from pydantic import BaseModel
from pydantic import field_validator
from integrations.elastic import Elastic
from rao.settings import RaoHandlerSettings

conf = RaoHandlerSettings()

SAR_ENDPOINT = "/api/v1/profiles/security-analysis-result"

# Fields of the documents written by HandlerOptimizer (rmq holds the headers of the optimization task)
SCENARIO_TIME_FIELD = "rmq.scenario-time"
TIME_HORIZON_FIELD = "rmq.time-horizon.keyword"

# Units a CNEC can be limited in, the first available one is used for the reported loading percentage
UNIT_PRIORITY = {"ampere": 0, "megawatt": 1}
UUID_PATTERN = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"


class SarRequest(BaseModel):
    """What to generate. Naive timestamps are taken as UTC, strings must be ISO formatted."""
    scenario_time: datetime
    time_horizon: str
    publisher: str = conf.sar_publisher
    reported_by_region: str = conf.sar_reported_by_region
    period_minutes: int = conf.sar_period_minutes

    @field_validator("scenario_time")
    @classmethod
    def as_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


# nc-profile-service request, field names are the ones of the service API
class SarHeader(BaseModel):
    scenarioTime: datetime
    startDate: datetime
    endDate: datetime
    publisher: str
    wasGeneratedBy: str


class PowerFlowResult(BaseModel):
    atTime: datetime
    isViolation: bool
    value: float | None  # loading in percent of the operational limit
    valueA: float | None
    valueW: float | None
    ACDCTerminal: str
    OperationalLimitValue: float | None
    ReportedByRegion: str
    EquipmentName: str | None


class ContingencyPowerFlowResult(PowerFlowResult):
    Contingency: str


class SarProfile(BaseModel):
    header: SarHeader
    BaseCasePowerFlowResult: list[PowerFlowResult]
    ContingencyPowerFlowResult: list[ContingencyPowerFlowResult]


def fetch_results(elastic: Elastic, request: SarRequest) -> pd.DataFrame:
    """Get all RAO result documents of the requested scenario timestamp and time horizon, flattened to columns."""
    scenario_time = request.scenario_time.isoformat()
    query = {"bool": {"filter": [
        {"range": {SCENARIO_TIME_FIELD: {"gte": scenario_time, "lte": scenario_time}}},
        {"term": {TIME_HORIZON_FIELD: request.time_horizon}},
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


def _at_own_instant(results: pd.DataFrame, metric: str) -> pd.Series:
    """Read cnecResults.<instant>.<unit>.side1.<metric> at the CNEC's own instant (preventive, curative...) and unit."""
    values = pd.Series(np.nan, index=results.index)
    for (instant, unit), rows in results.groupby(["cnec.instant", "cnec.thresholds.unit"]).groups.items():
        column = f"cnecResults.{instant}.{unit}.side1.{metric}"
        if column in results.columns:
            values[rows] = results.loc[rows, column]

    return values


def post_rao_values(results: pd.DataFrame) -> pd.DataFrame:
    """Reduce result documents to the worst post-RAO value per contingency, element and unit.

    Documents are repeated per threshold and per activated network action, but the flow is the same in every
    repetition. Base case CNECs are repeated in every contingency run and 3w transformers report per leg, so the
    worst value is kept. The base case has an empty contingency.
    """
    flow = _at_own_instant(results, "flow")
    loading = _at_own_instant(results, "loading")
    contingency = results.get("cnec.contingencyId", pd.Series(index=results.index, dtype=object))
    values = pd.DataFrame({
        "contingency": contingency.fillna("").astype(str).str.lstrip("_"),
        "element": results["cnec.networkElementId"].astype(str)
                   .str.replace(r"-Leg\d+$", "", regex=True)
                   .str.lstrip("_"),
        "name": results["cnec.name"],
        "unit": results["cnec.thresholds.unit"],
        "flow": flow,
        "loading": loading.abs(),
        "limit": np.where(flow >= 0, results["cnec.thresholds.max"], results["cnec.thresholds.min"]),
    }).dropna(subset=["flow"])

    values = values.sort_values("loading", ascending=False, na_position="last")

    return values.drop_duplicates(subset=["contingency", "element", "unit"], keep="first")


def _number(value) -> float | None:
    return None if pd.isna(value) else round(float(value), 2)


def build_profile(values: pd.DataFrame, request: SarRequest) -> SarProfile:
    """Convert post-RAO values to one PowerFlowResult per element and contingency."""
    key = ["contingency", "element"]
    values = values[values["unit"].isin(UNIT_PRIORITY)]
    valid = (values["element"].str.fullmatch(UUID_PATTERN)
             & (values["contingency"].eq("") | values["contingency"].str.fullmatch(UUID_PATTERN)))
    if not valid.all():
        skipped = values.loc[~valid, "element"].unique()
        logger.warning(f"Skipping {len(skipped)} elements without mRID identifier, e.g. {skipped[:5].tolist()}")
    values = values[valid]

    # One row per element: the preferred unit gives the limit and loading, flows are reported in both units
    flows = values.pivot(index=key, columns="unit", values="flow").reindex(columns=list(UNIT_PRIORITY))
    reported = (values.sort_values("unit", key=lambda units: units.map(UNIT_PRIORITY))
                      .drop_duplicates(subset=key, keep="first")
                      .set_index(key)
                      .join(flows))

    base_case, contingency_cases = [], []
    for row in reported.reset_index().itertuples():
        loading = row.loading * 100
        attributes = dict(
            atTime=request.scenario_time,
            isViolation=bool(loading > 100),
            value=_number(loading),
            valueA=_number(abs(row.ampere)),
            valueW=_number(row.megawatt),
            ACDCTerminal=row.element,
            OperationalLimitValue=_number(abs(row.limit)),
            ReportedByRegion=request.reported_by_region,
            EquipmentName=row.name if isinstance(row.name, str) else None,
        )
        if row.contingency:
            contingency_cases.append(ContingencyPowerFlowResult(**attributes, Contingency=row.contingency))
        else:
            base_case.append(PowerFlowResult(**attributes))

    header = SarHeader(
        scenarioTime=request.scenario_time,
        startDate=request.scenario_time,
        endDate=request.scenario_time + timedelta(minutes=request.period_minutes),
        publisher=request.publisher,
        wasGeneratedBy=f"RAO-{request.time_horizon}-SAR",
    )

    return SarProfile(header=header, BaseCasePowerFlowResult=base_case, ContingencyPowerFlowResult=contingency_cases)


def request_profile(profile: SarProfile) -> bytes:
    """Let nc-profile-service serialize the profile, returns the SAR file content."""
    url = f"{conf.nc_profile_service_url.rstrip('/')}{SAR_ENDPOINT}"
    response = requests.post(url, json=profile.model_dump(mode="json", exclude_none=True),
                             timeout=conf.nc_profile_service_timeout)
    if response.status_code == 422:
        logger.error(f"nc-profile-service rejected SAR request: {response.text}")
    response.raise_for_status()

    return response.content


def generate_sar(request: SarRequest, elastic: Elastic | None = None) -> bytes | None:
    """Generate SAR profile from the latest RAO results of the requested scenario timestamp and time horizon.

    Returns the profile file content or None if there are no RAO results for the request.
    """
    elastic = elastic or Elastic()

    results = fetch_results(elastic, request)
    if results.empty:
        logger.warning(f"No RAO results found for {request.scenario_time.isoformat()} [{request.time_horizon}]")
        return None

    results = keep_latest_runs(results)
    failed = results.loc[results["computationStatus"] == "failure", "rmq.contingency-id"].unique()
    if len(failed):
        logger.warning(f"Optimization failed for contingencies, reported values are not optimized: {failed.tolist()}")

    profile = build_profile(post_rao_values(results), request)
    all_results = profile.BaseCasePowerFlowResult + profile.ContingencyPowerFlowResult
    logger.info(f"Built {len(all_results)} power flow results for {results['rmq.contingency-id'].nunique()} "
                f"contingencies, violations remaining: {sum(result.isViolation for result in all_results)}")

    return request_profile(profile)
