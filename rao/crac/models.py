from loguru import logger
from pydantic import BaseModel, Field, field_serializer, field_validator, AliasChoices
from typing import Optional, List, Any, Dict, Literal
import uuid


class Contingency(BaseModel):
    id: str
    name: str
    networkElementsIds: List[str]

    @field_serializer("networkElementsIds", when_used='unless-none')
    def serialize_with_prefix(self, value: List[str]) -> List[str]:
        return [f"_{val}" for val in value]


class Threshold(BaseModel):
    unit: Literal['megawatt', 'ampere', 'percent_imax'] = 'ampere'  # Default unit is 'ampere', can be adjusted if needed
    min: float = 0
    max: float = 0
    side: int = 1  # Default side is 1, can be adjusted if needed


class Cnec(BaseModel):
    id: str
    name: str
    description: str = Field(exclude=True)
    networkElementId: str
    operator: str
    thresholds: List[Threshold]
    instant: Literal["preventive", "outage", "curative"] = "preventive"
    optimized: bool = True
    monitored: bool = False
    nominalV: List[float] = [330.0]  # Default nominal voltage, can be adjusted if needed
    contingencyId: Optional[str] = None

    @field_serializer("networkElementId", when_used='unless-none')
    def serialize_with_prefix(self, value: str) -> str:
        return f"_{value}"


class FlowCnec(Cnec):
    pass


class TerminalsAction(BaseModel):
    networkElementId: str
    actionType: Literal['open', 'close'] | float = Field(default="open", validation_alias=AliasChoices("normalValue", "actionType"))

    @field_validator("actionType", mode='after')
    @classmethod
    def map_to_string_open_close(cls, value: Literal['open', 'close'] | float) -> str:
        if isinstance(value, float):
            if value:
                return "open"
            else:
                return "close"
        return value

    @field_serializer("networkElementId", when_used='unless-none')
    def serialize_with_prefix(self, value: str) -> str:
        return f"_{value}"


class ShuntCompensatorPositionAction(BaseModel):
    networkElementId: str
    sectionCount: int = Field(default=0, validation_alias=AliasChoices("normalValue", "sectionCount"))

    @field_serializer("networkElementId", when_used='unless-none')
    def serialize_with_prefix(self, value: str) -> str:
        return f"_{value}"


class NetworkAction(BaseModel):
    id: str
    name: str
    operator: str
    onInstantUsageRules: List[Dict]
    terminalsConnectionActions: Optional[List[TerminalsAction]] = None
    shuntCompensatorPositionActions: Optional[List[ShuntCompensatorPositionAction]] = None

    @field_validator("terminalsConnectionActions", "shuntCompensatorPositionActions", mode='before')
    @classmethod
    def empty_list_to_none(cls, value: List[Any] | None) -> List[Any] | None:
        # Checks if the value is a list and empty, returns None
        if isinstance(value, list) and not value:
            return None
        return value


class Crac(BaseModel):

    class Config:
        # Serialize names with dashes if they have underscores
        alias_generator = lambda s: s.replace("_", "-")
        populate_by_name = True

    type: str = "CRAC"
    version: str = "2.7"
    info: str = "TC1 CRAC Example"
    id: str = "LS_unsecure"
    name: str = "LS_unsecure"
    instants: List[Dict] = Field(default_factory=lambda: [
        {"id": "preventive", "kind": "PREVENTIVE"},
        {"id": "outage", "kind": "OUTAGE"},
        {"id": "curative", "kind": "CURATIVE"}
    ])
    ra_usage_limits_per_instant: List[Any] = Field(default_factory=list)
    networkElementsNamePerId: Dict = Field(default_factory=dict)
    contingencies: List[Contingency] = Field(default_factory=list)
    flowCnecs: List[FlowCnec] = Field(default_factory=list)
    networkActions: List[NetworkAction] = Field(default_factory=list)

    @field_serializer("flowCnecs", mode='plain')
    def exclude_3w_transformer_from_flow_cnecs(self, values: List[FlowCnec]) -> List[FlowCnec]:
        # TODO TEMPORARY FILTER - remove after September release
        logger.warning(f"[TEMPORARY] Excluding 3W transformers from serialized CNECs for operator: ELERING")
        result = []
        for cnec in values:
            if "AT" in cnec.name and "10X1001A1001A39W" in cnec.operator:
                logger.warning(f"3W transformer CNEC excluded: {cnec.name} [{cnec.instant}]")
                continue
            else:
                result.append(cnec)

        return result


if __name__ == "__main__":
    flow_cnec = FlowCnec(
        id="example_cnec",
        name="Example CNEC",
        networkElementId="NE12345",
        operator="OperatorX",
        thresholds=[{"unit": "megawatt", "min": -350, "max": 350, "side": 1}],
        instant="preventive",
        nominalV=[330.0],
    )

