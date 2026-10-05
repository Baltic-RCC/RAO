"""
Input adapters for redispatching remedial-action rows.

Every source converts its native format into a list of :class:`RedispatchRow` records,
so the CRAC mapping in :mod:`rao.crac.costly_ra.redispatch` does not depend on where the
rows come from:
    - NcRemedialActionRowSource: NC RemedialAction profile (RA list from object storage),
      loaded into triplets the same way as for the topology action CRAC building
    - CsvRowSource / DataFrameRowSource: RCC remedial-action export
Ranges and availability are taken from the remedial action list only, never from the
network model.
"""
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Protocol, runtime_checkable
import pandas as pd
from loguru import logger

DIRECTION_UP = "up"
DIRECTION_DOWN = "down"
DIRECTION_NONE = "none"

REQUIRED_COLUMNS = (
    "kind", "ra_name", "available", "area", "party", "alteration_type", "alteration_name",
    "property", "grid_element_id", "normal_value", "direction", "value_kind",
)

_TRUE_VALUES = {"true", "1", "yes", "y"}
_FALSE_VALUES = {"false", "0", "no", "n"}


class RowParseError(ValueError):
    """Raised when an input row cannot be converted to a RedispatchRow."""


@dataclass(frozen=True)
class RedispatchRow:
    """One remedial-action row of the RCC export (one direction of one unit)."""
    kind: str
    ra_name: str
    available: bool
    area: str
    party: str
    alteration_type: str
    alteration_name: str
    property: str
    grid_element_id: str
    normal_value: float | None  # None: the remedial action list holds no range value
    direction: str
    value_kind: str


@runtime_checkable
class RowSource(Protocol):
    """Adapter interface: any source that yields typed redispatch rows."""

    def read(self) -> list[RedispatchRow]:
        ...


def _parse_bool(value: str, row_number: int) -> bool:
    text = str(value).strip().lower()
    if text in _TRUE_VALUES:
        return True
    if text in _FALSE_VALUES:
        return False
    raise RowParseError(f"Row {row_number}: invalid 'available' value '{value}', expected true/false")


def _parse_float(value: str, row_number: int) -> float | None:
    if not str(value).strip():
        return None
    try:
        return float(str(value).strip())
    except ValueError:
        raise RowParseError(f"Row {row_number}: invalid 'normal_value' value '{value}', expected a number") from None


class DataFrameRowSource:
    """Reads rows from a DataFrame with the RCC export columns (all values as text)."""

    def __init__(self, data: pd.DataFrame):
        self.data = data

    def read(self) -> list[RedispatchRow]:
        missing = [column for column in REQUIRED_COLUMNS if column not in self.data.columns]
        if missing:
            raise RowParseError(f"Input rows are missing required columns: {missing}")

        rows = []
        # Row numbers are reported 1-based after the header, as seen in a spreadsheet/CSV editor
        for row_number, record in enumerate(self.data.to_dict("records"), start=2):
            text = {column: str(record[column]).strip() for column in REQUIRED_COLUMNS}
            if not text["grid_element_id"]:
                raise RowParseError(f"Row {row_number}: 'grid_element_id' is empty ({text['ra_name']})")
            rows.append(RedispatchRow(
                kind=text["kind"],
                ra_name=text["ra_name"],
                available=_parse_bool(text["available"], row_number),
                area=text["area"],
                party=text["party"],
                alteration_type=text["alteration_type"],
                alteration_name=text["alteration_name"],
                property=text["property"],
                grid_element_id=text["grid_element_id"],
                normal_value=_parse_float(text["normal_value"], row_number),
                direction=text["direction"].lower(),
                value_kind=text["value_kind"].lower(),
            ))
        return rows


class CsvRowSource(DataFrameRowSource):
    """Reads rows from an RCC remedial-action export in CSV format."""

    def __init__(self, source: str | Path | IO, **read_csv_kwargs):
        self.source = source
        # Everything is read as text, conversion and validation happen in read()
        data = pd.read_csv(source, dtype=str, keep_default_na=False, **read_csv_kwargs)
        super().__init__(data)

    def read(self) -> list[RedispatchRow]:
        rows = super().read()
        logger.info(f"Read {len(rows)} remedial action rows from CSV: {getattr(self.source, 'name', self.source)}")
        return rows


def _enum_value(value) -> str:
    """'https://cim4.eu/ns/nc#RelativeDirectionKind.up' or 'RelativeDirectionKind.up' -> 'up'"""
    return str(value).split("#")[-1].split(".")[-1] if _present(value) else ""


def _last_segment(value) -> str:
    """'https://energy.referencedata.eu/PropertyReference/RotatingMachine.p' -> 'RotatingMachine.p'"""
    return str(value).split("/")[-1] if _present(value) else ""


def _direction_from_name(name: str) -> str:
    """'RA_RD_ME_G1_UP' -> 'up', 'RA_RD_ME_G1_DOWN' -> 'down'"""
    upper = name.upper()
    if upper.endswith("_UP"):
        return DIRECTION_UP
    if upper.endswith("_DOWN"):
        return DIRECTION_DOWN
    return ""


def _present(value) -> bool:
    return value is not None and not (isinstance(value, float) and pd.isna(value)) and value is not pd.NA


def _text(value) -> str:
    return str(value).strip() if _present(value) else ""


class NcRemedialActionRowSource:
    """
    Reads redispatching rows from the NC RemedialAction profile loaded into triplets
    (pd.read_RDF), as retrieved for the regular CRAC building.

    One row is produced per RotatingMachineAction and StaticPropertyRange:
        GridStateAlterationRemedialAction  -> ra_name, kind, party, available (normalAvailable)
        RotatingMachineAction              -> alteration_name, grid_element_id (RotatingMachine),
                                              available (normalEnabled)
        StaticPropertyRange                -> property, normal_value, direction, value_kind
    An alteration without StaticPropertyRange (or without normalValue) gives a row with
    normal_value None; its direction is then taken from the _UP/_DOWN suffix of the RA name.
    """

    def __init__(self, data: pd.DataFrame):
        self.data = data

    def _table(self, object_type: str) -> pd.DataFrame:
        try:
            table = self.data.type_tableview(object_type, string_to_number=False)
        except Exception:
            table = None
        if table is None or table.empty:
            return pd.DataFrame()
        return table

    def read(self) -> list[RedispatchRow]:
        remedial_actions = self._table("GridStateAlterationRemedialAction")
        alterations = self._table("RotatingMachineAction")
        ranges = self._table("StaticPropertyRange")
        if alterations.empty:
            logger.info("No RotatingMachineAction found in remedial action data")
            return []

        ranges_by_alteration = {}
        if "RangeConstraint.GridStateAlteration" in ranges.columns:
            for record in ranges.to_dict("records"):
                ranges_by_alteration.setdefault(record["RangeConstraint.GridStateAlteration"], []).append(record)
        remedial_actions = remedial_actions.to_dict("index")

        rows = []
        for alteration_id, alteration in alterations.to_dict("index").items():
            name = _text(alteration.get("IdentifiedObject.name")) or alteration_id
            remedial_action = remedial_actions.get(alteration.get("GridStateAlteration.GridStateAlterationRemedialAction"))
            if remedial_action is None:
                logger.warning(f"RotatingMachineAction {name} has no GridStateAlterationRemedialAction, ignored")
                continue
            if not _text(alteration.get("RotatingMachineAction.RotatingMachine")):
                logger.warning(f"RotatingMachineAction {name} has no RotatingMachine, ignored")
                continue
            ra_name = _text(remedial_action.get("IdentifiedObject.name"))
            # Without a StaticPropertyRange the action still counts for its direction (from the
            # RA name suffix) with no range value, the mapping decides how to handle that
            alteration_ranges = ranges_by_alteration.get(alteration_id) or [{}]

            # Missing flags are treated as true, as in the profile defaults
            available = (_text(remedial_action.get("RemedialAction.normalAvailable")).lower() != "false"
                         and _text(alteration.get("GridStateAlteration.normalEnabled")).lower() != "false")

            for property_range in alteration_ranges:
                normal_value = property_range.get("RangeConstraint.normalValue")
                if not _text(normal_value):
                    normal_value = None
                else:
                    try:
                        normal_value = float(normal_value)
                    except (TypeError, ValueError):
                        raise RowParseError(f"RotatingMachineAction {name}: invalid StaticPropertyRange normalValue "
                                            f"'{normal_value}'") from None
                direction = (_enum_value(property_range.get("RangeConstraint.direction")).lower()
                             or _direction_from_name(ra_name))
                rows.append(RedispatchRow(
                    kind=_enum_value(remedial_action.get("RemedialAction.kind")),
                    ra_name=ra_name,
                    available=available,
                    area=_last_segment(remedial_action.get("RemedialAction.AppointedToRegion")),
                    party=_text(remedial_action.get("RemedialAction.RemedialActionSystemOperator")),
                    alteration_type="RotatingMachineAction",
                    alteration_name=name,
                    property=_last_segment(property_range.get("StaticPropertyRange.PropertyReference")
                                                 or alteration.get("GridStateAlteration.PropertyReference")),
                    grid_element_id=_text(alteration.get("RotatingMachineAction.RotatingMachine")),
                    normal_value=normal_value,
                    direction=direction,
                    value_kind=_enum_value(property_range.get("RangeConstraint.valueKind")).lower(),
                ))

        logger.info(f"Read {len(rows)} RotatingMachineAction range rows from remedial action data")
        return rows
