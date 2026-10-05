"""
Input adapters for redispatching remedial-action rows.

Every source converts its native format into a list of :class:`RedispatchRow` records,
so the CRAC mapping in :mod:`rao.crac.redispatch.builder` does not depend on where the
rows come from. CSV (RCC remedial-action export) is the first adapter; others (e.g. the
NC/CSA RemedialAction XML profile) only need to implement :class:`RowSource`.
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
    normal_value: float
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


def _parse_float(value: str, row_number: int) -> float:
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
