"""Shared builders for synthetic test data.

Triplet frames mirror what ``pd.read_RDF`` returns (ID/KEY/VALUE/INSTANCE_ID, pandas
``string`` dtype, IDs without the leading ``_``) so that CracBuilder sees missing
attributes as ``pd.NA`` exactly as in production.
"""
from io import BytesIO
from pathlib import Path

import pandas as pd
import triplets  # noqa: F401  registers DataFrame.type_tableview / key_tableview and pd.read_RDF

REPO_ROOT = Path(__file__).resolve().parents[1]
TC1_DIR = REPO_ROOT / "test-data" / "tests" / "test-data"
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"

TC1_CO = "TC1_contingencies.xml"
TC1_AE = "TC1_assessed_elements.xml"
TC1_RA = "TC1_remedial_actions.xml"
TC1_CGMES = "TC1_CGMES.zip"
TC1_CONTINGENCY_BE_LINE_1 = "f38b9192-6064-4f1a-b2c6-5459ceb514b0"  # OCO_BE-Line_1

NC = "https://cim4.eu/ns/nc#"
DIRECTION_NONE = f"{NC}RelativeDirectionKind.none"
DIRECTION_UP_AND_DOWN = f"{NC}RelativeDirectionKind.upAndDown"
KIND_PREVENTIVE = f"{NC}RemedialActionKind.preventive"
KIND_CURATIVE = f"{NC}RemedialActionKind.curative"
LIMIT_PATL = "http://entsoe.eu/CIM/SchemaExtension/3/1#LimitTypeKind.patl"
LIMIT_TATL = "http://entsoe.eu/CIM/SchemaExtension/3/1#LimitTypeKind.tatl"
LIMIT_HIGH_VOLTAGE = "http://entsoe.eu/CIM/SchemaExtension/3/1#LimitTypeKind.highVoltage"
LIMIT_LOW_VOLTAGE = "http://entsoe.eu/CIM/SchemaExtension/3/1#LimitTypeKind.lowVoltage"
TSO_BE = "https://energy.referencedata.eu/EIC/10X1001A1001A094"
TSO_PL = "https://energy.referencedata.eu/EIC/10XPL-TSO------P"


def make_triplets(objects: dict, instance: str = "instance-1", label: str | None = "model.xml",
                  header: dict | None = None) -> pd.DataFrame:
    """Build a triplets frame from ``{object_id: (Type, {KEY: VALUE})}``.

    ``None`` values are skipped, which is how an absent RDF attribute looks after parsing.
    ``label`` adds the file-name row triplets stores per instance, ``header`` adds
    FullModel keys (e.g. ``Model.modelingAuthoritySet``) under a header object.
    """
    rows = []
    if label is not None:
        rows.append((f"{instance}-distribution", "Type", "Distribution", instance))
        rows.append((f"{instance}-distribution", "label", label, instance))
    if header:
        rows.append((f"{instance}-header", "Type", "FullModel", instance))
        rows.extend((f"{instance}-header", key, value, instance) for key, value in header.items())
    for object_id, (object_type, attributes) in objects.items():
        rows.append((object_id, "Type", object_type, instance))
        rows.extend((object_id, key, value, instance) for key, value in attributes.items() if value is not None)
    return pd.DataFrame(rows, columns=["ID", "KEY", "VALUE", "INSTANCE_ID"], dtype="string")


def concat_triplets(*frames: pd.DataFrame) -> pd.DataFrame:
    return pd.concat(frames, ignore_index=True)


def named_buffer(content: bytes, name: str) -> BytesIO:
    buffer = BytesIO(content)
    buffer.name = name
    return buffer


def tc1_buffer(file_name: str) -> BytesIO:
    return named_buffer((TC1_DIR / file_name).read_bytes(), file_name)


def logged(records, level: str | None = None) -> list[str]:
    """Messages from the ``loguru_messages`` fixture, optionally filtered by level name."""
    return [record["message"] for record in records if level is None or record["level"].name == level]


def make_properties(**headers):
    """RMQ-like properties object exposing only ``headers`` (all the handlers read)."""
    from types import SimpleNamespace
    return SimpleNamespace(headers=dict(headers))


# --- TC1 adaptation -----------------------------------------------------------------
# TC1 trips three CracBuilder defects (asserted as xfail in test_crac_builder_tc1.py).
# The end-to-end tests add only the rows needed to get past them, nothing else.

def adapt_tc1_profiles(data: pd.DataFrame) -> pd.DataFrame:
    """Add an empty ``IdentifiedObject.description`` to every AssessedElement and a
    direction-``none`` StaticPropertyRange (normalValue 0) to every alteration without one."""
    instance = data["INSTANCE_ID"].iloc[0]
    rows = []
    assessed_elements = data.type_tableview("AssessedElement", string_to_number=False)
    if assessed_elements is not None and "IdentifiedObject.description" not in assessed_elements.columns:
        rows += [(ae_id, "IdentifiedObject.description", "", instance) for ae_id in assessed_elements.index]

    alterations = data.key_tableview("GridStateAlteration.GridStateAlterationRemedialAction", string_to_number=False)
    ranges = data.type_tableview("StaticPropertyRange", string_to_number=False)
    covered = set(ranges["RangeConstraint.GridStateAlteration"]) if ranges is not None else set()
    for alteration_id in (alterations.index if alterations is not None else []):
        if alteration_id in covered:
            continue
        range_id = f"test-range-{alteration_id}"
        rows += [
            (range_id, "Type", "StaticPropertyRange", instance),
            (range_id, "RangeConstraint.GridStateAlteration", alteration_id, instance),
            (range_id, "RangeConstraint.direction", DIRECTION_NONE, instance),
            (range_id, "RangeConstraint.normalValue", "0.0", instance),
        ]
    patch = pd.DataFrame(rows, columns=["ID", "KEY", "VALUE", "INSTANCE_ID"], dtype="string")
    return pd.concat([data, patch], ignore_index=True)


def adapt_tc1_network(network: pd.DataFrame) -> pd.DataFrame:
    """TC1 is CGMES 3.0 (``OperationalLimitType.kind``); CracBuilder reads the CGMES 2.4.15
    ``OperationalLimitType.limitType``. Mirror every ``kind`` as ``limitType``."""
    kinds = network[network["KEY"] == "OperationalLimitType.kind"].copy()
    kinds["KEY"] = "OperationalLimitType.limitType"
    kinds["VALUE"] = kinds["VALUE"].str.replace(
        "http://iec.ch/TC57/CIM100-European#LimitKind.",
        "http://entsoe.eu/CIM/SchemaExtension/3/1#LimitTypeKind.",
        regex=False,
    )
    return pd.concat([network, kinds], ignore_index=True)


def read_rdf_adapting_tc1(original_read_rdf):
    """Wrap ``pd.read_RDF`` so the handler's own parsing of TC1 goes through the adaptation."""
    def wrapper(objects, *args, **kwargs):
        data = original_read_rdf(objects, *args, **kwargs)
        types = set(data.loc[data["KEY"] == "Type", "VALUE"])
        if "AssessedElement" in types:
            return adapt_tc1_profiles(data)
        if "OperationalLimitType" in types:
            return adapt_tc1_network(data)
        return data
    return wrapper


# --- OpenRAO result JSON factory ----------------------------------------------------

def flow_cnec_result(cnec_id: str, flows: dict) -> dict:
    """``flows``: ``{instant: {unit: flow}}`` -> OpenRAO flowCnecResults entry."""
    entry = {"flowCnecId": cnec_id}
    for instant, per_unit in flows.items():
        entry[instant] = {unit: {"margin": 0.0, "side1": {"flow": flow}} for unit, flow in per_unit.items()}
    return entry


def rao_result(flow_cnec_results: list, network_actions: list | None = None,
               range_actions: list | None = None, status: str = "default") -> dict:
    return {
        "type": "RAO_RESULT",
        "version": "1.8",
        "info": "Generated by Open RAO",
        "computationStatus": status,
        "executionDetails": "The RAO only went through first preventive",
        "costResults": {"initial": {"functionalCost": 0.0, "virtualCost": {}}},
        "computationStatusMap": [],
        "flowCnecResults": flow_cnec_results,
        "networkActionResults": network_actions or [],
        "rangeActionResults": range_actions or [],
    }
