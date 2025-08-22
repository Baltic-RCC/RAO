from pika import BasicProperties
import uuid
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from collections import defaultdict
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import RDF, XSD
import pandas as pd
import config
from loguru import logger
from integrations.elastic import Elastic
from common.config_parser import parse_app_properties

parse_app_properties(caller_globals=globals(),
                     path=str(Path(__file__).parent.joinpath("config.properties")),
                     section="HANDLER",
                     eval_types=True)


# ---------- Helper functions ----------
def _strip_namespace(uri_or_qname: str) -> str:
    """Remove namespace/prefix, keep only local name (e.g., 'nc.X.y'|'...#y' -> 'X.y' or 'y')."""
    u = str(uri_or_qname)
    if ":" in u and not u.startswith("http"):
        u = u.split(":", 1)[1]
    if "#" in u:
        u = u.split("#", 1)[1]
    elif "/" in u:
        u = u.rsplit("/", 1)[1]
    return u

def _literal_to_py(lit: Literal):
    dt = lit.datatype
    if dt in (XSD.integer, XSD.int, XSD.long, XSD.short, XSD.byte,
              XSD.unsignedInt, XSD.unsignedLong, XSD.unsignedShort, XSD.unsignedByte):
        try: return int(lit)
        except: return str(lit)
    if dt in (XSD.decimal, XSD.double, XSD.float):
        try: return float(lit)
        except: return str(lit)
    if dt in (XSD.boolean,):
        s = str(lit).strip().lower()
        return s in ("true", "1", "yes")
    return str(lit)

def _best_id_for_subject(g: Graph, s: URIRef) -> str:
    """Prefer fragment, then mRID, then last path segment."""
    uri = str(s)
    if "#" in uri:
        frag = uri.split("#", 1)[1]
        if frag:
            return frag
    mrid = URIRef("http://iec.ch/TC57/2013/CIM-schema-cim16#IdentifiedObject.mRID")
    for _, _, v in g.triples((s, mrid, None)):
        if isinstance(v, Literal):
            return str(v)
    return uri.rsplit("/", 1)[-1]

def _class_of_subject(g: Graph, s: URIRef) -> Optional[str]:
    for _, _, t in g.triples((s, RDF.type, None)):
        if isinstance(t, URIRef):
            try:
                return _strip_namespace(g.qname(t))
            except Exception:
                return _strip_namespace(str(t))
    return None

def _localname(uri: Union[str, URIRef]) -> str:
    return _strip_namespace(uri)


class CIMFlattener:
    def __init__(self, g: Graph, *, inline_depth: int = 99, include_uri: bool = False, key_mode: str = "qualified"):
        """
        "qualified" -> "IdentifiedObject.mRID"; "local" -> "mRID"
        """
        if key_mode not in ("qualified", "local"):
            raise ValueError("key_mode must be 'qualified' or 'local'")
        self.g = g
        self.inline_depth = inline_depth
        self.include_uri = include_uri
        self.key_mode = key_mode

        # Build incoming index once. Exclude md:FullModel nodes as sources (header).
        self._incoming_index: Dict[URIRef, List[Tuple[URIRef, URIRef]]] = defaultdict(list)
        for s, p, o in self.g.triples((None, None, None)):
            if isinstance(o, URIRef):
                if not self._is_fullmodel(s):
                    self._incoming_index[o].append((s, p))

    def _is_fullmodel(self, s: URIRef) -> bool:
        """Detect md:FullModel or any *FullModel class/localname (prefix-agnostic)."""
        for _, _, t in self.g.triples((s, RDF.type, None)):
            if isinstance(t, URIRef) and _localname(t).endswith("FullModel"):
                return True
        return _localname(s).endswith("FullModel")

    def _format_key(self, qname_or_uri: str) -> str:
        """
        Strip namespace, then optionally drop the class segment before the final dot.
        - qualified: 'IdentifiedObject.mRID'
        - local:     'mRID'
        """
        base = _strip_namespace(qname_or_uri)
        if self.key_mode == "local" and "." in base:
            return base.rsplit(".", 1)[-1]
        return base

    def _key(self, pred) -> str:
        try:
            qname = self.g.qname(pred)
        except Exception:
            qname = str(pred)
        return self._format_key(qname)

    def _value_for_object(self, o, depth_left: int, visited: Set[URIRef]) -> Any:
        if isinstance(o, Literal):
            return _literal_to_py(o)
        if isinstance(o, URIRef):
            if depth_left > 0 and o not in visited and any(True for _ in self.g.triples((o, None, None))):
                return self._subject_to_object(o, depth_left - 1, visited, is_root=False)
            return _best_id_for_subject(self.g, o)
        return str(o)

    def _subject_to_object(self, s: URIRef, depth_left: int, visited: Optional[Set[URIRef]] = None, *, is_root: bool = False) -> Dict[str, Any]:
        if visited is None:
            visited = set()
        if s in visited:
            return _best_id_for_subject(self.g, s)
        visited = set(visited)
        visited.add(s)

        obj: Dict[str, Any] = {}
        cls = _class_of_subject(self.g, s)
        if cls:
            obj["@type"] = self._format_key(cls)  # respects key_mode
        obj["@id"] = _best_id_for_subject(self.g, s)
        if self.include_uri:
            obj["@uri"] = str(s)

        from collections import defaultdict
        multimap: Dict[str, List[Any]] = defaultdict(list)
        object_keys: Set[str] = set()  # keys that contain expanded dict children

        # ---- Outgoing edges ----
        for _, p, o in self.g.triples((s, None, None)):
            if p == RDF.type:
                continue

            pred_key = self._key(p)  # default key = predicate (stripped + key_mode)

            if isinstance(o, URIRef) and depth_left > 0 and o not in visited and any(
                    True for _ in self.g.triples((o, None, None))):
                child = self._subject_to_object(o, depth_left - 1, visited, is_root=is_root)
                # If expanded, prefer child's @type as the key
                use_key = child.get("@type", pred_key) if isinstance(child, dict) else pred_key
                multimap[use_key].append(child if child else _best_id_for_subject(self.g, o))
                if isinstance(child, dict):
                    object_keys.add(use_key)
            else:
                multimap[pred_key].append(self._value_for_object(o, depth_left, visited))

        # ---- Incoming edges (same rule; merged under same key) ----
        if depth_left > 0:
            for src, pred in self._incoming_index.get(s, []):
                pred_key = self._key(pred)
                if src not in visited and any(True for _ in self.g.triples((src, None, None))):
                    child = self._subject_to_object(src, depth_left - 1, visited, is_root=is_root)
                    use_key = child.get("@type", pred_key) if isinstance(child, dict) else pred_key
                    multimap[use_key].append(child if child else _best_id_for_subject(self.g, src))
                    if isinstance(child, dict):
                        object_keys.add(use_key)
                else:
                    multimap[pred_key].append(_best_id_for_subject(self.g, src))

        # ---- collapse: lists for object-like keys (or when multiple values) ----
        for k, vals in multimap.items():
            if (k in object_keys) or (len(vals) > 1):
                obj[k] = vals  # always list for expanded children
            else:
                obj[k] = vals[0]  # keep simple scalars as scalars

        return obj

    def _subjects_by_class(self, class_name: str) -> List[URIRef]:
        # Accept "nc:RemedialActionSchedule" or "RemedialActionSchedule"
        want = _strip_namespace(class_name)
        out: List[URIRef] = []
        for s, _, t in self.g.triples((None, RDF.type, None)):
            if isinstance(t, URIRef):
                try:
                    cur = _strip_namespace(self.g.qname(t))
                except Exception:
                    cur = _strip_namespace(str(t))
                if cur == want:
                    out.append(s)
        return out

    def build_from_class(self, class_name: str) -> List[Dict[str, Any]]:
        roots = self._subjects_by_class(class_name)
        seen: Set[URIRef] = set()
        uniq: List[URIRef] = []
        for r in roots:
            if r not in seen:
                seen.add(r); uniq.append(r)
        return [self._subject_to_object(s, self.inline_depth, set(), is_root=True) for s in uniq]


def convert_cim_rdf_to_json(rdfxml: str, *, root_class: List[str], inline_depth: int = 99, key_mode: str = "qualified") -> Dict[str, Any]:
    """
    Returns:
    {
      "fullModel": {...},     # header (FullModel), keys per key_mode
      "<root_class>": [ ... ] # all objects of that class with full linked subgraph
    }
    """
    g = Graph()
    # Accept either a filepath or an XML string
    try:
        # Try as a path/URI first
        g.parse(rdfxml)
    except Exception:
        # Fallback: treat as raw RDF/XML string
        g.parse(data=rdfxml, format="application/rdf+xml")

    # Extract header FullModel (if present) — keys also respect key_mode
    def _format_key_local(qname_or_uri: str) -> str:
        base = _strip_namespace(qname_or_uri)
        return base.rsplit(".", 1)[-1] if key_mode == "local" and "." in base else base

    header: Dict[str, Any] = {}
    for s, _, _ in g.triples((None, None, None)):
        is_full = False
        for _, _, t in g.triples((s, RDF.type, None)):
            if isinstance(t, URIRef) and _localname(t).endswith("FullModel"):
                is_full = True; break
        if is_full or _localname(s).endswith("FullModel"):
            hmap: Dict[str, List[Any]] = defaultdict(list)
            for _, p, o in g.triples((s, None, None)):
                if p == RDF.type:
                    continue
                try:
                    pk = g.qname(p)
                except Exception:
                    pk = str(p)
                key = _format_key_local(pk)
                val = _literal_to_py(o) if isinstance(o, Literal) else _strip_namespace(o)
                hmap[key].append(val)
            for k, vals in hmap.items():
                header[k] = vals[0] if len(vals) == 1 else vals
            break  # assume single header

    fl = CIMFlattener(g, inline_depth=inline_depth, include_uri=False, key_mode=key_mode)

    output = {"FullModel": header}

    for cls in root_class:
        roots = fl.build_from_class(cls)
        output[cls] = roots

    return output


class RemedialActionScheduleToElasticHandler:

    def __init__(self):
        self.elastic_service = Elastic()

    @staticmethod
    def normalize_cim_payload(payload: dict) -> pd.DataFrame:
        # --- 1) FullModel meta ---
        meta = pd.json_normalize(payload.get("FullModel", {})).iloc[0].to_dict() if payload.get("FullModel") else {}

        # --- 2) Per-root normalize + attach meta ---
        frames = []
        for root_key, rows in payload.items():
            if root_key == "FullModel":
                continue
            rows = rows if isinstance(rows, list) else [rows]
            df = pd.json_normalize(rows)
            for k, v in meta.items():
                df[f"FullModel.{k}"] = v
            frames.append(df)

        if not frames:
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True)

        # --- helpers ---
        def any_list(s: pd.Series) -> bool:
            return s.apply(lambda x: isinstance(x, list)).any()

        def any_dict(s: pd.Series) -> bool:
            return s.apply(lambda x: isinstance(x, dict)).any()

        # --- 3) Recursively explode list columns and normalize dicts they contain ---
        changed = True
        while changed:
            changed = False
            for col in list(df.columns):
                if any_list(df[col]):
                    df = df.explode(col, ignore_index=True)
                    # if exploded into dicts, normalize & merge
                    if any_dict(df[col]):
                        norm = pd.json_normalize(df[col]).add_prefix(f"{col}.")
                        df = pd.concat([df.drop(columns=[col]).reset_index(drop=True), norm], axis=1)
                    changed = True

        # --- 4) Normalize any remaining plain dict columns (not lists) ---
        for col in list(df.columns):
            if any_dict(df[col]):
                norm = pd.json_normalize(df[col]).add_prefix(f"{col}.")
                df = pd.concat([df.drop(columns=[col]).reset_index(drop=True), norm], axis=1)

        return df

    def handle(self, message: bytes, properties: BasicProperties,  **kwargs):

        # Convert message from NC to JSON
        data = convert_cim_rdf_to_json(rdfxml=message,
                                       root_class=["RemedialActionSchedule"],
                                       key_mode=CONVERTER_KEY_MODE)

        # JSON normalize and transform to DataFrame
        df = self.normalize_cim_payload(data)

        # TODO need to get CO and RA from object storage and merge

        # Convert to dictionary
        data_to_send = df.to_dict(orient='records')

        response = self.elastic_service.send_to_elastic_bulk(
            index=ELASTIC_METADATA_INDEX,
            json_message_list=data_to_send,
            id_from_metadata=True,
            id_metadata_list=["@id"],
        )

        logger.info(f"Message sending to Elastic successful: {response}")

        return message, properties


if __name__ == "__main__":
    # rdf_xml = r"C:\Users\martynas.karobcickas\Downloads\ras-example.xml"
    # rdf_xml = r"C:\Users\martynas.karobcickas\Documents\Python projects\RAO\test-data\TC1_assessed_elements.xml"
    # rdf_xml = r"C:\Users\martynas.karobcickas\Documents\Python projects\RAO\test-data\TC1_contingencies.xml"
    # rdf_xml = r"C:\Users\martynas.karobcickas\Documents\Python projects\RAO\test-data\TC1_remedial_actions.xml"
    # g = Graph()
    # g.parse(rdf_xml, format="xml")  # Put your RDF/XML file

    # result = convert_cim_rdf_to_json(rdf_xml, root_class=["RemedialActionSchedule"], key_mode="local")
    # result = convert_cim_rdf_to_json(rdf_xml, root_class=["RemedialActionSchedule"], key_mode="qualified")
    # result = convert_cim_rdf_to_json(rdf_xml, root_class=["GridStateAlterationRemedialAction"], key_mode="local")
    # result = convert_cim_rdf_to_json(rdf_xml, root_class=["OrdinaryContingency", "ExceptionalContingency"], key_mode="local")

    # print json
    # print(json.dumps(result, indent=2))

    # with open("test.json", "w") as f:
    #     json.dump(result, f, ensure_ascii=False, indent=4)
    # df = RemedialActionScheduleToElasticHandler.normalize_cim_payload(result)
    # print(df.head())

    # Define RMQ test message
    headers = {
        "baCorrelationID": f"{uuid.uuid4()}",
        "baseMessageID": f"{uuid.uuid4()}",
        "businessType": "CSA-INPUT",
        "messageID": f"{uuid.uuid4()}",
        "sendTimestamp": datetime.utcnow().isoformat(),
        "sender": "TSOX",
        "senderApplication": "APPX",
        "service": "INPUT-DATA",
    }

    properties = BasicProperties(
        content_type="application/octet-stream",
        delivery_mode=2,
        priority=4,
        message_id=f"{uuid.uuid4()}",
        timestamp=147728025,
        headers=headers,
    )

    with open(r"C:\Users\martynas.karobcickas\Downloads\ras-example.xml", "rb") as file:
        file_bytes = file.read()

    # Create instance
    service = RemedialActionScheduleToElasticHandler()
    result = service.handle(message=file_bytes, properties=properties)
