"""Input data retrieval: NC profile ingestion (input_retriever), Object Storage queries used by the RAO,
the RDF/XML -> JSON converter and the remedial action schedule handler."""
from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest
import rdflib

import input_retriever.handlers as input_handlers
import remedial_action_schedules.handlers as schedule_handlers
from common import rdf_converter
from common.object_storage import ObjectStorage, resolve_party_field
from helpers import TC1_AE, TC1_CO, TC1_RA, make_properties

TC1_HEADERS = {
    TC1_CO: ("CO", "urn:uuid:e09a6b24-e3c3-46de-93dc-5fcea7439b26"),
    TC1_AE: ("AE", "urn:uuid:354e6dc4-56da-4c27-a995-3a65b37f1c33"),
    TC1_RA: ("RA", "urn:uuid:ad7d0e5d-c056-491c-8eb6-81b021fccd75"),
}
NO_HEADER_PROFILE = (b'<?xml version="1.0"?><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" '
                     b'xmlns:cim="http://iec.ch/TC57/CIM100#"><cim:Line rdf:ID="_a">'
                     b'<cim:IdentifiedObject.name>x</cim:IdentifiedObject.name></cim:Line></rdf:RDF>')


@pytest.fixture
def services(monkeypatch):
    s3, elastic = MagicMock(name="S3Minio"), MagicMock(name="Elastic")
    monkeypatch.setattr(input_handlers, "S3Minio", s3)
    monkeypatch.setattr(input_handlers, "Elastic", elastic)
    return s3.return_value, elastic.return_value


# --- HandlerMetadataToObjectStorage ---------------------------------------------------------

class TestMetadataToObjectStorage:
    @pytest.mark.parametrize("profile", [TC1_CO, TC1_AE, TC1_RA])
    def test_tc1_profile_stored_and_indexed(self, services, tc1_dir, profile):
        s3, elastic = services
        keyword, identifier = TC1_HEADERS[profile]
        message = (tc1_dir / profile).read_bytes()
        properties = make_properties(messageID="msg-1", sender="TSO")
        assert input_handlers.HandlerMetadataToObjectStorage().handle(message, properties) == (message, properties)

        expected_name = f"CSA/{keyword}_1_38X-BALTIC-RSC-H_2024-06-12T22:00:00_2024-06-13T22:00:00.xml"
        upload = s3.upload_object.call_args.kwargs
        assert upload["file_path_or_file_object"].name == expected_name
        assert upload["file_path_or_file_object"].getvalue() == message
        assert upload["bucket_name"] == "pdn-data"
        assert properties.headers["keyword"] == keyword and upload["metadata"] is properties.headers

        document = elastic.send_to_elastic.call_args.kwargs
        assert (document["index"], document["id"]) == ("csa-input-metadata", identifier)
        metadata = document["json_message"]
        assert (metadata["content_bucket"], metadata["content_reference"]) == ("pdn-data", expected_name)
        assert metadata["rmq"]["headers"]["messageID"] == "msg-1"
        # triplets converts numeric header values, so the version is indexed as a number
        assert (metadata["Model.version"], metadata["startDate"]) == (1, "2024-06-12T22:00:00")

    def test_missing_header_fields_become_undefined(self, services):
        s3, elastic = services
        profile = (b'<?xml version="1.0"?><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" '
                   b'xmlns:md="http://iec.ch/TC57/61970-552/ModelDescription/1#">'
                   b'<md:FullModel rdf:about="urn:uuid:1"><md:Model.created>2024</md:Model.created></md:FullModel></rdf:RDF>')
        properties = make_properties(messageID="m")
        input_handlers.HandlerMetadataToObjectStorage().handle(profile, properties)
        assert s3.upload_object.call_args.kwargs["file_path_or_file_object"].name == (
            "CSA/UNDEFINED_UNDEFINED_UNDEFINED_UNDEFINED_UNDEFINED.xml")
        assert properties.headers["keyword"] == "UNDEFINED"
        assert elastic.send_to_elastic.call_args.kwargs["id"] is None  # Elastic assigns a random id

    def test_message_id_header_required(self, services, tc1_dir):
        with pytest.raises(KeyError, match="messageID"):
            input_handlers.HandlerMetadataToObjectStorage().handle((tc1_dir / TC1_CO).read_bytes(), make_properties())

    def test_profile_without_full_model_rejected(self, services):
        with pytest.raises(AttributeError):
            input_handlers.HandlerMetadataToObjectStorage().handle(NO_HEADER_PROFILE, make_properties(messageID="m"))
        services[0].upload_object.assert_not_called()


# --- HandlerInputDataToElastic --------------------------------------------------------------

class TestInputDataToElastic:
    @pytest.mark.parametrize("profile, index, rows, root_types", [
        (TC1_CO, "csa-contingencies", 8, {"OrdinaryContingency", "ExceptionalContingency", "OutOfRangeContingency"}),
        (TC1_AE, "csa-assessed-elements", 9, {"AssessedElement"}),
        (TC1_RA, "csa-remedial-actions", 9, {"GridStateAlterationRemedialAction"}),
    ])
    def test_tc1_profile_to_elastic(self, services, tc1_dir, profile, index, rows, root_types):
        _, elastic = services
        keyword, identifier = TC1_HEADERS[profile]
        input_handlers.HandlerInputDataToElastic().handle((tc1_dir / profile).read_bytes(), make_properties(keyword=keyword))
        call = elastic.send_to_elastic_bulk.call_args.kwargs
        documents = call["json_message_list"]
        assert call["index"] == index and len(documents) == rows
        assert {d["@type"] for d in documents} == root_types
        assert {d["FullModel.identifier"] for d in documents} == {identifier}
        assert not any(isinstance(v, float) and v != v for d in documents for v in d.values())  # NaN -> None

    @pytest.mark.parametrize("headers", [{}, {"keyword": "UNDEFINED"}, {"keyword": ""}])
    def test_missing_keyword_skipped(self, services, headers, loguru_messages):
        input_handlers.HandlerInputDataToElastic().handle(b"<rdf/>", make_properties(**headers))
        services[1].send_to_elastic_bulk.assert_not_called()

    @pytest.mark.xfail(strict=True, raises=KeyError,
                       reason="BUG: profiles other than CO/AE/RA (e.g. ER, SSI) reach this handler after the metadata "
                              "handler set their keyword, and KEYWORD_MAP[keyword] raises instead of skipping them")
    def test_unsupported_keyword_skipped(self, services, tc1_dir):
        input_handlers.HandlerInputDataToElastic().handle((tc1_dir / TC1_CO).read_bytes(), make_properties(keyword="ER"))
        services[1].send_to_elastic_bulk.assert_not_called()


# --- ObjectStorage retrieval used by the RAO handler ------------------------------------------

def hits(*sources):
    return [{"_index": "csa-input-metadata-202406", "_id": str(n), "_source": source} for n, source in enumerate(sources)]


@pytest.fixture
def storage():
    instance = ObjectStorage.__new__(ObjectStorage)
    instance.s3_service = MagicMock(name="S3Minio")
    instance.elastic_service = MagicMock(name="Elastic")
    instance.s3_service.download_object.side_effect = lambda bucket, reference: f"{bucket}/{reference}".encode()
    return instance


def serve(storage, *pages):
    storage.elastic_service.client.search.return_value = {"_scroll_id": "scroll-1", "hits": {"hits": pages[0] if pages else []}}
    storage.elastic_service.client.scroll.side_effect = [{"hits": {"hits": page}} for page in pages[1:]] + [{"hits": {"hits": []}}]


def metadata(keyword, version, created="2024-06-12T10:00:00Z", start="2024-06-12T22:00:00Z", **extra):
    return {"keyword": keyword, "Model.version": version, "Model.created": created, "startDate": start,
            "content_reference": f"{keyword}-v{version}.xml", **extra}


class TestQuery:
    def test_query_construction_and_scrolling(self, storage):
        serve(storage, hits(metadata("CO", "1")), hits(metadata("AE", "1")))
        result = storage.query(metadata_query={"keyword": ["CO", "AE"], "entity": "BE"},
                               range_query=[{"range": {"startDate": {"lte": "t"}}}],
                               query_filter=[{"range": {"startDate": {"gte": "now-2w"}}}], index="csa-input-metadata")
        search = storage.elastic_service.client.search.call_args.kwargs
        assert search["index"] == "csa-input-metadata*"
        assert search["query"] == {"bool": {
            "must": [{"terms": {"keyword.keyword": ["CO", "AE"]}}, {"term": {"entity.keyword": "BE"}},
                     {"range": {"startDate": {"lte": "t"}}}],
            "filter": [{"range": {"startDate": {"gte": "now-2w"}}}]}}
        assert [r["keyword"] for r in result] == ["CO", "AE"]
        assert result[0]["_id"] == "0" and "_source" not in result[0]  # _source flattened into the hit
        storage.elastic_service.client.clear_scroll.assert_called_once_with(scroll_id="scroll-1")

    def test_wildcard_index_kept_and_payload_returned(self, storage):
        serve(storage, hits(metadata("CO", "1", content_bucket="bucket")))
        [content] = storage.query(metadata_query={"keyword": "CO"}, index="idx-*", return_payload=True)
        assert storage.elastic_service.client.search.call_args.kwargs["index"] == "idx-*"
        assert content.getvalue() == b"bucket/CO-v1.xml" and content.name == "CO-v1.xml"


class TestGetContent:
    def test_bucket_precedence(self, storage):
        assert storage.get_content({"content_reference": "a.xml", "content_bucket": "meta"}, "arg").getvalue() == b"arg/a.xml"
        assert storage.get_content({"content_reference": "a.xml", "content_bucket": "meta"}).getvalue() == b"meta/a.xml"
        assert storage.get_content({"content_reference": "a.xml"}).getvalue() == b"pdn-data/a.xml"

    def test_missing_reference(self, storage, loguru_messages):
        assert storage.get_content({"_id": "1"}) is None
        storage.s3_service.download_object.assert_not_called()


@pytest.mark.parametrize("columns, expected", [(["entity", "publisher"], "entity"), (["publisher"], "publisher"),
                                               (["keyword"], None)])
def test_resolve_party_field(columns, expected):
    assert resolve_party_field(pd.DataFrame(columns=columns)) == expected


class TestInputDataForTimestamp:
    TIMESTAMP = datetime(2024, 6, 13, 9, 30)

    def test_latest_version_per_party_and_keyword(self, storage):
        serve(storage, hits(metadata("CO", "1", entity="BE"), metadata("CO", "2", entity="BE"),
                            metadata("CO", "1", entity="NL"), metadata("AE", "3", entity="BE")))
        result = storage.get_input_data_for_timestamp(["CO", "AE"], self.TIMESTAMP, entity=["BE", "NL"])
        assert sorted((r["entity"], r["keyword"], r["Model.version"]) for r in result) == [
            ("BE", "AE", "3"), ("BE", "CO", "2"), ("NL", "CO", "1")]
        assert {r["content"].name for r in result} == {"CO-v2.xml", "CO-v1.xml", "AE-v3.xml"}
        query = storage.elastic_service.client.search.call_args.kwargs["query"]["bool"]["must"]
        assert {"range": {"startDate": {"lte": self.TIMESTAMP}}} in query
        assert {"range": {"endDate": {"gte": self.TIMESTAMP}}} in query
        assert {"terms": {"entity.keyword": ["BE", "NL"]}} in query

    def test_latest_created_breaks_version_ties(self, storage):
        serve(storage, hits(metadata("CO", "1", created="2024-06-12T08:00:00Z", publisher="p", content_reference="old.xml"),
                            metadata("CO", "1", created="2024-06-12T09:00:00Z", publisher="p", content_reference="new.xml")))
        [result] = storage.get_input_data_for_timestamp(["CO"], self.TIMESTAMP)
        assert result["content_reference"] == "new.xml"

    def test_without_party_fields_grouped_by_keyword(self, storage):
        serve(storage, hits(metadata("CO", "1"), metadata("CO", "2")))
        assert [r["Model.version"] for r in storage.get_input_data_for_timestamp(["CO"], self.TIMESTAMP)] == ["2"]

    def test_download_failure_skips_profile(self, storage):
        serve(storage, hits(metadata("CO", "1", entity="BE"), metadata("AE", "1", entity="BE")))
        storage.s3_service.download_object.side_effect = lambda bucket, ref: (_ for _ in ()).throw(OSError("s3")) \
            if ref.startswith("CO") else b"ae"
        assert [r["keyword"] for r in storage.get_input_data_for_timestamp(["CO", "AE"], self.TIMESTAMP)] == ["AE"]

    def test_nothing_found(self, storage):
        serve(storage)
        assert storage.get_input_data_for_timestamp(["CO"], self.TIMESTAMP) == []

    @pytest.mark.xfail(strict=True, raises=AttributeError,
                       reason="BUG: the signature allows an ISO string timestamp, but .isoformat() is called on it")
    def test_iso_string_timestamp(self, storage):
        serve(storage)
        assert storage.get_input_data_for_timestamp(["CO"], "2024-06-13T09:30:00") == []

    def test_numeric_versions_as_indexed_by_input_retriever(self, storage):
        serve(storage, hits(metadata("CO", 9, entity="BE"), metadata("CO", 10, entity="BE")))
        assert [r["Model.version"] for r in storage.get_input_data_for_timestamp(["CO"], self.TIMESTAMP)] == [10]

    def test_string_versions_sort_lexicographically(self, storage):
        # Edge: metadata indexed by another producer with string versions picks '9' over '10'
        serve(storage, hits(metadata("CO", "9", entity="BE"), metadata("CO", "10", entity="BE")))
        assert [r["Model.version"] for r in storage.get_input_data_for_timestamp(["CO"], self.TIMESTAMP)] == ["9"]

    @pytest.mark.xfail(strict=True, raises=AssertionError,
                       reason="BUG: groupby().first() takes the first non-null value per column, so a latest version "
                              "without content_reference is combined with an older version's file")
    def test_fields_not_mixed_between_versions(self, storage):
        latest = metadata("AE", "2", entity="BE")
        del latest["content_reference"]
        serve(storage, hits(latest, metadata("AE", "1", entity="BE")))
        [result] = storage.get_input_data_for_timestamp(["AE"], self.TIMESTAMP)
        assert (result["Model.version"], result["content"]) == ("2", None)

    def test_records_without_party_value_dropped(self, storage):
        # Edge: groupby drops NaN keys; a profile lacking 'entity' disappears when others carry it
        serve(storage, hits(metadata("CO", "1", entity="BE"), metadata("AE", "1")))
        assert [r["keyword"] for r in storage.get_input_data_for_timestamp(["CO", "AE"], self.TIMESTAMP)] == ["CO"]


class TestLatestAvailableInputData:
    def test_latest_start_date_wins(self, storage):
        serve(storage, hits(metadata("CO", "5", start="2024-06-10T22:00:00Z", entity="BE"),
                            metadata("CO", "1", start="2024-06-12T22:00:00Z", entity="BE")))
        [result] = storage.get_latest_available_input_data(["CO"], "2024-06-13T09:30:00", entity=["BE"])
        assert result["startDate"] == "2024-06-12T22:00:00Z"
        query = storage.elastic_service.client.search.call_args.kwargs["query"]["bool"]
        assert query["filter"] == [{"range": {"startDate": {"gte": "now-2w"}}},
                                   {"range": {"startDate": {"lte": "2024-06-13T09:30:00"}}}]
        assert {"terms": {"entity.keyword": ["BE"]}} in query["must"]

    def test_nothing_found(self, storage):
        serve(storage)
        assert storage.get_latest_available_input_data(["CO"], datetime(2024, 6, 13), range_filter="now-1d") == []


# --- RDF/XML -> JSON converter ---------------------------------------------------------------

class TestRdfConverter:
    @pytest.mark.parametrize("raw, expected", [
        ("nc:AssessedElement.inBaseCase", "AssessedElement.inBaseCase"),
        ("http://iec.ch/TC57/CIM100#IdentifiedObject.name", "IdentifiedObject.name"),
        ("https://energy.referencedata.eu/EIC/10X1001A1001A094", "10X1001A1001A094"),
        ("urn:uuid:abc", "uuid:abc"),
    ])
    def test_strip_namespace(self, raw, expected):
        assert rdf_converter._strip_namespace(raw) == expected

    @pytest.mark.parametrize("literal, expected", [
        (rdflib.Literal("5", datatype=rdflib.XSD.integer), 5),
        (rdflib.Literal("1.5", datatype=rdflib.XSD.double), 1.5),
        (rdflib.Literal("true", datatype=rdflib.XSD.boolean), True),
        (rdflib.Literal("30.0"), "30.0"),  # untyped CIM literals stay strings
    ])
    def test_literal_to_python(self, literal, expected):
        assert rdf_converter._literal_to_py(literal) == expected

    def test_invalid_key_mode(self):
        with pytest.raises(ValueError, match="key_mode"):
            rdf_converter.CIMFlattener(rdflib.Graph(), key_mode="short")

    def test_contingencies_with_nested_equipment(self, tc1_dir):
        payload = rdf_converter.convert_cim_rdf_to_json((tc1_dir / TC1_CO).read_bytes(),
                                                        root_class=["OrdinaryContingency", "MissingClass"], key_mode="local")
        assert payload["FullModel"]["keyword"] == "CO" and payload["FullModel"]["publisher"] == "38X-BALTIC-RSC-H"
        assert payload["MissingClass"] == []
        be_line_1 = next(c for c in payload["OrdinaryContingency"] if c["name"] == "OCO_BE-Line_1")
        assert (be_line_1["mRID"], be_line_1["normalMustStudy"], be_line_1["EquipmentOperator"]) == (
            "f38b9192-6064-4f1a-b2c6-5459ceb514b0", "true", "10X1001A1001A094")
        assert [e["Equipment"] for e in be_line_1["ContingencyEquipment"]] == ["_17086487-56ba-4979-b8de-064025a6b4da"]

    def test_absolute_path_input(self, tc1_dir):
        payload = rdf_converter.convert_cim_rdf_to_json(str(tc1_dir / TC1_CO), root_class=["OrdinaryContingency"])
        assert len(payload["OrdinaryContingency"]) == 6

    @pytest.mark.xfail(strict=True, raises=Exception,
                       reason="BUG: Path(relative).as_uri() raises, and the fallback parses the path string as XML")
    def test_relative_path_input(self, tc1_dir, monkeypatch):
        monkeypatch.chdir(tc1_dir)
        assert len(rdf_converter.convert_cim_rdf_to_json(TC1_CO, root_class=["OrdinaryContingency"])["OrdinaryContingency"]) == 6

    def test_normalize_root_only_and_exploded(self, tc1_dir):
        payload = rdf_converter.convert_cim_rdf_to_json(
            (tc1_dir / TC1_CO).read_bytes(),
            root_class=["OrdinaryContingency", "ExceptionalContingency", "OutOfRangeContingency"], key_mode="local")
        roots = rdf_converter.normalize_cim_payload(payload, root_only=True)
        assert len(roots) == 8 and set(roots["FullModel.keyword"]) == {"CO"}
        exploded = rdf_converter.normalize_cim_payload(payload, root_only=False)
        assert len(exploded) == 10  # EXCO and OOR contingencies have two equipment each
        assert "ContingencyEquipment.Equipment" in exploded.columns

    def test_normalize_header_only_payload(self):
        assert rdf_converter.normalize_cim_payload({"FullModel": {"keyword": "CO"}}).empty


def test_remedial_action_schedule_handler(monkeypatch, tc1_dir):
    elastic = MagicMock(name="Elastic")
    monkeypatch.setattr(schedule_handlers, "Elastic", elastic)
    message = (tc1_dir / TC1_RA).read_bytes()
    properties = make_properties()
    assert schedule_handlers.HandlerRemedialActionScheduleToElastic().handle(message, properties) == (message, properties)
    elastic.return_value.send_to_elastic_bulk.assert_called_once_with(index="csa-optimized-schedules", json_message_list=[])
