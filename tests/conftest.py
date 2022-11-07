"""Pytest test config tools."""
import os
from typing import Dict, Any, Optional, List, Callable
import json
from pathlib import Path

import pytest

from therapy.etl.base import Base
from therapy.query import QueryHandler
from therapy.schemas import Drug, MatchType, MatchesKeyed
from therapy.database import Database


TEST_ROOT = Path(__file__).resolve().parents[1]
TEST_DATA_DIRECTORY = TEST_ROOT / "tests" / "data"


@pytest.fixture(scope="session")
def test_data():
    """Provide test data location to test modules"""
    return TEST_DATA_DIRECTORY


@pytest.fixture(scope="session", autouse=True)
def db():
    """Provide a database instance to be used by tests."""
    return Database()


@pytest.fixture(scope="session")
def disease_normalizer():
    """TODO"""
    with open(TEST_DATA_DIRECTORY / "disease_normalization.json", "r") as f:
        disease_data = json.load(f)

        def _normalize_disease(query: str):
            return disease_data.get(query.lower())

    return _normalize_disease


@pytest.fixture(scope="session")
def test_source(
        db: Database, test_data: Path,
        disease_normalizer: Callable
):
    """TODO"""
    def test_source_factory(EtlClass: Base):
        if os.environ.get("THERAPY_TEST") is not None:
            test_class = EtlClass(db, test_data)  # type: ignore
            test_class._normalize_disease = disease_normalizer  # type: ignore
            test_class.perform_etl(use_existing=True)

        class QueryGetter:

            def __init__(self):
                self.query_handler = QueryHandler()
                self._src_name = EtlClass.__name__  # type: ignore

            def search(self, query_str: str):
                resp = self.query_handler.search(
                    query_str, keyed=True, incl=self._src_name
                )
                return resp.source_matches[self._src_name]

        return QueryGetter()

    return test_source_factory


@pytest.fixture(scope="module")
def mock_database():
    """Return MockDatabase object."""

    class MockDatabase(Database):
        """Mock database object to use in test cases."""

        def __init__(self):
            """Initialize mock database object. This class's methods shadow the actual
            Database class methods.
            `self.records` loads preexisting DB items.
            `self.added_records` stores add record requests, with the concept_id as the
            key and the complete record as the value.
            `self.updates` stores update requests, with the concept_id as the key, and
            a dict of {new_attribute: new_value} as the value.
            """
            infile = TEST_DATA_DIRECTORY / "therapies.json"
            self.records = {}
            with open(infile, "r") as f:
                records_json = json.load(f)
            for record in records_json:
                try:
                    label_and_type = record["label_and_type"]
                except KeyError:
                    raise Exception
                concept_id = record["concept_id"]
                if self.records.get(label_and_type):
                    self.records[label_and_type][concept_id] = record
                else:
                    self.records[label_and_type] = {concept_id: record}
            self.added_records: Dict[str, Dict[Any, Any]] = {}
            self.updates: Dict[str, Dict[Any, Any]] = {}

            meta = TEST_DATA_DIRECTORY / "metadata.json"
            with open(meta, "r") as f:
                meta_json = json.load(f)
            self.cached_sources = {}
            for src in meta_json:
                name = src["src_name"]
                self.cached_sources[name] = src
                del self.cached_sources[name]["src_name"]

        def get_record_by_id(self, record_id: str,
                             case_sensitive: bool = True,
                             merge: bool = False) -> Optional[Dict]:
            """Fetch record corresponding to provided concept ID.

            :param str record_id: concept ID for therapy record
            :param bool case_sensitive: if true, performs exact lookup, which is more
                efficient. Otherwise, performs filter operation, which doesn"t require
                correct casing.
            :param bool merge: if true, retrieve merged record
            :return: complete therapy record, if match is found; None otherwise
            """
            if merge:
                label_and_type = f"{record_id.lower()}##merger"
                record_lookup = self.records.get(label_and_type)
                if record_lookup:
                    return list(record_lookup.values())[0].copy()
                else:
                    return None
            else:
                label_and_type = f"{record_id.lower()}##identity"
            record_lookup = self.records.get(label_and_type, None)
            if record_lookup:
                if case_sensitive:
                    record = record_lookup.get(record_id, None)
                    if record:
                        return record.copy()
                    else:
                        return None
                elif record_lookup.values():
                    return list(record_lookup.values())[0].copy()
            return None

        def get_records_by_type(self, query: str,
                                match_type: str) -> List[Dict]:
            """Retrieve records for given query and match type.

            :param query: string to match against
            :param str match_type: type of match to look for. Should be one
                of {"alias", "trade_name", "label", "rx_brand", "xref"
                "associated_with"} (use get_record_by_id for concept ID lookup)
            :return: list of matching records. Empty if lookup fails.
            """
            assert match_type in ("alias", "trade_name", "label", "rx_brand",
                                  "xref", "associated_with")
            label_and_type = f"{query}##{match_type.lower()}"
            records_lookup = self.records.get(label_and_type, None)
            if records_lookup:
                return [v.copy() for v in records_lookup.values()]
            else:
                return []

        def add_record(self, record: Dict, record_type: str) -> None:
            """Store add record request sent to database.

            :param Dict record: record (of any type) to upload. Must include
                `concept_id` key. If record is of the `identity` type, the
                concept_id must be correctly-cased.
            :param str record_type: ignored by this function
            """
            self.added_records[record["concept_id"]] = record

        def update_record(self, concept_id: str, attribute: str,
                          new_value: Any):  # noqa: ANN401
            """Store update request sent to database.

            :param str concept_id: record to update
            :param str attribute: name of field to update
            :param str new_value: new value
            """
            assert f"{concept_id.lower()}##identity" in self.records
            self.updates[concept_id] = {attribute: new_value}

    return MockDatabase


def compare_records(actual: Drug, fixt: Drug):
    """Check that identity records are identical."""
    assert actual.concept_id == fixt.concept_id
    assert actual.label == fixt.label

    assert (actual.aliases is None) == (fixt.aliases is None)
    if (actual.aliases is not None) and (fixt.aliases is not None):
        assert set(actual.aliases) == set(fixt.aliases)

    assert (actual.trade_names is None) == (fixt.trade_names is None)
    if (actual.trade_names is not None) and (fixt.trade_names is not None):
        assert set(actual.trade_names) == set(fixt.trade_names)

    assert (actual.xrefs is None) == (fixt.xrefs is None)
    if (actual.xrefs is not None) and (fixt.xrefs is not None):
        assert set(actual.xrefs) == set(fixt.xrefs)

    assert (actual.associated_with is None) == (fixt.associated_with is None)
    if (actual.associated_with is not None) and (fixt.associated_with is not None):
        assert set(actual.associated_with) == set(fixt.associated_with)

    assert (not actual.approval_ratings) == (not fixt.approval_ratings)
    if (actual.approval_ratings) and (fixt.approval_ratings):
        assert set(actual.approval_ratings) == set(fixt.approval_ratings)

    assert (actual.approval_year is None) == (fixt.approval_year is None)
    if (actual.approval_year is not None) and (fixt.approval_year is not None):
        assert set(actual.approval_year) == set(fixt.approval_year)

    assert (actual.has_indication is None) == (fixt.has_indication is None)
    if (actual.has_indication is not None) and (fixt.has_indication is not None):
        actual_inds = actual.has_indication.copy()
        fixture_inds = fixt.has_indication.copy()
        assert len(actual_inds) == len(fixture_inds)
        actual_inds.sort(key=lambda x: x.disease_id)
        fixture_inds.sort(key=lambda x: x.disease_id)
        for i in range(len(actual_inds)):
            assert actual_inds[i] == fixture_inds[i]


def compare_response(response: MatchesKeyed, match_type: MatchType,
                     fixture: Optional[Drug] = None,
                     fixture_list: Optional[List[Drug]] = None,
                     num_records: int = 0):
    """Check that test response is correct. Only 1 of {fixture, fixture_list}
    should be passed as arguments. num_records should only be passed with fixture_list.

    :param Dict response: response object returned by QueryHandler
    :param MatchType match_type: expected match type
    :param Drug fixture: single Drug object to match response against
    :param List[Drug] fixture_list: multiple Drug objects to match response against
    :param int num_records: expected number of records in response. If not given, tests
        for number of fixture Drugs given (ie, 1 for single fixture and length of
        fixture_list otherwise)
    """
    if fixture and fixture_list:
        raise Exception("Args provided for both `fixture` and `fixture_list`")
    elif not fixture and not fixture_list:
        raise Exception("Must pass 1 of {fixture, fixture_list}")
    if fixture and num_records:
        raise Exception("`num_records` should only be given with "
                        "`fixture_list`.")

    assert response.match_type == match_type
    if fixture:
        assert len(response.records) == 1
        compare_records(response.records[0], fixture)
    elif fixture_list:
        if not num_records:
            assert len(response.records) == len(fixture_list)
        else:
            assert len(response.records) == num_records
        for fixt in fixture_list:
            for record in response.records:
                if fixt.concept_id == record.concept_id:
                    compare_records(record, fixt)
                    break
            else:
                assert False  # test fixture not found in response
