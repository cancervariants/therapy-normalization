"""Pytest test config tools."""
import os
from typing import Dict, Any, Optional, List
import json
from pathlib import Path

import pytest

from therapy.schemas import Drug, MatchType
from therapy.database import Database

TEST_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module", autouse=True)
def db():
    """Create a DynamoDB test fixture."""

    class DB:
        def __init__(self):
            self.db = Database()
            if os.environ.get("TEST") is not None:
                self.load_test_data()

        def load_test_data(self) -> None:
            with open(f"{TEST_ROOT}/tests/unit/"
                      f"data/therapies.json", "r") as f:
                therapies = json.load(f)
                with self.db.therapies.batch_writer() as batch:
                    for therapy in therapies:
                        batch.put_item(Item=therapy)

            with open(f"{TEST_ROOT}/tests/unit/"
                      f"data/metadata.json", "r") as f:
                metadata = json.load(f)
                with self.db.metadata.batch_writer() as batch:
                    for m in metadata:
                        batch.put_item(Item=m)

    return DB().db


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
            infile = TEST_ROOT / "tests" / "unit" / "data" / "therapies.json"
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

            meta = TEST_ROOT / "tests" / "unit" / "data" / "metadata.json"
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
                          new_value: Any):
            """Store update request sent to database.

            :param str concept_id: record to update
            :param str attribute: name of field to update
            :param str new_value: new value
            """
            assert f"{concept_id.lower()}##identity" in self.records
            self.updates[concept_id] = {attribute: new_value}

    return MockDatabase


def compare_records(actual: Dict, fixt: Drug):
    """Check that identity records are identical."""
    fixture = fixt.dict()
    assert actual["concept_id"] == fixture["concept_id"]
    assert actual["label"] == fixture["label"]
    assert set(actual["aliases"]) == set(fixture["aliases"])
    assert set(actual["trade_names"]) == set(fixture["trade_names"])
    assert set(actual["xrefs"]) == set(fixture["xrefs"])
    assert set(actual["associated_with"]) == set(fixture["associated_with"])
    if actual["approval_status"] or fixture["approval_status"]:
        assert actual["approval_status"] == fixture["approval_status"]
    if actual["approval_year"] or fixture["approval_year"]:
        assert set(actual["approval_year"]) == set(fixture["approval_year"])
    if actual["has_indication"] or fixture["has_indication"]:
        actual_inds = actual["has_indication"].copy()
        fixture_inds = fixture["has_indication"].copy()
        assert len(actual_inds) == len(fixture_inds)
        actual_inds.sort(key=lambda x: x["disease_id"])
        fixture_inds.sort(key=lambda x: x["disease_id"])
        for i in range(len(actual_inds)):
            assert actual_inds[i] == fixture_inds[i]


def compare_response(response: Dict, match_type: MatchType,
                     fixture: Drug = None, fixture_list: List[Drug] = None,
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

    assert response["match_type"] == match_type
    if fixture:
        assert len(response["records"]) == 1
        compare_records(response["records"][0], fixture)
    elif fixture_list:
        if not num_records:
            assert len(response["records"]) == len(fixture_list)
        else:
            assert len(response["records"]) == num_records
        for fixt in fixture_list:
            for record in response["records"]:
                if fixt.concept_id == record.concept_id:
                    compare_records(record, fixt)
                    break
            else:
                assert False  # test fixture not found in response
