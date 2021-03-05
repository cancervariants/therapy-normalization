"""Pytest test config tools."""
from therapy.database import Database
from typing import Dict, Any, Optional, List
import json
import pytest
from pathlib import Path


TEST_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope='module')
def mock_database():
    """Return MockDatabase object."""

    class MockDatabase(Database):
        """Mock database object to use in test cases."""

        def __init__(self):
            """Initialize mock database object. This class's method's shadow the
            actual Database class methods.

            `self.records` loads preexisting DB items.
            `self.added_records` stores add record requests, with the
            concept_id as the key and the complete record as the value.
            `self.updates` stores update requests, with the concept_id as the
            key and the updated attribute and new value as the value.
            """
            infile = TEST_ROOT / 'tests' / 'unit' / 'data' / 'therapies.json'  # noqa: E501
            self.records = {}
            with open(infile, 'r') as f:
                records_json = json.load(f)
            for record in records_json:
                self.records[record['label_and_type']] = {
                    record['concept_id']: record
                }
            self.added_records: Dict[str, Dict[Any, Any]] = {}
            self.updates: Dict[str, Dict[Any, Any]] = {}
            self.cached_sources = {
                'Wikidata': {
                    "data_license": "CC0 1.0",
                    "data_license_url": "https://creativecommons.org/publicdomain/zero/1.0/",  # noqa: E501
                    "version": "20200812",
                    "data_url": None,
                    "rdp_url": None,
                    "data_license_attributes": {
                        "non_commercial": False,
                        "attribution": False,
                        "share_alike": False
                    }
                },
                'ChemIDplus': {
                    "data_license": "custom",
                    "data_license_url": "https://www.nlm.nih.gov/databases/download/terms_and_conditions.html",  # noqa: E501
                    "version": "20200327",
                    "data_url": "ftp://ftp.nlm.nih.gov/nlmdata/.chemidlease/",
                    "rdp_url": None,
                    "data_license_attributes": {
                        "non_commercial": False,
                        "attribution": False,
                        "share_alike": False
                    }
                },
                'RxNorm': {
                    "data_license": "UMLS Metathesaurus",
                    "data_license_url": "https://www.nlm.nih.gov/research/umls/rxnorm/docs/termsofservice.html",  # noqa: E501
                    "version": "20210104",
                    "data_url": "https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html",  # noqa: E501
                    "rdp_url": None,
                    "data_license_attributes": {
                        "non_commercial": False,
                        "attribution": False,
                        "share_alike": False
                    }
                },
                'DrugBank': {
                    "data_license": "CC BY-NC 4.0",
                    "data_license_url": "https://creativecommons.org/licenses/by-nc/4.0/legalcode",  # noqa: E501
                    "version": "5.1.7",
                    "data_url": "https://go.drugbank.com/releases/5-1-7/downloads/all-full-database",  # noqa: E501
                    "rdp_url": "http://reusabledata.org/drugbank.html",
                    "data_license_attributes": {
                        "non_commercial": True,
                        "attribution": True,
                        "share_alike": False,
                    }
                },
                'NCIt': {
                    "data_license": "CC BY 4.0",
                    "data_license_url": "https://creativecommons.org/licenses/by/4.0/legalcode",  # noqa: E501
                    "version": "20.09d",
                    "data_url": "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/archive/20.09d_Release/",  # noqa: E501
                    "rdp_url": "http://reusabledata.org/ncit.html",
                    "data_license_attributes": {
                        "non_commercial": False,
                        "attribution": True,
                        "share_alike": False
                    }
                },
                'ChEMBL': {
                    "data_license": "CC BY-SA 3.0",
                    "data_license_url": "https://creativecommons.org/licenses/by-sa/3.0/",  # noqa: E501
                    "version": "27",
                    "data_url": "http://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_27/",  # noqa: E501
                    "rdp_url": "http://reusabledata.org/chembl.html",
                    "data_license_attributes": {
                        "non_commercial": False,
                        "attribution": True,
                        "share_alike": True
                    }
                }
            }

        def get_record_by_id(self, record_id: str,
                             case_sensitive: bool = True,
                             merge: bool = False) -> Optional[Dict]:
            """Fetch record corresponding to provided concept ID.

            :param str concept_id: concept ID for therapy record
            :param bool case_sensitive: if true, performs exact lookup, which
                is more efficient. Otherwise, performs filter operation, which
                doesn't require correct casing.
            :param bool merge: if true, retrieve merged record
            :return: complete therapy record, if match is found; None otherwise
            """
            if merge:
                label_and_type = f'{record_id.lower()}##merger'
            else:
                label_and_type = f'{record_id.lower()}##identity'
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
                of "alias", "trade_name", or "label" (use get_record_by_id for
                concept ID lookup)
            :return: list of matching records. Empty if lookup fails.
            """
            assert match_type in ('alias', 'trade_name', 'label', 'rx_brand')
            label_and_type = f'{query}##{match_type.lower()}'
            records_lookup = self.records.get(label_and_type, None)
            if records_lookup:
                return [v.copy() for v in records_lookup.values()]
            else:
                return []

        def get_merged_record(self, merge_ref) -> Optional[Dict]:
            """Fetch merged record from given reference.

            :param str merge_ref: key for merged record, formated as a string
                of grouped concept IDs separated by vertical bars, ending with
                `##merger`. Must be correctly-cased.
            :return: complete merged record if lookup successful, None
                otherwise
            """
            record_lookup = self.records.get(merge_ref, None)
            if record_lookup:
                vals = list(record_lookup.values())
                if vals:
                    return vals[0].copy()
            return None

        def add_record(self, record: Dict, record_type: str):
            """Store add record request sent to database.

            :param Dict record: record (of any type) to upload. Must include
                `concept_id` key. If record is of the `identity` type, the
                concept_id must be correctly-cased.
            :param str record_type: ignored by this function
            """
            self.added_records[record['concept_id']] = record

        def update_record(self, concept_id: str, attribute: str,
                          new_value: Any):
            """Store update request sent to database.

            :param str concept_id: record to update
            :param str field: name of field to update
            :parm str new_value: new value
            """
            assert f'{concept_id.lower()}##identity' in self.records
            self.updates[concept_id] = {attribute: new_value}

    return MockDatabase
