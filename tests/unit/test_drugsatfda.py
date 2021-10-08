"""Test correctness of Drugs@FDA ETL methods."""
from datetime import datetime
from typing import Dict, List

import pytest

from tests.conftest import compare_records
from therapy.query import QueryHandler
from therapy.schemas import MatchType


@pytest.fixture(scope="module")
def drugsatfda():
    """Build Drugs@FDA test fixture."""
    class QueryGetter:

        def __init__(self):
            self.normalizer = QueryHandler()

        def search(self, query_str):
            resp = self.normalizer.search_sources(query_str, keyed=True,
                                                  incl="drugsatfda")
            return resp["source_matches"]["DrugsAtFDA"]
    return QueryGetter()


@pytest.fixture(scope="module")
def everolimus() -> Dict:
    """Build afinitor test fixture."""
    return {
        "label": "EVEROLIMUS",
        "concept_id": "drugsatfda:NDA022334",
        "aliases": [],
        "xrefs": [
            "rxcui:998191",
            "rxcui:1310144",
            "rxcui:1310147",
            "rxcui:845507",
            "rxcui:845518",
            "rxcui:845515",
            "rxcui:1310138",
            "rxcui:845512",
            "rxcui:998189",
            "rxcui:1308430",
            "rxcui:1119402",
            "rxcui:1308428",
            "rxcui:1119400",
            "rxcui:1308432"
        ],
        "associated_with": ["unii:9HW64Q8G6G"],
        "approval_status": "fda_prescription",
        "trade_names": ["AFINITOR"]
    }


@pytest.fixture(scope="module")
def cosmegen() -> Dict:
    """Build cosmegen test fixture."""
    return {
        "label": "DACTINOMYCIN",
        "concept_id": "drugsatfda:NDA050682",
        "xrefs": [
            "rxcui:105569",
            "rxcui:239179"
        ],
        "associated_with": ["unii:1CC1JFE158"],
        "approval_status": "fda_prescription",
        "trade_names": ["COSMEGEN"]
    }


@pytest.fixture(scope="module")
def cisplatin() -> List[Dict]:
    """Build cisplatin test fixtures"""
    return [
        {
            "label": "CISPLATIN",
            "concept_id": "drugsatfda:ANDA074656",
            "xrefs": ["rxcui:309311"],
            "associated_with": ["unii:Q20Q21Q62J"],
            "approval_status": "fda_prescription",
        },
        {
            "label": "CISPLATIN",
            "concept_id": "drugsatfda:ANDA075036",
            "xrefs": ["rxcui:309311"],
            "associated_with": ["unii:Q20Q21Q62J"],
            "approval_status": "fda_prescription",
        },
        {
            "label": "CISPLATIN",
            "concept_id": "drugsatfda:ANDA074735",
            "xrefs": ["rxcui:309311"],
            "associated_with": ["unii:Q20Q21Q62J"],
            "approval_status": "fda_prescription",
        },
        {
            "label": "CISPLATIN",
            "concept_id": "drugsatfda:ANDA206774",
            "xrefs": ["rxcui:309311"],
            "associated_with": ["unii:Q20Q21Q62J"],
            "approval_status": "fda_prescription",
        },
        {
            "label": "CISPLATIN",
            "concept_id": "drugsatfda:ANDA207323",
            "aliases": [],
            "xrefs": ["rxcui:309311"],
            "associated_with": ["unii:Q20Q21Q62J"],
            "approval_status": "fda_prescription",
        }
    ]


def test_everolimus(drugsatfda, everolimus):
    """Test everolimus/afinitor"""
    # test concept ID
    response = drugsatfda.search("drugsatfda:NDA022334")
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert len(response["records"]) == 1
    compare_records(response[0], everolimus)

    response = drugsatfda.search("drugsatfda:ANDA022334")
    assert response["match_type"] == MatchType.NO_MATCH

    # test label
    response = drugsatfda.search("everolimus")
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert len(response["records"]) == 1
    compare_records(response[0], everolimus)

    # test trade name
    response = drugsatfda.search('afinitor')
    assert response["match_type"] == MatchType.TRADE_NAME
    assert len(response["records"]) == 1
    compare_records(response[0], everolimus)

    # test xref
    response = drugsatfda.search('rxcui:998191')
    assert response["match_type"] == MatchType.XREF
    assert len(response["records"]) == 1
    compare_records(response[0], everolimus)

    response = drugsatfda.search('rxcui:845507')
    assert response["match_type"] == MatchType.XREF
    assert len(response["records"]) == 1
    compare_records(response[0], everolimus)

    response = drugsatfda.search('rxcui:998189')
    assert response["match_type"] == MatchType.XREF
    assert len(response["records"]) == 1
    compare_records(response[0], everolimus)

    response = drugsatfda.search('rxcui:1308432')
    assert response["match_type"] == MatchType.XREF
    assert len(response["records"]) == 1
    compare_records(response[0], everolimus)


def test_cosmegen(drugsatfda, cosmegen):
    """Test cosmegen"""
    # test concept ID
    response = drugsatfda.search("drugsatfda:NDA050682")
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert len(response["records"]) == 1
    compare_records(response[0], cosmegen)

    response = drugsatfda.search("drugsatfda:ANDA050682")


def test_other_parameters(drugsatfda):
    """Test other design decisions that aren't captured in the defined
    fixtures.
    """
    # test dropping BLA records
    response = drugsatfda.search("drugsatfda:BLA020725")
    assert response["match_type"] == MatchType.NO_MATCH


def test_meta(drugsatfda):
    """Test correctness of source metadata."""
    response = drugsatfda.search("incoherent-string-of-text")
    assert response["source_meta_"]["data_license"] == "CC0"
    assert response["source_meta_"]["data_license_url"] == "https://creativecommons.org/publicdomain/zero/1.0/legalcode"  # noqa: E501
    version = response["source_meta_"]["version"]
    assert datetime.strptime(version, "%Y%m%d")
    assert response["source_meta_"]["data_url"] == "https://open.fda.gov/apis/drug/drugsfda/download/"  # noqa: E501
    assert response["source_meta_"]["rdp_url"] is None
    assert response["source_meta_"]["data_license_attributes"] == {
        "non_commercial": False,
        "share_alike": False,
        "attribution": False,
    }
