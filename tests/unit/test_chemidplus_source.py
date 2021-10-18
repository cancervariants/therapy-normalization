"""Test ChemIDplus ETL methods."""
import datetime

import pytest

from therapy.query import QueryHandler
from therapy.schemas import Drug, MatchType
from tests.conftest import compare_records


@pytest.fixture(scope="module")
def chemidplus():
    """Build ETL test fixture."""
    class QueryGetter:

        def __init__(self):
            self.normalizer = QueryHandler()

        def search(self, query_str):
            resp = self.normalizer.search_sources(query_str, keyed=True,
                                                  incl="chemidplus")
            return resp["source_matches"]["ChemIDplus"]
    return QueryGetter()


@pytest.fixture(scope="module")
def penicillin_v():
    """Build test fixture for Penicillin V drug."""
    return Drug(**{
        "concept_id": "chemidplus:87-08-1",
        "label": "Penicillin V",
        "aliases": [
            "Phenoxymethylpenicillin"
        ],
        "trade_names": [],
        "xrefs": [
            "drugbank:DB00417",
        ],
        "associated_with": [
            "unii:Z61I075U2W",
        ]
    })


@pytest.fixture(scope="module")
def imatinib():
    """Build test fixture for Imatinib."""
    return Drug(**{
        "concept_id": "chemidplus:152459-95-5",
        "label": "Imatinib",
        "aliases": [],
        "trade_names": [],
        "xrefs": [
            "drugbank:DB00619",
        ],
        "associated_with": [
            "unii:BKJ8M8G5HI",
        ]
    })


@pytest.fixture(scope="module")
def other_imatinib():
    """Build test fixture for imatinib mesylate."""
    return Drug(**{
        "concept_id": "chemidplus:220127-57-1",
        "label": "Imatinib mesylate",
        "aliases": [],
        "xrefs": ["drugbank:DB00619"],
        "associated_with": ["unii:8A1O1M485B"],
        "trade_names": [],
    })


@pytest.fixture(scope="module")
def cisplatin():
    """Build test fixture for cisplatin."""
    return Drug(**{
        "concept_id": "chemidplus:15663-27-1",
        "label": "Cisplatin",
        "aliases": [
            "cis-Diaminedichloroplatinum",
            "1,2-Diaminocyclohexaneplatinum II citrate"
        ],
        "trade_names": [],
        "xrefs": [
            "drugbank:DB00515"
        ],
        "associated_with": [
            "unii:Q20Q21Q62J"
        ]
    })


def test_penicillin(chemidplus, penicillin_v):
    """Test record retrieval for Penicillin V."""
    response = chemidplus.search("chemidplus:87-08-1")
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert len(response["records"]) == 1
    compare_records(response["records"][0], penicillin_v)

    response = chemidplus.search("CHemidplus:87-08-1")
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert len(response["records"]) == 1
    compare_records(response["records"][0], penicillin_v)

    response = chemidplus.search("Penicillin V")
    assert response["match_type"] == MatchType.LABEL
    assert len(response["records"]) == 1
    compare_records(response["records"][0], penicillin_v)

    response = chemidplus.search("Phenoxymethylpenicillin")
    assert response["match_type"] == MatchType.ALIAS
    assert len(response["records"]) == 1
    compare_records(response["records"][0], penicillin_v)

    response = chemidplus.search("drugbank:DB00417")
    assert response["match_type"] == MatchType.XREF
    assert len(response["records"]) == 1
    compare_records(response["records"][0], penicillin_v)

    response = chemidplus.search("unii:Z61I075U2W")
    assert response["match_type"] == MatchType.ASSOCIATED_WITH
    assert len(response["records"]) == 1
    compare_records(response["records"][0], penicillin_v)

    response = chemidplus.search("87-08-1")
    assert response["match_type"] == MatchType.NO_MATCH

    response = chemidplus.search("87081")
    assert response["match_type"] == MatchType.NO_MATCH

    response = chemidplus.search("PenicillinV")
    assert response["match_type"] == MatchType.NO_MATCH


def test_imatinib(chemidplus, imatinib, other_imatinib):
    """Test record retrieval for imatinib."""
    response = chemidplus.search("Imatinib")
    assert response["match_type"] == MatchType.LABEL
    assert len(response["records"]) == 1
    compare_records(response["records"][0], imatinib)

    response = chemidplus.search("imatiniB")
    assert response["match_type"] == MatchType.LABEL
    assert len(response["records"]) == 1
    compare_records(response["records"][0], imatinib)

    response = chemidplus.search("drugbank:DB00619")
    assert response["match_type"] == MatchType.XREF
    assert len(response["records"]) == 2
    for record in response["records"]:
        if record["concept_id"] == "chemidplus:152459-95-5":
            compare_records(record, imatinib)
        elif record["concept_id"] == "chemidplus:220127-57-1":
            compare_records(record, other_imatinib)
        else:
            assert False  # retrieved incorrect record

    response = chemidplus.search("unii:BKJ8M8G5HI")
    assert response["match_type"] == MatchType.ASSOCIATED_WITH
    assert len(response["records"]) == 1
    compare_records(response["records"][0], imatinib)


def test_cisplatin(chemidplus, cisplatin):
    """Test record retrieval for cisplatin."""
    response = chemidplus.search("chemidplus:15663-27-1")
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert len(response["records"]) == 1
    compare_records(response["records"][0], cisplatin)

    response = chemidplus.search("Cisplatin")
    assert response["match_type"] == MatchType.LABEL
    assert len(response["records"]) == 1
    compare_records(response["records"][0], cisplatin)

    response = chemidplus.search("cis-Diaminedichloroplatinum")
    assert response["match_type"] == MatchType.ALIAS
    assert len(response["records"]) == 1
    compare_records(response["records"][0], cisplatin)

    response = chemidplus.search("drugbank:DB00515")
    assert response["match_type"] == MatchType.XREF
    assert len(response["records"]) == 1
    compare_records(response["records"][0], cisplatin)

    response = chemidplus.search("Cisplatine")
    assert response["match_type"] == MatchType.NO_MATCH


def test_meta(chemidplus):
    """Test correctness of source metadata."""
    response = chemidplus.search("incoherent-string-of-text")
    assert response["source_meta_"]["data_license"] == "custom"
    assert response["source_meta_"]["data_license_url"] == "https://www.nlm.nih.gov/databases/download/terms_and_conditions.html"  # noqa: E501
    version = response["source_meta_"]["version"]
    assert datetime.datetime.strptime(version, "%Y%m%d")
    assert response["source_meta_"]["data_url"] == "ftp://ftp.nlm.nih.gov/nlmdata/.chemidlease/"  # noqa: E501
    assert response["source_meta_"]["rdp_url"] is None
    assert response["source_meta_"]["data_license_attributes"] == {
        "non_commercial": False,
        "share_alike": False,
        "attribution": True
    }
