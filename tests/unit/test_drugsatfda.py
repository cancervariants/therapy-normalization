"""Test correctness of Drugs@FDA ETL methods."""
from datetime import datetime as dt
from typing import List

import pytest

from tests.conftest import compare_records
from therapy.query import QueryHandler
from therapy.schemas import MatchType, Drug


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
def everolimus() -> Drug:
    """Build afinitor test fixture."""
    return Drug(**{
        "label": "EVEROLIMUS",
        "concept_id": "drugsatfda:NDA022334",
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
    })


@pytest.fixture(scope="module")
def dactinomycin() -> Drug:
    """Build cosmegen test fixture."""
    return Drug(**{
        "label": "DACTINOMYCIN",
        "concept_id": "drugsatfda:NDA050682",
        "xrefs": [
            "rxcui:105569",
            "rxcui:239179"
        ],
        "associated_with": ["unii:1CC1JFE158"],
        "approval_status": "fda_prescription",
        "trade_names": ["COSMEGEN"]
    })


@pytest.fixture(scope="module")
def cisplatin() -> List[Drug]:
    """Build cisplatin test fixtures"""
    return [
        Drug(**{
            "label": "CISPLATIN",
            "concept_id": "drugsatfda:ANDA074656",
            "xrefs": ["rxcui:309311"],
            "associated_with": ["unii:Q20Q21Q62J"],
            "approval_status": "fda_prescription",
        }),
        Drug(**{
            "label": "CISPLATIN",
            "concept_id": "drugsatfda:ANDA075036",
            "xrefs": ["rxcui:309311"],
            "associated_with": ["unii:Q20Q21Q62J"],
            "approval_status": "fda_prescription",
        }),
        Drug(**{
            "label": "CISPLATIN",
            "concept_id": "drugsatfda:ANDA074735",
            "xrefs": ["rxcui:309311"],
            "associated_with": ["unii:Q20Q21Q62J"],
            "approval_status": "fda_prescription",
        }),
        Drug(**{
            "label": "CISPLATIN",
            "concept_id": "drugsatfda:ANDA206774",
            "xrefs": ["rxcui:309311"],
            "associated_with": ["unii:Q20Q21Q62J"],
            "approval_status": "fda_prescription",
        }),
        Drug(**{
            "label": "CISPLATIN",
            "concept_id": "drugsatfda:ANDA207323",
            "xrefs": ["rxcui:309311"],
            "associated_with": ["unii:Q20Q21Q62J"],
            "approval_status": "fda_prescription",
        }),
        Drug(**{
            "label": "CISPLATIN",
            "concept_id": "drugsatfda:NDA018057",
            "xrefs": ["rxcui:1736854", "rxcui:309311"],
            "associated_with": ["unii:Q20Q21Q62J"],
            "trade_names": ["PLATINOL-AQ", "PLATINOL"]
        }),
    ]


@pytest.fixture(scope="module")
def fenortho() -> List[Drug]:
    """Provide fenortho fixture. Tests for correct handling of conflicting
    approval_status values.
    """
    return [
        Drug(**{
            "label_and_type": "drugsatfda:anda072267##identity",
            "item_type": "identity",
            "src_name": "DrugsAtFDA",
            "label": "FENOPROFEN CALCIUM",
            "concept_id": "drugsatfda:ANDA072267",
            "xrefs": ["rxcui:310291", "rxcui:351398"],
            "associated_with": ["unii:0X2CW1QABJ"],
            "approval_status": "fda_prescription",
            "trade_names": ["NALFON"]
        }),
        Drug(**{
            "label_and_type": "drugsatfda:NDA017604##identity",
            "item_type": "identity",
            "src_name": "DrugsAtFDA",
            "label": "FENOPROFEN CALCIUM",
            "concept_id": "drugsatfda:NDA017604",
            "xrefs": [
                "rxcui:197694",
                "rxcui:858118",
                "rxcui:858116",
                "rxcui:260323"
            ],
            "associated_with": ["unii:0X2CW1QABJ"],
            "trade_names": ["NALFON", "FENORTHO"]
        }),
    ]


def test_everolimus(drugsatfda, everolimus):
    """Test everolimus/afinitor"""
    concept_id = "drugsatfda:NDA022334"

    # test concept ID
    response = drugsatfda.search(concept_id)
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert len(response["records"]) == 1
    compare_records(response["records"][0], everolimus)

    response = drugsatfda.search("drugsatfda:ANDA022334")
    assert response["match_type"] == MatchType.NO_MATCH

    # test label
    response = drugsatfda.search("everolimus")
    assert response["match_type"] == MatchType.LABEL
    records = [r for r in response["records"] if r["concept_id"] == concept_id]
    compare_records(records[0], everolimus)

    # test trade name
    response = drugsatfda.search("afinitor")
    assert response["match_type"] == MatchType.TRADE_NAME
    records = [r for r in response["records"] if r["concept_id"] == concept_id]
    compare_records(records[0], everolimus)

    # test xref
    response = drugsatfda.search("rxcui:998191")
    assert response["match_type"] == MatchType.XREF
    records = [r for r in response["records"] if r["concept_id"] == concept_id]
    compare_records(records[0], everolimus)

    response = drugsatfda.search("rxcui:845507")
    assert response["match_type"] == MatchType.XREF
    records = [r for r in response["records"] if r["concept_id"] == concept_id]
    compare_records(records[0], everolimus)

    response = drugsatfda.search("rxcui:998189")
    assert response["match_type"] == MatchType.XREF
    records = [r for r in response["records"] if r["concept_id"] == concept_id]
    compare_records(records[0], everolimus)

    response = drugsatfda.search("rxcui:1308432")
    assert response["match_type"] == MatchType.XREF
    records = [r for r in response["records"] if r["concept_id"] == concept_id]
    compare_records(records[0], everolimus)

    # test assoc_with
    response = drugsatfda.search("unii:9HW64Q8G6G")
    assert response["match_type"] == MatchType.ASSOCIATED_WITH
    records = [r for r in response["records"] if r["concept_id"] == concept_id]
    compare_records(records[0], everolimus)


def test_dactinomycin(drugsatfda, dactinomycin):
    """Test dactinomycin"""
    concept_id = "drugsatfda:NDA050682"

    # test concept ID
    response = drugsatfda.search(concept_id)
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert len(response["records"]) == 1
    compare_records(response["records"][0], dactinomycin)

    # test label
    response = drugsatfda.search("DACTINOMYCIN")
    assert response["match_type"] == MatchType.LABEL
    records = [r for r in response["records"] if r["concept_id"] == concept_id]
    compare_records(records[0], dactinomycin)

    # test trade name
    response = drugsatfda.search("cosmegen")
    assert response["match_type"] == MatchType.TRADE_NAME
    records = [r for r in response["records"] if r["concept_id"] == concept_id]
    compare_records(records[0], dactinomycin)

    # test xref
    response = drugsatfda.search("rxcui:105569")
    assert response["match_type"] == MatchType.XREF
    records = [r for r in response["records"] if r["concept_id"] == concept_id]
    compare_records(records[0], dactinomycin)

    response = drugsatfda.search("rxcui:239179")
    assert response["match_type"] == MatchType.XREF
    records = [r for r in response["records"] if r["concept_id"] == concept_id]
    compare_records(records[0], dactinomycin)

    # test assoc_with
    response = drugsatfda.search("unii:1cc1jfe158")
    assert response["match_type"] == MatchType.XREF
    records = [r for r in response["records"] if r["concept_id"] == concept_id]
    compare_records(records[0], dactinomycin)


def test_cisplatin(drugsatfda, cisplatin):
    """Test cisplatin"""
    # test concept IDs
    concept_id = "drugsatfda:ANDA074656"
    response = drugsatfda.search(concept_id)
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert len(response["records"]) == 1
    compare_records(response["records"][0],
                    [c for c in cisplatin if c.concept_id == concept_id][0])

    concept_id = "drugsatfda:anda075036"
    response = drugsatfda.search(concept_id)
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert len(response["records"]) == 1
    compare_records(response["records"][0],
                    [c for c in cisplatin
                     if c.concept_id.lower() == concept_id.lower()][0])

    concept_id = "drugsatfda:ANDA074735"
    response = drugsatfda.search(concept_id)
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert len(response["records"]) == 1
    compare_records(response["records"][0],
                    [c for c in cisplatin if c.concept_id == concept_id][0])

    concept_id = "drugsatfda:ANDA206774"
    response = drugsatfda.search(concept_id)
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert len(response["records"]) == 1
    compare_records(response["records"][0],
                    [c for c in cisplatin if c.concept_id == concept_id][0])

    concept_id = "drugsatfda:ANDA207323"
    response = drugsatfda.search(concept_id)
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert len(response["records"]) == 1
    compare_records(response["records"][0],
                    [c for c in cisplatin if c.concept_id == concept_id][0])

    # test label
    response = drugsatfda.search("cisplatin")
    assert response["match_type"] == MatchType.LABEL
    assert len(response["records"]) == 6
    for r in response["records"]:
        fixture = [c for c in cisplatin if r["concept_id"] == c.concept_id][0]
        compare_records(r, fixture)

    # test xref
    response = drugsatfda.search("rxcui:309311")
    assert response["match_type"] == MatchType.XREF
    assert len(response["records"]) == 6
    for r in response["records"]:
        fixture = [c for c in cisplatin if r["concept_id"] == c.concept_id][0]
        compare_records(r, fixture)

    # test assoc_with
    response = drugsatfda.search("unii:q20q21q62j")
    assert response["match_type"] == MatchType.ASSOCIATED_WITH
    assert len(response["records"]) == 6
    for r in response["records"]:
        fixture = [c for c in cisplatin if r["concept_id"] == c.concept_id][0]
        compare_records(r, fixture)


def test_fenortho(fenortho, drugsatfda):
    """Test fenortho."""
    response = drugsatfda.search("drugsatfda:ANDA072267")
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert len(response["records"]) == 1
    compare_records(response["records"][0], fenortho[0])

    response = drugsatfda.search("drugsatfda:NDA017604")
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert len(response["records"]) == 1
    compare_records(response["records"][0], fenortho[1])

    response = drugsatfda.search("fenoprofen calcium")
    assert response["match_type"] == MatchType.LABEL
    assert len(response["records"]) == 2
    for r in response["records"]:
        fixture = [f for f in fenortho if r["concept_id"] == f.concept_id][0]
        compare_records(r, fixture)

    response = drugsatfda.search("rxcui:310291")
    assert response["match_type"] == MatchType.XREF
    assert len(response["records"]) == 1
    compare_records(response["records"][0], fenortho[0])

    response = drugsatfda.search("unii:0X2CW1QABJ")
    assert response["match_type"] == MatchType.ASSOCIATED_WITH
    assert len(response["records"]) == 2
    for r in response["records"]:
        fixture = [f for f in fenortho if r["concept_id"] == f.concept_id][0]
        compare_records(r, fixture)


def test_other_parameters(drugsatfda):
    """Test other design decisions that aren"t captured in the defined
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
    assert dt.strptime(response["source_meta_"]["version"], "%Y%m%d")
    assert response["source_meta_"]["data_url"] == "https://open.fda.gov/apis/drug/drugsfda/download/"  # noqa: E501
    assert response["source_meta_"]["rdp_url"] is None
    assert response["source_meta_"]["data_license_attributes"] == {
        "non_commercial": False,
        "share_alike": False,
        "attribution": False,
    }
