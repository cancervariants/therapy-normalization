"""Test correctness of Drugs@FDA ETL methods."""

import isodate
import pytest

from therapy.etl.drugsatfda import DrugsAtFDA
from therapy.schemas import MatchType, Therapy


@pytest.fixture(scope="module")
def drugsatfda(test_source):
    """Provide test Drugs@FDA query endpoint"""
    return test_source(DrugsAtFDA)


@pytest.fixture(scope="module")
def everolimus() -> Therapy:
    """Build afinitor test fixture."""
    return Therapy(
        label="EVEROLIMUS",
        concept_id="drugsatfda.nda:022334",
        xrefs=[
            "rxcui:1119400",
            "rxcui:1119402",
            "rxcui:1308428",
            "rxcui:1308430",
            "rxcui:1308432",
            "rxcui:1310138",
            "rxcui:1310144",
            "rxcui:1310147",
            "rxcui:845507",
            "rxcui:845512",
            "rxcui:845515",
            "rxcui:845518",
            "rxcui:998189",
            "rxcui:998191",
        ],
        associated_with=[
            "ndc:0078-0566",
            "ndc:0078-0566",
            "ndc:0078-0567",
            "ndc:0078-0594",
            "ndc:0078-0620",
            "ndc:0078-0626",
            "ndc:0078-0627",
            "ndc:0078-0628",
            "unii:9HW64Q8G6G",
            "spl:b808a4e0-23a8-40c8-834d-b31cd642febe",
        ],
        approval_ratings=["fda_prescription"],
        trade_names=["AFINITOR"],
    )


@pytest.fixture(scope="module")
def dactinomycin() -> Therapy:
    """Build cosmegen test fixture."""
    return Therapy(
        label=None,
        concept_id="drugsatfda.nda:050682",
        xrefs=[],
        associated_with=[],
        approval_ratings=["fda_discontinued"],
        trade_names=["COSMEGEN"],
    )


@pytest.fixture(scope="module")
def cisplatin() -> list[Therapy]:
    """Build cisplatin test fixtures"""
    return [
        Therapy(
            label="CISPLATIN",
            concept_id="drugsatfda.anda:074656",
            xrefs=["rxcui:309311"],
            associated_with=[
                "ndc:0703-5747",
                "ndc:0703-5748",
                "spl:adf5773e-9095-4cb4-a90f-72cbf82f4493",
                "unii:Q20Q21Q62J",
            ],
            approval_ratings=["fda_prescription"],
        ),
        Therapy(
            label="CISPLATIN",
            concept_id="drugsatfda.anda:075036",
            xrefs=["rxcui:309311"],
            associated_with=[
                "ndc:0143-9504",
                "ndc:0143-9505",
                "unii:Q20Q21Q62J",
                "spl:b463892e-6ca3-4c57-ae72-bdd3a7370a08",
            ],
            approval_ratings=["fda_prescription"],
        ),
        Therapy(
            label="CISPLATIN",
            concept_id="drugsatfda.anda:074735",
            xrefs=["rxcui:309311"],
            associated_with=[
                "ndc:63323-103",
                "unii:Q20Q21Q62J",
                "spl:24604fc6-875c-4f70-973a-9cca93f9a7a7",
            ],
            approval_ratings=["fda_prescription"],
        ),
        Therapy(
            label="CISPLATIN",
            concept_id="drugsatfda.anda:206774",
            xrefs=["rxcui:309311"],
            associated_with=[
                "ndc:16729-288",
                "ndc:68001-283",
                "unii:Q20Q21Q62J",
                "spl:11314cd7-5167-4699-e063-6394a90a93b8",
                "spl:2860827e-1817-2b85-e063-6394a90a127e",
            ],
            approval_ratings=["fda_prescription"],
        ),
        Therapy(
            label="CISPLATIN",
            concept_id="drugsatfda.anda:207323",
            xrefs=["rxcui:309311"],
            associated_with=[
                "ndc:25021-253",
                "ndc:72266-252",
                "ndc:72266-253",
                "ndc:68083-162",
                "ndc:68083-163",
                "spl:89007399-3c34-40d2-8068-a0b8e09cbef8",
                "unii:Q20Q21Q62J",
                "ndc:68001-608",
                "ndc:68001-609",
                "spl:0c3ef530-de61-73f7-e063-6294a90a9973",
                "spl:189a1ee6-231d-7e56-e063-6394a90abc01",
                "spl:bf489cd3-17a3-4398-ba7e-2cb9e87b9c13",
            ],
            approval_ratings=["fda_prescription"],
        ),
        Therapy(
            label="CISPLATIN",
            concept_id="drugsatfda.nda:018057",
            xrefs=["rxcui:1736854", "rxcui:309311"],
            associated_with=[
                "ndc:44567-509",
                "ndc:44567-510",
                "ndc:44567-511",
                "ndc:44567-530",
                "spl:dd45d777-d4c1-40ee-b4f0-c9e001a15a8c",
                "unii:Q20Q21Q62J",
                "spl:bf273433-d76e-4071-99a9-a71caf064734",
            ],
            trade_names=["PLATINOL-AQ", "PLATINOL"],
        ),
    ]


@pytest.fixture(scope="module")
def fenortho() -> list[Therapy]:
    """Provide discontinued and prescription fenoprofen product fixtures."""
    return [
        Therapy(
            label=None,
            concept_id="drugsatfda.anda:072267",
            xrefs=[],
            associated_with=[],
            approval_ratings=["fda_discontinued"],
            trade_names=["FENOPROFEN CALCIUM"],
        ),
        Therapy(
            label=None,
            concept_id="drugsatfda.nda:017604",
            xrefs=[],
            associated_with=[],
            trade_names=["NALFON"],
            approval_ratings=["fda_discontinued"],
        ),
        Therapy(
            concept_id="drugsatfda.anda:214475",
            label="FENOPROFEN CALCIUM",
            xrefs=["rxcui:858116", "rxcui:197694", "rxcui:197695", "rxcui:2699827"],
            associated_with=[
                "unii:0X2CW1QABJ",
                "ndc:16571-688",
                "ndc:16571-886",
                "ndc:16571-887",
                "ndc:61825-311",
                "ndc:71993-308",
                "ndc:71993-313",
                "spl:14a4aa5b-30ff-8ad7-e063-6294a90a889e",
                "spl:1ad61a67-8758-4f2f-b7d5-f00f62ea69aa",
                "spl:51ac5c0c-2aef-abe1-e063-6294a90a7943",
                "spl:ba5e0066-40c5-4ea4-bd0c-cfcb18bdbc48",
            ],
            approval_ratings=["fda_prescription"],
            trade_names=["FENOPRON TM"],
        ),
    ]


def test_everolimus(drugsatfda, compare_records, everolimus):
    """Test everolimus/afinitor"""
    concept_id = "drugsatfda.nda:022334"

    # test concept ID
    response = drugsatfda.search(concept_id)
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], everolimus)

    response = drugsatfda.search("drugsatfda.anda:022334")
    assert response.match_type == MatchType.NO_MATCH

    # test label
    response = drugsatfda.search("everolimus")
    assert response.match_type == MatchType.LABEL
    records = [r for r in response.records if r.concept_id == concept_id]
    compare_records(records[0], everolimus)

    # test trade name
    response = drugsatfda.search("afinitor")
    assert response.match_type == MatchType.TRADE_NAME
    records = [r for r in response.records if r.concept_id == concept_id]
    compare_records(records[0], everolimus)

    # test xref
    response = drugsatfda.search("rxcui:998191")
    assert response.match_type == MatchType.XREF
    records = [r for r in response.records if r.concept_id == concept_id]
    compare_records(records[0], everolimus)

    response = drugsatfda.search("rxcui:845507")
    assert response.match_type == MatchType.XREF
    records = [r for r in response.records if r.concept_id == concept_id]
    compare_records(records[0], everolimus)

    response = drugsatfda.search("rxcui:998189")
    assert response.match_type == MatchType.XREF
    records = [r for r in response.records if r.concept_id == concept_id]
    compare_records(records[0], everolimus)

    response = drugsatfda.search("rxcui:1308432")
    assert response.match_type == MatchType.XREF
    records = [r for r in response.records if r.concept_id == concept_id]
    compare_records(records[0], everolimus)

    # test assoc_with
    response = drugsatfda.search("unii:9HW64Q8G6G")
    assert response.match_type == MatchType.ASSOCIATED_WITH
    records = [r for r in response.records if r.concept_id == concept_id]
    compare_records(records[0], everolimus)


def test_dactinomycin(drugsatfda, compare_records, dactinomycin):
    """Find discontinued Cosmegen by ID and trade name without OpenFDA metadata."""
    concept_id = "drugsatfda.nda:050682"

    # test concept ID
    response = drugsatfda.search(concept_id)
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], dactinomycin)

    # test label
    response = drugsatfda.search("DACTINOMYCIN")
    assert concept_id not in {r.concept_id for r in response.records}

    # test trade name
    response = drugsatfda.search("cosmegen")
    assert response.match_type == MatchType.TRADE_NAME
    records = [r for r in response.records if r.concept_id == concept_id]
    compare_records(records[0], dactinomycin)

    # test xref
    response = drugsatfda.search("rxcui:105569")
    assert concept_id not in {r.concept_id for r in response.records}

    response = drugsatfda.search("rxcui:239179")
    assert concept_id not in {r.concept_id for r in response.records}

    # test assoc_with
    response = drugsatfda.search("unii:1cc1jfe158")
    assert concept_id not in {r.concept_id for r in response.records}


def test_cisplatin(drugsatfda, compare_records, cisplatin):
    """Test cisplatin"""
    # test concept IDs
    concept_id = "drugsatfda.anda:074656"
    response = drugsatfda.search(concept_id)
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(
        response.records[0], next(c for c in cisplatin if c.concept_id == concept_id)
    )

    concept_id = "drugsatfda.anda:075036"
    response = drugsatfda.search(concept_id)
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(
        response.records[0],
        next(c for c in cisplatin if c.concept_id.lower() == concept_id.lower()),
    )

    concept_id = "drugsatfda.anda:074735"
    response = drugsatfda.search(concept_id)
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(
        response.records[0], next(c for c in cisplatin if c.concept_id == concept_id)
    )

    concept_id = "drugsatfda.anda:206774"
    response = drugsatfda.search(concept_id)
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(
        response.records[0], next(c for c in cisplatin if c.concept_id == concept_id)
    )

    concept_id = "drugsatfda.anda:207323"
    response = drugsatfda.search(concept_id)
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(
        response.records[0], next(c for c in cisplatin if c.concept_id == concept_id)
    )

    # test label
    response = drugsatfda.search("cisplatin")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 6
    for r in response.records:
        fixture = next(c for c in cisplatin if r.concept_id == c.concept_id)
        compare_records(r, fixture)

    # test xref
    response = drugsatfda.search("rxcui:309311")
    assert response.match_type == MatchType.XREF
    assert len(response.records) == 6
    for r in response.records:
        fixture = next(c for c in cisplatin if r.concept_id == c.concept_id)
        compare_records(r, fixture)

    # test assoc_with
    response = drugsatfda.search("unii:q20q21q62j")
    assert response.match_type == MatchType.ASSOCIATED_WITH
    assert len(response.records) == 6
    for r in response.records:
        fixture = next(c for c in cisplatin if r.concept_id == c.concept_id)
        compare_records(r, fixture)


def test_fenortho(fenortho, compare_records, drugsatfda):
    """Find discontinued fenoprofen by ID and the active product by metadata."""
    response = drugsatfda.search("drugsatfda.anda:072267")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], fenortho[0])

    response = drugsatfda.search("drugsatfda.nda:017604")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], fenortho[1])

    response = drugsatfda.search("fenoprofen calcium")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    for r in response.records:
        fixture = next(f for f in fenortho if r.concept_id == f.concept_id)
        compare_records(r, fixture)

    response = drugsatfda.search("rxcui:310291")
    assert response.match_type == MatchType.NO_MATCH
    assert response.records == []

    response = drugsatfda.search("unii:0X2CW1QABJ")
    assert response.match_type == MatchType.ASSOCIATED_WITH
    assert len(response.records) == 1
    for r in response.records:
        fixture = next(f for f in fenortho if r.concept_id == f.concept_id)
        compare_records(r, fixture)


def test_other_parameters(drugsatfda):
    """Test other design decisions that aren"t captured in the defined
    fixtures.
    """
    # test dropping BLA records
    response = drugsatfda.search("drugsatfda:BLA020725")
    assert response.match_type == MatchType.NO_MATCH


def test_meta(drugsatfda):
    """Test correctness of source metadata."""
    response = drugsatfda.search("incoherent-string-of-text")
    assert response.source_meta_.data_license == "CC0"
    assert (
        response.source_meta_.data_license_url
        == "https://creativecommons.org/publicdomain/zero/1.0/legalcode"
    )
    assert isodate.parse_date(response.source_meta_.version)
    assert (
        response.source_meta_.data_url
        == "https://open.fda.gov/apis/drug/drugsfda/download/"
    )
    assert response.source_meta_.rdp_url is None
    assert response.source_meta_.data_license_attributes == {
        "non_commercial": False,
        "share_alike": False,
        "attribution": False,
    }
