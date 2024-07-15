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
            "spl:a2136e71-186e-4bfc-901b-5ed9ca048a1f",
            "unii:9HW64Q8G6G",
        ],
        approval_ratings=["fda_prescription"],
        trade_names=["AFINITOR"],
    )


@pytest.fixture(scope="module")
def dactinomycin() -> Therapy:
    """Build cosmegen test fixture."""
    return Therapy(
        label="DACTINOMYCIN",
        concept_id="drugsatfda.nda:050682",
        xrefs=["rxcui:105569", "rxcui:239179"],
        associated_with=[
            "ndc:55292-811",
            "ndc:66993-489",
            "spl:c76d44e7-3869-4e8b-93cf-b031f6f62c0a",
            "spl:2afdf978-8e90-40e9-872f-9f8775dcd41c",
            "unii:1CC1JFE158",
        ],
        approval_ratings=["fda_prescription"],
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
                "spl:2c569ef0-588f-4828-8b2d-03a2120c9b4c",
                "unii:Q20Q21Q62J",
            ],
            approval_ratings=["fda_prescription"],
        ),
        Therapy(
            label="CISPLATIN",
            concept_id="drugsatfda.anda:074735",
            xrefs=["rxcui:309311"],
            associated_with=[
                "ndc:63323-103",
                "spl:9b008181-ab66-db2f-e053-2995a90aad57",
                "unii:Q20Q21Q62J",
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
                "spl:c3ddc4a5-9f1b-a8ee-e053-2a95a90a2265",
                "spl:c43de769-d6d8-3bb9-e053-2995a90a5aa2",
                "unii:Q20Q21Q62J",
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
                "ndc:70860-206",
                "spl:01c7a680-ee0d-42da-85e8-8d56c6fe7006",
                "spl:0219ee77-eb38-b81f-e063-6394a90a5ca4",
                "spl:64bcce1a-6e31-4e73-8da5-11aa9e890da2",
                "spl:89007399-3c34-40d2-8068-a0b8e09cbef8",
                "unii:Q20Q21Q62J",
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
                "spl:a66eda32-1164-439a-ac8e-73138365ec06",
                "spl:dd45d777-d4c1-40ee-b4f0-c9e001a15a8c",
                "unii:Q20Q21Q62J",
            ],
            trade_names=["PLATINOL-AQ", "PLATINOL"],
        ),
    ]


@pytest.fixture(scope="module")
def fenortho() -> list[Therapy]:
    """Provide fenortho fixture. Tests for correct handling of conflicting
    approval_ratings values.
    """
    return [
        Therapy(
            label="FENOPROFEN CALCIUM",
            concept_id="drugsatfda.anda:072267",
            xrefs=["rxcui:310291", "rxcui:351398"],
            associated_with=[
                "ndc:42195-471",
                "ndc:42195-688",
                "unii:0X2CW1QABJ",
                "spl:079e140a-b3a2-1c2e-e063-6394a90a8491",
                "spl:079e260a-4bee-a423-e063-6394a90aaf0d",
            ],
            approval_ratings=["fda_prescription"],
            trade_names=["NALFON"],
        ),
        Therapy(
            label="FENOPROFEN CALCIUM",
            concept_id="drugsatfda.nda:017604",
            xrefs=[
                "rxcui:197694",
                "rxcui:197695",
                "rxcui:260323",
                "rxcui:858116",
                "rxcui:858118",
            ],
            associated_with=[
                "ndc:0276-0501",
                "ndc:0276-0502",
                "ndc:0276-0503",
                "ndc:15014-400",
                "ndc:42195-100",
                "ndc:42195-308",
                "ndc:42195-600",
                "spl:d8a6c130-bc1d-01a4-e053-2995a90afd52",
                "spl:f0305a11-4494-e237-e053-2995a90ad12e",
                "unii:0X2CW1QABJ",
                "spl:079cb594-ac07-14e3-e063-6394a90aeb77",
                "spl:079db405-aab1-a400-e063-6294a90a1489",
            ],
            trade_names=["NALFON"],
        ),
        Therapy(
            concept_id="drugsatfda.anda:214475",
            label="FENOPROFEN CALCIUM",
            xrefs=["rxcui:858116"],
            associated_with=[
                "spl:b3a58d34-7780-4056-b144-bad674c51494",
                "unii:0X2CW1QABJ",
                "ndc:16571-688",
            ],
            approval_ratings=["fda_prescription"],
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
    """Test dactinomycin"""
    concept_id = "drugsatfda.nda:050682"

    # test concept ID
    response = drugsatfda.search(concept_id)
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], dactinomycin)

    # test label
    response = drugsatfda.search("DACTINOMYCIN")
    assert response.match_type == MatchType.LABEL
    records = [r for r in response.records if r.concept_id == concept_id]
    compare_records(records[0], dactinomycin)

    # test trade name
    response = drugsatfda.search("cosmegen")
    assert response.match_type == MatchType.TRADE_NAME
    records = [r for r in response.records if r.concept_id == concept_id]
    compare_records(records[0], dactinomycin)

    # test xref
    response = drugsatfda.search("rxcui:105569")
    assert response.match_type == MatchType.XREF
    records = [r for r in response.records if r.concept_id == concept_id]
    compare_records(records[0], dactinomycin)

    response = drugsatfda.search("rxcui:239179")
    assert response.match_type == MatchType.XREF
    records = [r for r in response.records if r.concept_id == concept_id]
    compare_records(records[0], dactinomycin)

    # test assoc_with
    response = drugsatfda.search("unii:1cc1jfe158")
    assert response.match_type == MatchType.XREF
    records = [r for r in response.records if r.concept_id == concept_id]
    compare_records(records[0], dactinomycin)


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
    """Test fenortho."""
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
    assert len(response.records) == 3
    for r in response.records:
        fixture = next(f for f in fenortho if r.concept_id == f.concept_id)
        compare_records(r, fixture)

    response = drugsatfda.search("rxcui:310291")
    assert response.match_type == MatchType.XREF
    assert len(response.records) == 1
    compare_records(response.records[0], fenortho[0])

    response = drugsatfda.search("unii:0X2CW1QABJ")
    assert response.match_type == MatchType.ASSOCIATED_WITH
    assert len(response.records) == 3
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
