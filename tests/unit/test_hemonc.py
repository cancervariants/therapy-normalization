"""Test that the therapy normalizer works as intended for the HemOnc.org
source.
"""

import isodate
import pytest

from therapy.etl.hemonc import HemOnc
from therapy.schemas import ApprovalRating, MatchType, Therapy


@pytest.fixture(scope="module")
def hemonc(test_source):
    """Provide test HemOnc query endpoint"""
    return test_source(HemOnc)


@pytest.fixture(scope="module")
def cisplatin():
    """Construct cisplatin fixture."""
    return Therapy(
        concept_id="hemonc:105",
        label="Cisplatin",
        aliases=[
            "cis-diamminedichloroplatinum III",
            "DDP",
            "cisplatinum",
            "cis-platinum",
            "DACP",
            "NSC 119875",
            "NSC-119875",
            "NSC119875",
            "CDDP",
        ],
        trade_names=[],
        xrefs=[
            "hcpcs:C9418",
            "hcpcs:J9060",
            "hcpcs:J9062",
            "ndc:0143-9504",
            "ndc:0143-9505",
            "ndc:0703-5747",
            "ndc:0703-5748",
            "ndc:16729-288",
            "ndc:44567-509",
            "ndc:44567-510",
            "ndc:44567-511",
            "ndc:44567-530",
            "ndc:63323-103",
            "ndc:68001-283",
            "ndc:68083-162",
            "ndc:68083-163",
            "ndc:70860-206",
            "rxcui:2555",
        ],
        associated_with=[],
        approval_ratings=[ApprovalRating.HEMONC_APPROVED],
        approval_year=["1978"],
        has_indication=[
            {
                "disease_id": "hemonc:569",
                "disease_label": "Bladder cancer",
                "normalized_disease_id": "ncit:C9334",
                "supplemental_info": {"regulatory_body": "FDA"},
            },
            {
                "disease_id": "hemonc:671",
                "disease_label": "Testicular cancer",
                "normalized_disease_id": "ncit:C7251",
                "supplemental_info": {"regulatory_body": "FDA"},
            },
            {
                "disease_id": "hemonc:645",
                "disease_label": "Ovarian cancer",
                "normalized_disease_id": "ncit:C7431",
                "supplemental_info": {"regulatory_body": "FDA"},
            },
        ],
    )


@pytest.fixture(scope="module")
def bendamustine():
    """Construct bendamustine fixture."""
    return Therapy(
        concept_id="hemonc:65",
        label="Bendamustine",
        aliases=[
            "CEP-18083",
            "cytostasan hydrochloride",
            "SyB L-0501",
            "SDX-105",
            "CEP 18083",
            "CEP18083",
            "SDX 105",
            "SDX105",
            "bendamustine hydrochloride",
            "bendamustin hydrochloride",
        ],
        xrefs=[
            "hcpcs:C9042",
            "hcpcs:C9243",
            "hcpcs:J9033",
            "hcpcs:J9034",
            "hcpcs:J9036",
            "hcpcs:J9056",
            "hcpcs:J9058",
            "hcpcs:J9059",
            "ndc:42367-521",
            "ndc:63459-348",
            "ndc:63459-390",
            "ndc:63459-391",
            "rxcui:134547",
        ],
        associated_with=[],
        trade_names=[],
        approval_ratings=[ApprovalRating.HEMONC_APPROVED],
        approval_year=["2008"],
        has_indication=[
            {
                "disease_id": "hemonc:581",
                "disease_label": "Chronic lymphocytic leukemia",
                "normalized_disease_id": "ncit:C3163",
                "supplemental_info": {"regulatory_body": "FDA"},
            },
            {
                "disease_id": "hemonc:46094",
                "disease_label": "Indolent lymphoma",
                "normalized_disease_id": "ncit:C8504",
                "supplemental_info": {"regulatory_body": "FDA"},
            },
        ],
    )


@pytest.fixture(scope="module")
def degarelix():
    """Create fixture for degarelix drug."""
    return Therapy(
        label="Degarelix",
        concept_id="hemonc:151",
        aliases=[
            "degarelix acetate",
            "FE200486",
            "ASP3550",
            "ASP 3550",
            "ASP-3550",
            "FE 200486",
            "FE-200486",
        ],
        xrefs=["hcpcs:J9155", "ndc:55566-8303", "ndc:55566-8403", "rxcui:475230"],
        associated_with=[],
        trade_names=[
            "Degafelix",
            "Degalyn",
            "Degapride",
            "Degatide",
            "Deghor",
            "Degrinta",
            "Firmagon",
            "Gonax",
            "Segarelix",
        ],
        approval_ratings=[ApprovalRating.HEMONC_APPROVED],
        approval_year=["2008"],
        has_indication=[
            {
                "disease_id": "hemonc:658",
                "disease_label": "Prostate cancer",
                "normalized_disease_id": "ncit:C7378",
                "supplemental_info": {"regulatory_body": "FDA"},
            }
        ],
    )


@pytest.fixture(scope="module")
def filgrastim():
    """Create fixture for filgrastim drug (tests handling of deprecated brand name)"""
    return Therapy(
        label="Filgrastim-aafi",
        concept_id="hemonc:66258",
        aliases=[],
        xrefs=[
            "hcpcs:Q5110",
            "ndc:0069-0291",
            "ndc:0069-0292",
            "ndc:0069-0293",
            "ndc:0069-0294",
            "rxcui:2057198",
        ],
        trade_names=["Nivestym", "Nivestim"],
        approval_ratings=[ApprovalRating.HEMONC_APPROVED],
        approval_year=["2018"],
    )


def test_concept_id_match(
    hemonc, compare_response, cisplatin, bendamustine, degarelix, filgrastim
):
    """Test that concept ID queries resolve to correct record."""
    response = hemonc.search("hemonc:105")
    compare_response(response, MatchType.CONCEPT_ID, cisplatin)

    response = hemonc.search("hemonc:65")
    compare_response(response, MatchType.CONCEPT_ID, bendamustine)

    response = hemonc.search("hemonc:151")
    compare_response(response, MatchType.CONCEPT_ID, degarelix)

    response = hemonc.search("hemonc:66258")
    compare_response(response, MatchType.CONCEPT_ID, filgrastim)


def test_label_match(
    hemonc, compare_response, cisplatin, bendamustine, degarelix, filgrastim
):
    """Test that label queries resolve to correct record."""
    response = hemonc.search("cisplatin")
    compare_response(response, MatchType.LABEL, cisplatin)

    response = hemonc.search("Bendamustine")
    compare_response(response, MatchType.LABEL, bendamustine)

    response = hemonc.search("DEGARELIX")
    compare_response(response, MatchType.LABEL, degarelix)

    response = hemonc.search("Filgrastim-aafi")
    compare_response(response, MatchType.LABEL, filgrastim)


def test_alias_match(hemonc, compare_response, cisplatin, bendamustine, degarelix):
    """Test that alias queries resolve to correct record."""
    response = hemonc.search("ddp")
    compare_response(response, MatchType.ALIAS, cisplatin)

    response = hemonc.search("dacp")
    compare_response(response, MatchType.ALIAS, cisplatin)

    response = hemonc.search("nsc 119875")
    compare_response(response, MatchType.ALIAS, cisplatin)

    response = hemonc.search("cep-18083")
    compare_response(response, MatchType.ALIAS, bendamustine)

    response = hemonc.search("bendamustine hydrochloride")
    compare_response(response, MatchType.ALIAS, bendamustine)

    response = hemonc.search("asp3550")
    compare_response(response, MatchType.ALIAS, degarelix)


def test_trade_name(hemonc, compare_response, degarelix):
    """Test that trade name queries resolve to correct record."""
    response = hemonc.search("firmagon")
    compare_response(response, MatchType.TRADE_NAME, degarelix)

    # no trade names for records with > 20
    response = hemonc.search("platinol")
    assert response.match_type == MatchType.NO_MATCH


def test_xref_match(hemonc, compare_response, cisplatin, bendamustine, degarelix):
    """Test that xref query resolves to correct record."""
    response = hemonc.search("rxcui:2555")
    compare_response(response, MatchType.XREF, cisplatin)

    response = hemonc.search("rxcui:134547")
    compare_response(response, MatchType.XREF, bendamustine)

    response = hemonc.search("rxcui:475230")
    compare_response(response, MatchType.XREF, degarelix)


def test_qc(hemonc):
    """Test that QC checks are restricting unsupported records from being imported.

    See https://github.com/cancervariants/therapy-normalization/issues/417
    """
    response = hemonc.search("aspirin and dipyridamole")
    assert response.match_type == MatchType.NO_MATCH

    response = hemonc.search("rxcui:226716")
    assert response.match_type == MatchType.NO_MATCH

    response = hemonc.search("hemonc:517")
    assert response.match_type == MatchType.NO_MATCH


def test_metadata(hemonc):
    """Test that source metadata returns correctly."""
    response = hemonc.search("search")
    assert response.source_meta_.data_license == "CC BY 4.0"
    assert (
        response.source_meta_.data_license_url
        == "https://creativecommons.org/licenses/by/4.0/legalcode"
    )
    assert isodate.parse_date(response.source_meta_.version)
    assert (
        response.source_meta_.data_url
        == "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/9CY9C6"
    )
    assert response.source_meta_.rdp_url is None
    assert response.source_meta_.data_license_attributes == {
        "non_commercial": False,
        "attribution": True,
        "share_alike": False,
    }
