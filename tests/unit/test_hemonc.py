"""Test that the therapy normalizer works as intended for the HemOnc.org
source.
"""

import isodate
import pytest

from therapy.etl.hemonc import HemOnc
from therapy.schemas import MatchType, Therapy


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
        xrefs=["rxcui:2555"],
        associated_with=[],
        approval_ratings=["hemonc_approved"],
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
        xrefs=["rxcui:134547"],
        associated_with=[],
        trade_names=[
            "Belrapzo",
            "Bendamax",
            "Bendawel",
            "Bendeka",
            "Bendit",
            "Innomustine",
            "Leuben",
            "Levact",
            "Maxtorin",
            "MyMust",
            "Purplz",
            "Ribomustin",
            "Treakisym",
            "Treanda",
            "Vivimusta",
            "Xyotin",
        ],
        approval_ratings=["hemonc_approved"],
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
        xrefs=["rxcui:475230"],
        associated_with=[],
        trade_names=["Firmagon", "Gonax"],
        approval_ratings=["hemonc_approved"],
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
        xrefs=["rxcui:68442"],
        trade_names=["Nivestym"],
        approval_ratings=["hemonc_approved"],
        approval_year=["2018"],
    )


@pytest.fixture(scope="module")
def combo_drug():
    """Create fixture for `aspirin and dipyridamole`, a combo drug. Ensure that xref
    to rxcui:226716 isn't added.
    """
    return Therapy(
        label="Aspirin and dipyridamole",
        concept_id="hemonc:48",
        aliases=[],
        xrefs=[],
        trade_names=["Aggrenox"],
        approval_ratings=["hemonc_approved"],
        approval_year=["1999"],
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


def test_trade_name(hemonc, compare_response, bendamustine, degarelix):
    """Test that trade name queries resolve to correct record."""
    response = hemonc.search("bendamax")
    compare_response(response, MatchType.TRADE_NAME, bendamustine)

    response = hemonc.search("purplz")
    compare_response(response, MatchType.TRADE_NAME, bendamustine)

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


def test_combo_drug_xref(hemonc, compare_response, combo_drug):
    """Ensure that xrefs in combo treatments aren't included.

    See https://github.com/cancervariants/therapy-normalization/issues/417
    """
    response = hemonc.search("aspirin and dipyridamole")
    compare_response(response, MatchType.LABEL, combo_drug)

    response = hemonc.search("rxcui:226716")
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
