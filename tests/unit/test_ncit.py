"""Test NCIT source"""

import re

import pytest

from therapy.etl.ncit import NCIt
from therapy.schemas import MatchType, Therapy


@pytest.fixture(scope="module")
def ncit(test_source):
    """Provide test ncit query endpoint"""
    return test_source(NCIt)


@pytest.fixture(scope="module")
def voglibose():
    """Create a voglibose drug fixture.."""
    params = {
        "label": "Voglibose",
        "concept_id": "ncit:C95221",
        "aliases": [
            "3,4-Dideoxy-4-((2-Hydroxy-1-(Hydroxymethyl)Ethyl)Amino)-2-C-(Hydroxymethyl)-D-Epi-Inositol",  # F401
            "A-71100",
            "AO-128",
            "Basen",
            "N-(1,3-Dihydroxy-2-Propyl)Valiolamine",
            "VOGLIBOSE",
        ],
        "xrefs": ["chemidplus:83480-29-9"],
        "associated_with": ["unii:S77P977AG8", "umls:C0532578"],
        "approval_ratings": None,
        "trade_names": [],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def apricoxib():
    """Create an apricoxib drug fixture."""
    params = {
        "label": "Apricoxib",
        "concept_id": "ncit:C74021",
        "aliases": [
            "APRICOXIB",
            "COX-2 Inhibitor TG01",
            "CS-706",
            "R-109339",
            "TG01",
            "TP2001",
        ],
        "xrefs": ["chemidplus:197904-84-0"],
        "associated_with": ["unii:5X5HB3VZ3Z", "umls:C1737955"],
        "approval_ratings": None,
        "trade_names": [],
    }
    return Therapy(**params)


# Test aliases > 20
@pytest.fixture(scope="module")
def trastuzumab():
    """Create a Trastuzumab drug fixture."""
    params = {
        "label": "Trastuzumab",
        "concept_id": "ncit:C1647",
        "aliases": [],
        "xrefs": ["chemidplus:180288-69-1"],
        "associated_with": ["umls:C0728747", "unii:P188ANX8CK"],
        "approval_ratings": None,
        "trade_names": [],
    }
    return Therapy(**params)


# Needed for MetaKB
@pytest.fixture(scope="module")
def therapeutic_procedure():
    """Create a fixture for the Therapeutic Procedure class."""
    params = {
        "concept_id": "ncit:C49236",
        "label": "Therapeutic Procedure",
        "aliases": [
            "any therapy",
            "any_therapy",
            "Therapeutic",
            "Therapeutic Interventions",
            "Therapeutic Method",
            "Therapeutic Technique",
            "therapy",
            "Therapy",
            "TREAT",
            "Treatment",
            "TX",
            "treatment",
            "treatment or therapy",
            "treatment_or_therapy",
        ],
        "trade_names": [],
        "xrefs": [],
        "associated_with": ["umls:C0087111"],
    }
    return Therapy(**params)


# test correct CHEBI naming
@pytest.fixture(scope="module")
def ivermectin():
    """Create a fixture for Ivermectin."""
    params = {
        "label_and_type": "ncit:c61796",
        "src_name": "NCIt",
        "item_type": "identity",
        "concept_id": "ncit:C61796",
        "label": "Ivermectin",
        "aliases": [
            "Stromectol",
            "IVERMECTIN",
            "Avermectin A1a, 5-O-demethyl-25-de(1-methylpropyl)-22,23-dihydro-25-(1-methylethyl)-",
            "Sklice",
        ],
        "xrefs": ["chemidplus:70288-86-7"],
        "associated_with": ["umls:C0022322", "unii:8883YP2R6D", "CHEBI:6078"],
    }
    return Therapy(**params)


def test_concept_id_match(
    ncit,
    compare_response,
    voglibose,
    apricoxib,
    trastuzumab,
    therapeutic_procedure,
    ivermectin,
):
    """Test that concept ID query resolves to correct record."""
    response = ncit.search("ncit:C95221")
    compare_response(response, MatchType.CONCEPT_ID, voglibose)

    response = ncit.search("NCIT:C95221")
    compare_response(response, MatchType.CONCEPT_ID, voglibose)

    response = ncit.search("NCIt:c95221")
    compare_response(response, MatchType.CONCEPT_ID, voglibose)

    response = ncit.search("C95221")
    compare_response(response, MatchType.CONCEPT_ID, voglibose)

    response = ncit.search("ncit:C74021")
    compare_response(response, MatchType.CONCEPT_ID, apricoxib)

    response = ncit.search("NCIt:C74021")
    compare_response(response, MatchType.CONCEPT_ID, apricoxib)

    response = ncit.search("ncit:c74021")
    compare_response(response, MatchType.CONCEPT_ID, apricoxib)

    response = ncit.search("C74021")
    compare_response(response, MatchType.CONCEPT_ID, apricoxib)

    response = ncit.search("ncit:C1647")
    compare_response(response, MatchType.CONCEPT_ID, trastuzumab)

    response = ncit.search("NCIT:C1647")
    compare_response(response, MatchType.CONCEPT_ID, trastuzumab)

    response = ncit.search("NCIt:c1647")
    compare_response(response, MatchType.CONCEPT_ID, trastuzumab)

    response = ncit.search("C1647")
    compare_response(response, MatchType.CONCEPT_ID, trastuzumab)

    response = ncit.search("ncit:C49236")
    compare_response(response, MatchType.CONCEPT_ID, therapeutic_procedure)

    response = ncit.search("NCIT:C61796")
    compare_response(response, MatchType.CONCEPT_ID, ivermectin)


def test_label_match(ncit, compare_response, voglibose, apricoxib, trastuzumab):
    """Test that label query resolves to correct record."""
    response = ncit.search("voglibose")
    compare_response(response, MatchType.LABEL, voglibose)

    response = ncit.search("voglibose")
    compare_response(response, MatchType.LABEL, voglibose)

    response = ncit.search("trastuzumab")
    compare_response(response, MatchType.LABEL, trastuzumab)

    response = ncit.search("Trastuzumab")
    compare_response(response, MatchType.LABEL, trastuzumab)

    response = ncit.search("Apricoxib")
    compare_response(response, MatchType.LABEL, apricoxib)

    response = ncit.search("APRICOXIB")
    compare_response(response, MatchType.LABEL, apricoxib)


def test_alias_match(ncit, compare_response, voglibose, apricoxib):
    """Test that alias query resolves to correct record."""
    response = ncit.search("BASEN")
    compare_response(response, MatchType.ALIAS, voglibose)

    response = ncit.search("AO-128")
    compare_response(response, MatchType.ALIAS, voglibose)

    response = ncit.search("COX-2 Inhibitor TG01")
    compare_response(response, MatchType.ALIAS, apricoxib)


def test_xref_match(ncit, compare_response, voglibose, apricoxib, trastuzumab):
    """Test that xref query resolves to correct record."""
    response = ncit.search("chemidplus:83480-29-9")
    compare_response(response, MatchType.XREF, voglibose)

    response = ncit.search("CHEMIDPLUS:197904-84-0")
    compare_response(response, MatchType.XREF, apricoxib)

    response = ncit.search("chemidplus:180288-69-1")
    compare_response(response, MatchType.XREF, trastuzumab)


def test_assoc_with_match(ncit, compare_response, voglibose, apricoxib, trastuzumab):
    """Test that associated_with query resolves to correct record."""
    response = ncit.search("unii:S77P977AG8")
    compare_response(response, MatchType.ASSOCIATED_WITH, voglibose)

    response = ncit.search("unii:5X5HB3VZ3Z")
    compare_response(response, MatchType.ASSOCIATED_WITH, apricoxib)

    response = ncit.search("unii:P188ANX8CK")
    compare_response(response, MatchType.ASSOCIATED_WITH, trastuzumab)


def test_no_match(ncit):
    """Test that a term normalizes to NO match."""
    response = ncit.search("voglibo")
    assert response.match_type == MatchType.NO_MATCH
    assert len(response.records) == 0

    # Test white space in between label
    response = ncit.search("Volgibo")
    assert response.match_type == MatchType.NO_MATCH

    # Test empty query
    response = ncit.search("")
    assert response.match_type == MatchType.NO_MATCH
    assert len(response.records) == 0


def test_meta_info(ncit):
    """Test that the meta field is correct."""
    response = ncit.search("NCIt")
    assert response.source_meta_.data_license == "CC BY 4.0"
    assert (
        response.source_meta_.data_license_url
        == "https://creativecommons.org/licenses/by/4.0/legalcode"
    )
    assert re.match(r"[0-9][0-9]\.[0-9][0-9][a-z]", response.source_meta_.version)
    assert response.source_meta_.data_url.startswith(
        "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/"
    )
    assert response.source_meta_.rdp_url == "http://reusabledata.org/ncit.html"
    assert response.source_meta_.data_license_attributes == {
        "non_commercial": False,
        "share_alike": False,
        "attribution": True,
    }
