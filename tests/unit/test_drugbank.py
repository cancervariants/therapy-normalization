"""Test that the therapy normalizer works as intended for the DrugBank
source.
"""

import re

import pytest

from therapy.etl.drugbank import DrugBank
from therapy.schemas import MatchType, Therapy


@pytest.fixture(scope="module")
def drugbank(test_source):
    """Provide test DrugBank query endpoint"""
    return test_source(DrugBank)


@pytest.fixture(scope="module")
def cisplatin():
    """Create a cisplatin drug fixture."""
    params = {
        "label": "Cisplatin",
        "concept_id": "drugbank:DB00515",
        "aliases": [
            "CDDP",
            "Cis-DDP",
            "cis-diamminedichloroplatinum(II)",
            "APRD00359",
            "cisplatino",
        ],
        "xrefs": [
            "chemidplus:15663-27-1",
        ],
        "trade_names": [],
        "associated_with": ["unii:Q20Q21Q62J", "inchikey:LXZZYRPGZAFOLE-UHFFFAOYSA-L"],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def bentiromide():
    """Create a bentiromide drug fixture."""
    params = {
        "label": "Bentiromide",
        "concept_id": "drugbank:DB00522",
        "aliases": [
            "APRD00818",
            "Bentiromido",
            "Bentiromidum",
            "BTPABA",
            "PFT",
        ],
        "xrefs": [
            "chemidplus:37106-97-1",
        ],
        "trade_names": [],
        "associated_with": ["unii:239IF5W61J", "inchikey:SPPTWHFVYKCNNK-FQEVSTJZSA-N"],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def aloe_ferox_leaf():
    """Create aloe ferox leaf fixture. Record has >20 aliases, so ETL deletes
    all of them.
    """
    params = {
        "concept_id": "drugbank:DB14257",
        "label": "Aloe ferox leaf",
        "aliases": [],
        "xrefs": [],
        "trade_names": [],
        "associated_with": ["unii:0D145J8EME"],
    }
    return Therapy(**params)


def test_concept_id_match(
    drugbank, compare_response, cisplatin, bentiromide, aloe_ferox_leaf
):
    """Test that concept ID query resolves to correct record."""
    response = drugbank.search("drugbank:DB00515")
    compare_response(response, MatchType.CONCEPT_ID, cisplatin)

    response = drugbank.search("DB00515")
    compare_response(response, MatchType.CONCEPT_ID, cisplatin)

    response = drugbank.search("drugbank:db00515")
    compare_response(response, MatchType.CONCEPT_ID, cisplatin)

    response = drugbank.search("Drugbank:db00515")
    compare_response(response, MatchType.CONCEPT_ID, cisplatin)

    response = drugbank.search("druGBank:DB00515")
    compare_response(response, MatchType.CONCEPT_ID, cisplatin)

    response = drugbank.search("drugbank:DB00522")
    compare_response(response, MatchType.CONCEPT_ID, bentiromide)

    response = drugbank.search("DB00522")
    compare_response(response, MatchType.CONCEPT_ID, bentiromide)

    response = drugbank.search("drugbank:db00522")
    compare_response(response, MatchType.CONCEPT_ID, bentiromide)

    response = drugbank.search("Drugbank:db00522")
    compare_response(response, MatchType.CONCEPT_ID, bentiromide)

    response = drugbank.search("druGBank:DB00522")
    compare_response(response, MatchType.CONCEPT_ID, bentiromide)

    response = drugbank.search("drugbank:DB14257")
    compare_response(response, MatchType.CONCEPT_ID, aloe_ferox_leaf)


def test_label_match(
    drugbank, compare_response, cisplatin, bentiromide, aloe_ferox_leaf
):
    """Test that label query resolves to correct record."""
    response = drugbank.search("cisplatin")
    compare_response(response, MatchType.LABEL, cisplatin)

    response = drugbank.search("cisplatin")
    compare_response(response, MatchType.LABEL, cisplatin)

    response = drugbank.search("Bentiromide")
    compare_response(response, MatchType.LABEL, bentiromide)

    response = drugbank.search("bentiromide")
    compare_response(response, MatchType.LABEL, bentiromide)

    response = drugbank.search("aloe ferox leaf")
    compare_response(response, MatchType.LABEL, aloe_ferox_leaf)


def test_alias_match(drugbank, compare_response, cisplatin, bentiromide):
    """Test that alias query resolves to correct record."""
    response = drugbank.search("CISPLATINO")
    compare_response(response, MatchType.ALIAS, cisplatin)

    response = drugbank.search("Cis-ddp")
    compare_response(response, MatchType.ALIAS, cisplatin)

    response = drugbank.search("APRD00818")
    compare_response(response, MatchType.ALIAS, bentiromide)

    response = drugbank.search("PFT")
    compare_response(response, MatchType.ALIAS, bentiromide)

    # verify aliases > 20 not stored
    response = drugbank.search("Aloe Capensis")
    assert response.match_type == MatchType.NO_MATCH

    response = drugbank.search("Aloe Ferox Juice")
    assert response.match_type == MatchType.NO_MATCH


def test_xref_match(drugbank, compare_response, cisplatin, bentiromide):
    """Test that xref query resolves to correct record."""
    response = drugbank.search("chemidplus:15663-27-1")
    compare_response(response, MatchType.XREF, cisplatin)

    response = drugbank.search("chemidplus:37106-97-1")
    compare_response(response, MatchType.XREF, bentiromide)


def test_assoc_with_match(
    drugbank, compare_response, cisplatin, bentiromide, aloe_ferox_leaf
):
    """Test that associated_with query resolves to correct record."""
    response = drugbank.search("inchikey:lxzzyrpgzafole-uhfffaoysa-l")
    compare_response(response, MatchType.ASSOCIATED_WITH, cisplatin)

    response = drugbank.search("unii:239if5w61j")
    compare_response(response, MatchType.ASSOCIATED_WITH, bentiromide)

    response = drugbank.search("UNII:0D145J8EME")
    compare_response(response, MatchType.ASSOCIATED_WITH, aloe_ferox_leaf)


def test_no_match(drugbank):
    """Test that a term normalizes to correct drug concept as a NO match."""
    response = drugbank.search("lepirudi")
    assert response.match_type == MatchType.NO_MATCH
    assert len(response.records) == 0

    # Test white space in between id
    response = drugbank.search("DB 00001")
    assert response.match_type == MatchType.NO_MATCH

    # Test empty query
    response = drugbank.search("")
    assert response.match_type == MatchType.NO_MATCH
    assert len(response.records) == 0


def test_meta_info(drugbank):
    """Test that the meta field is correct."""
    response = drugbank.search("search")
    assert response.source_meta_.data_license == "CC0 1.0"
    assert (
        response.source_meta_.data_license_url
        == "https://creativecommons.org/publicdomain/zero/1.0/"
    )
    assert re.match(r"[0-9]+\.[0-9]+\.[0-9]", response.source_meta_.version)
    assert (
        response.source_meta_.data_url
        == "https://go.drugbank.com/releases/latest#open-data"
    )
    assert response.source_meta_.rdp_url == "http://reusabledata.org/drugbank.html"
    assert response.source_meta_.data_license_attributes == {
        "non_commercial": False,
        "share_alike": False,
        "attribution": False,
    }
