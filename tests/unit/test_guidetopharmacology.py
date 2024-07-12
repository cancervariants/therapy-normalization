"""Test Guide to PHARMACOLOGY source."""

import re

import pytest

from therapy.etl.guidetopharmacology import GuideToPHARMACOLOGY
from therapy.schemas import MatchType, Therapy


@pytest.fixture(scope="module")
def guidetopharmacology(test_source):
    """Provide test gtop query endpoint"""
    return test_source(GuideToPHARMACOLOGY)


@pytest.fixture(scope="module")
def cisplatin():
    """Create a cisplatin test fixture."""
    params = {
        "concept_id": "iuphar.ligand:5343",
        "label": "cisplatin",
        "approval_ratings": ["gtopdb_approved"],
        "xrefs": ["chembl:CHEMBL11359", "chemidplus:15663-27-1", "drugbank:DB00515"],
        "associated_with": [
            "pubchem.substance:178102005",
            "pubchem.compound:441203",
            "CHEBI:27899",
        ],
        "aliases": ["Platinol"],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def arginine_vasotocin():
    """Create an arginine vasotocin test fixture"""
    params = {
        "concept_id": "iuphar.ligand:2169",
        "label": "arginine vasotocin",
        "xrefs": ["chemidplus:113-80-4"],
        "associated_with": [
            "pubchem.substance:135652004",
            "pubchem.compound:68649",
            "inchikey:OXDZADMCOWPSOC-ICBIOJHSSA-N",
        ],
        "aliases": [
            "L-cysteinyl-L-tyrosyl-(3S)-DL-isoleucyl-L-glutaminyl-L-asparagyl-L-cysteinyl-DL-prolyl-L-arginyl-glycinamide (1->6)-disulfide",
            "argiprestocin",
            "[Arg8]vasotocin",
            "AVT",
        ],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def phenobarbital():
    """Create Phenobarbital test fixture"""
    params = {
        "concept_id": "iuphar.ligand:2804",
        "label": "phenobarbital",
        "approval_ratings": ["gtopdb_approved"],
        "xrefs": ["chembl:CHEMBL40", "chemidplus:50-06-6", "drugbank:DB01174"],
        "associated_with": [
            "pubchem.substance:135650817",
            "pubchem.compound:4763",
            "CHEBI:8069",
            "inchikey:DDBREPKUVSBGFI-UHFFFAOYSA-N",
            "drugcentral:2134",
        ],
        "aliases": [
            "5-ethyl-5-phenyl-1,3-diazinane-2,4,6-trione",
            "fenobarbital",
            "Luminal",
            "phenobarb",
            "phenobarbital sodium",
            "phenobarbitone",
            "phenylethylbarbiturate",
        ],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def cisapride():
    """Create cisapride test fixture"""
    params = {
        "concept_id": "iuphar.ligand:240",
        "label": "cisapride",
        "approval_ratings": ["gtopdb_withdrawn"],
        "xrefs": ["chembl:CHEMBL1729", "chemidplus:81098-60-4", "drugbank:DB00604"],
        "associated_with": [
            "pubchem.substance:135650104",
            "pubchem.compound:2769",
            "CHEBI:151790",
            "inchikey:DCSUBABJRXZOMT-UHFFFAOYSA-N",
            "drugcentral:660",
        ],
        "aliases": [
            "4-amino-5-chloro-N-[1-[3-(4-fluorophenoxy)propyl]-3-methoxypiperidin-4-yl]-2-methoxybenzamide",
            "Prepulsid",
            "Propulsid",
        ],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def rolipram():
    """Create rolipram test fixture.
    Checks for correct handling of multiple references under same namespace.
    """
    return Therapy(
        concept_id="iuphar.ligand:5260",
        label="rolipram",
        xrefs=[
            "chembl:CHEMBL63",
            "chemidplus:61413-54-5",
            "drugbank:DB04149",
            "drugbank:DB03606",
        ],
        aliases=[
            "4-[3-(cyclopentyloxy)-4-methoxyphenyl]pyrrolidin-2-one",
            "(R,S)-rolipram",
            "(±)-rolipram",
        ],
        associated_with=[
            "CHEBI:104872",
            "pubchem.substance:178101944",
            "pubchem.compound:5092",
            "inchikey:HJORMJIFDVBMOB-UHFFFAOYSA-N",
        ],
    )


def test_concept_id_match(
    guidetopharmacology,
    compare_response,
    cisplatin,
    arginine_vasotocin,
    phenobarbital,
    cisapride,
    rolipram,
):
    """Test that concept ID queries work correctly."""
    resp = guidetopharmacology.search("iuphar.ligand:5343")
    compare_response(resp, MatchType.CONCEPT_ID, cisplatin)

    resp = guidetopharmacology.search("iuphar.ligand:2169")
    compare_response(resp, MatchType.CONCEPT_ID, arginine_vasotocin)

    resp = guidetopharmacology.search("iuphar.ligand:2804")
    compare_response(resp, MatchType.CONCEPT_ID, phenobarbital)

    resp = guidetopharmacology.search("iuphar.ligand:240")
    compare_response(resp, MatchType.CONCEPT_ID, cisapride)

    resp = guidetopharmacology.search("iuphar.ligand:5260")
    compare_response(resp, MatchType.CONCEPT_ID, rolipram)


def test_label_match(
    guidetopharmacology,
    compare_response,
    cisplatin,
    arginine_vasotocin,
    phenobarbital,
    cisapride,
    rolipram,
):
    """Test that label queries work correctly."""
    resp = guidetopharmacology.search("cisplatin")
    compare_response(resp, MatchType.LABEL, cisplatin)

    resp = guidetopharmacology.search("arginine vasotocin")
    compare_response(resp, MatchType.LABEL, arginine_vasotocin)

    resp = guidetopharmacology.search("Phenobarbital")
    compare_response(resp, MatchType.LABEL, phenobarbital)

    resp = guidetopharmacology.search("cisapride")
    compare_response(resp, MatchType.LABEL, cisapride)

    resp = guidetopharmacology.search("rolipram")
    compare_response(resp, MatchType.LABEL, rolipram)


def test_alias_match(
    guidetopharmacology,
    compare_response,
    cisplatin,
    arginine_vasotocin,
    phenobarbital,
    cisapride,
    rolipram,
):
    """Test that alias queries work correctly."""
    resp = guidetopharmacology.search("platinol")
    compare_response(resp, MatchType.ALIAS, cisplatin)

    resp = guidetopharmacology.search("AVT")
    compare_response(resp, MatchType.ALIAS, arginine_vasotocin)

    resp = guidetopharmacology.search("5-ethyl-5-phenyl-1,3-diazinane-2,4,6-trione")
    compare_response(resp, MatchType.ALIAS, phenobarbital)

    resp = guidetopharmacology.search("Prepulsid")
    compare_response(resp, MatchType.ALIAS, cisapride)

    resp = guidetopharmacology.search("(R,S)-rolipram")
    compare_response(resp, MatchType.ALIAS, rolipram)


def test_xref_match(
    guidetopharmacology,
    compare_response,
    cisplatin,
    arginine_vasotocin,
    phenobarbital,
    cisapride,
    rolipram,
):
    """Test that xref queries work correctly."""
    resp = guidetopharmacology.search("chemidplus:15663-27-1")
    compare_response(resp, MatchType.XREF, cisplatin)

    resp = guidetopharmacology.search("chemidplus:113-80-4")
    compare_response(resp, MatchType.XREF, arginine_vasotocin)

    resp = guidetopharmacology.search("chembl:CHEMBL40")
    compare_response(resp, MatchType.XREF, phenobarbital)

    resp = guidetopharmacology.search("drugbank:DB00604")
    compare_response(resp, MatchType.XREF, cisapride)

    resp = guidetopharmacology.search("drugbank:DB03606")
    compare_response(resp, MatchType.XREF, rolipram)


def test_associated_with_match(
    guidetopharmacology,
    compare_response,
    cisplatin,
    arginine_vasotocin,
    phenobarbital,
    cisapride,
    rolipram,
):
    """Test that associated_with queries work correctly."""
    resp = guidetopharmacology.search("pubchem.substance:178102005")
    compare_response(resp, MatchType.ASSOCIATED_WITH, cisplatin)

    resp = guidetopharmacology.search("pubchem.compound:441203")
    compare_response(resp, MatchType.ASSOCIATED_WITH, cisplatin)

    resp = guidetopharmacology.search("pubchem.substance:135652004")
    compare_response(resp, MatchType.ASSOCIATED_WITH, arginine_vasotocin)

    resp = guidetopharmacology.search("pubchem.compound:68649")
    compare_response(resp, MatchType.ASSOCIATED_WITH, arginine_vasotocin)

    resp = guidetopharmacology.search("pubchem.substance:135650817")
    compare_response(resp, MatchType.ASSOCIATED_WITH, phenobarbital)

    resp = guidetopharmacology.search("pubchem.compound:4763")
    compare_response(resp, MatchType.ASSOCIATED_WITH, phenobarbital)

    resp = guidetopharmacology.search("drugcentral:2134")
    compare_response(resp, MatchType.ASSOCIATED_WITH, phenobarbital)

    resp = guidetopharmacology.search("pubchem.substance:135650104")
    compare_response(resp, MatchType.ASSOCIATED_WITH, cisapride)

    resp = guidetopharmacology.search("pubchem.compound:2769")
    compare_response(resp, MatchType.ASSOCIATED_WITH, cisapride)

    resp = guidetopharmacology.search("inchikey:DCSUBABJRXZOMT-UHFFFAOYSA-N")
    compare_response(resp, MatchType.ASSOCIATED_WITH, cisapride)

    resp = guidetopharmacology.search("pubchem.substance:178101944")
    compare_response(resp, MatchType.ASSOCIATED_WITH, rolipram)


def test_no_match(guidetopharmacology):
    """Test that no match queries work correctly."""
    resp = guidetopharmacology.search("178102005")
    assert resp.match_type == MatchType.NO_MATCH
    assert len(resp.records) == 0

    resp = guidetopharmacology.search("guidetopharmacology:5343")
    assert resp.match_type == MatchType.NO_MATCH
    assert len(resp.records) == 0

    resp = guidetopharmacology.search("")
    assert resp.match_type == MatchType.NO_MATCH
    assert len(resp.records) == 0


def test_meta_info(guidetopharmacology):
    """Test that metadata is correct."""
    resp = guidetopharmacology.search("search")
    assert resp.source_meta_.data_license == "CC BY-SA 4.0"
    assert (
        resp.source_meta_.data_license_url
        == "https://creativecommons.org/licenses/by-sa/4.0/"
    )
    assert re.match(r"\d{4}.\d+", resp.source_meta_.version)
    assert (
        resp.source_meta_.data_url == "https://www.guidetopharmacology.org/download.jsp"
    )
    assert resp.source_meta_.rdp_url is None
    assert resp.source_meta_.data_license_attributes == {
        "non_commercial": False,
        "attribution": True,
        "share_alike": True,
    }
