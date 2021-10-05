"""Test Guide to PHARMACOLOGY source."""
import pytest
from tests.conftest import compare_response
from therapy.query import QueryHandler
from therapy.schemas import Drug, MatchType
import re


@pytest.fixture(scope="module")
def guidetopharmacology():
    """Build Guide To PHARMACOLOGY test fixture."""
    class QueryGetter:

        def __init__(self):
            self.query_handler = QueryHandler()

        def search(self, query_str):
            resp = self.query_handler.search_sources(
                query_str, keyed=True, incl="guidetopharmacology")
            return resp["source_matches"]["GuideToPHARMACOLOGY"]

        def fetch_meta(self):
            return self.query_handler._fetch_meta("GuideToPHARMACOLOGY")
    return QueryGetter()


@pytest.fixture(scope="module")
def cisplatin():
    """Create a cisplatin test fixture."""
    params = {
        "concept_id": "iuphar.ligand:5343",
        "label": "cisplatin",
        "approval_status": "approved",
        "xrefs": [
            "chembl:CHEMBL11359",
            "chemidplus:15663-27-1",
            "drugbank:DB00515"
        ],
        "associated_with": [
            "pubchem.substance:178102005",
            "pubchem.compound:441203",
            "CHEBI:27899"
        ],
        "aliases": [
            "Platinol"
        ]
    }
    return Drug(**params)


@pytest.fixture(scope="module")
def arginine_vasotocin():
    """Create an arginine vasotocin test fixture"""
    params = {
        "concept_id": "iuphar.ligand:2169",
        "label": "arginine vasotocin",
        "xrefs": [
            "chemidplus:113-80-4"
        ],
        "associated_with": [
            "pubchem.substance:135652004",
            "pubchem.compound:68649",
            "inchikey:OXDZADMCOWPSOC-ICBIOJHSSA-N"
        ],
        "aliases": [
            "L-cysteinyl-L-tyrosyl-(3S)-DL-isoleucyl-L-glutaminyl-L-asparagyl-L-cysteinyl-DL-prolyl-L-arginyl-glycinamide (1->6)-disulfide",  # noqa: E501
            "argiprestocin",
            "[Arg<sup>8</sup>]vasotocin",
            "AVT"
        ]
    }
    return Drug(**params)


@pytest.fixture(scope="module")
def phenobarbital():
    """Create Phenobarbital test fixture"""
    params = {
        "concept_id": "iuphar.ligand:2804",
        "label": "phenobarbital",
        "approval_status": "approved",
        "xrefs": [
            "chembl:CHEMBL40",
            "chemidplus:50-06-6",
            "drugbank:DB01174"
        ],
        "associated_with": [
            "pubchem.substance:135650817",
            "pubchem.compound:4763",
            "CHEBI:8069",
            "inchikey:DDBREPKUVSBGFI-UHFFFAOYSA-N",
            "drugcentral:2134"
        ],
        "aliases": [
            "5-ethyl-5-phenyl-1,3-diazinane-2,4,6-trione",
            "fenobarbital",
            "Luminal",
            "phenobarb",
            "phenobarbital sodium",
            "phenobarbitone",
            "phenylethylbarbiturate"
        ]
    }
    return Drug(**params)


@pytest.fixture(scope="module")
def cisapride():
    """Create cisapride test fixture"""
    params = {
        "concept_id": "iuphar.ligand:240",
        "label": "cisapride",
        "approval_status": "withdrawn",
        "xrefs": [
            "chembl:CHEMBL1729",
            "chemidplus:81098-60-4",
            "drugbank:DB00604"
        ],
        "associated_with": [
            "pubchem.substance:135650104",
            "pubchem.compound:2769",
            "CHEBI:151790",
            "inchikey:DCSUBABJRXZOMT-UHFFFAOYSA-N",
            "drugcentral:660"
        ],
        "aliases": [
            "4-amino-5-chloro-N-[1-[3-(4-fluorophenoxy)propyl]-3-methoxypiperidin-4-yl]-2-methoxybenzamide",  # noqa: E501
            "Prepulsid",
            "Propulsid"
        ]
    }
    return Drug(**params)


def test_concept_id_match(guidetopharmacology, cisplatin, arginine_vasotocin,
                          phenobarbital, cisapride):
    """Test that concept ID queries work correctly."""
    resp = guidetopharmacology.search("iuphar.ligand:5343")
    compare_response(resp, MatchType.CONCEPT_ID, cisplatin)

    resp = guidetopharmacology.search("iuphar.ligand:2169")
    compare_response(resp, MatchType.CONCEPT_ID, arginine_vasotocin)

    resp = guidetopharmacology.search("iuphar.ligand:2804")
    compare_response(resp, MatchType.CONCEPT_ID, phenobarbital)

    resp = guidetopharmacology.search("iuphar.ligand:240")
    compare_response(resp, MatchType.CONCEPT_ID, cisapride)


def test_label_match(guidetopharmacology, cisplatin, arginine_vasotocin,
                     phenobarbital, cisapride):
    """Test that label queries work correctly."""
    resp = guidetopharmacology.search("cisplatin")
    compare_response(resp, MatchType.LABEL, cisplatin)

    resp = guidetopharmacology.search("arginine vasotocin")
    compare_response(resp, MatchType.LABEL, arginine_vasotocin)

    resp = guidetopharmacology.search("Phenobarbital")
    compare_response(resp, MatchType.LABEL, phenobarbital)

    resp = guidetopharmacology.search("cisapride")
    compare_response(resp, MatchType.LABEL, cisapride)


def test_alias_match(guidetopharmacology, cisplatin, arginine_vasotocin,
                     phenobarbital, cisapride):
    """Test that alias queries work correctly."""
    resp = guidetopharmacology.search("platinol")
    compare_response(resp, MatchType.ALIAS, cisplatin)

    resp = guidetopharmacology.search("AVT")
    compare_response(resp, MatchType.ALIAS, arginine_vasotocin)

    resp = guidetopharmacology.search("5-ethyl-5-phenyl-1,3-diazinane-2,4,6-trione")  # noqa: E501
    compare_response(resp, MatchType.ALIAS, phenobarbital)

    resp = guidetopharmacology.search("Prepulsid")
    compare_response(resp, MatchType.ALIAS, cisapride)


def test_xref_match(guidetopharmacology, cisplatin, arginine_vasotocin,
                    phenobarbital, cisapride):
    """Test that xref queries work correctly."""
    resp = guidetopharmacology.search("chemidplus:15663-27-1")
    compare_response(resp, MatchType.XREF, cisplatin)

    resp = guidetopharmacology.search("chemidplus:113-80-4")
    compare_response(resp, MatchType.XREF, arginine_vasotocin)

    resp = guidetopharmacology.search("chembl:CHEMBL40")
    compare_response(resp, MatchType.XREF, phenobarbital)

    resp = guidetopharmacology.search("drugbank:DB00604")
    compare_response(resp, MatchType.XREF, cisapride)


def test_associated_with_match(guidetopharmacology, cisplatin,
                               arginine_vasotocin, phenobarbital, cisapride):
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


def test_no_match(guidetopharmacology):
    """Test that no match queries work correctly."""
    resp = guidetopharmacology.search("178102005")
    assert resp["match_type"] == MatchType.NO_MATCH
    assert len(resp["records"]) == 0

    resp = guidetopharmacology.search("guidetopharmacology:5343")
    assert resp["match_type"] == MatchType.NO_MATCH
    assert len(resp["records"]) == 0

    resp = guidetopharmacology.search("")
    assert resp["match_type"] == MatchType.NO_MATCH
    assert len(resp["records"]) == 0


def test_meta_info(guidetopharmacology):
    """Test that metadata is correct."""
    resp = guidetopharmacology.fetch_meta()
    assert resp.data_license == "CC BY-SA 4.0"
    assert resp.data_license_url == "https://creativecommons.org/licenses/by-sa/4.0/"  # noqa: E501
    assert re.match(r"\d{4}.\d+", resp.version)
    assert resp.data_url == "https://www.guidetopharmacology.org/download.jsp"
    assert resp.rdp_url is None
    assert resp.data_license_attributes == {
        "non_commercial": False,
        "attribution": True,
        "share_alike": True
    }
