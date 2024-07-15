"""Test that the therapy normalizer works as intended for the
Wikidata source.
"""

import isodate
import pytest

from therapy.etl.wikidata import Wikidata
from therapy.schemas import MatchType, Therapy


@pytest.fixture(scope="module")
def wikidata(test_source):
    """Provide test Wikidata query endpoint"""
    return test_source(Wikidata)


@pytest.fixture(scope="module")
def cisplatin():
    """Create a cisplatin drug fixture."""
    params = {
        "label": "cisplatin",
        "concept_id": "wikidata:Q412415",
        "aliases": [
            "CDDP",
            "Cis-DDP",
            "CIS-DDP",
            "cis-diamminedichloroplatinum(II)",
            "Platinol",
            "Platinol-AQ",
        ],
        "xrefs": [
            "chembl:CHEMBL11359",
            "drugbank:DB00515",
            "rxcui:2555",
            "chemidplus:15663-27-1",
            "iuphar.ligand:5343",
        ],
        "associated_with": ["pubchem.compound:5702198"],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def interferon_alfacon_1():
    """Create an Interferon alfacon-1 drug fixture."""
    params = {
        "label": "interferon alfacon-1",
        "concept_id": "wikidata:Q15353101",
        "aliases": [
            "CIFN",
            "consensus interferon",
            "IFN Alfacon-1",
            "Interferon Consensus, Methionyl",
            "methionyl interferon consensus",
            "methionyl-interferon-consensus",
            "rCon-IFN",
            "Recombinant Consensus Interferon",
            "Recombinant methionyl human consensus interferon",
        ],
        "xrefs": [
            "chembl:CHEMBL1201557",
            "drugbank:DB00069",
            "rxcui:59744",
            "chemidplus:118390-30-0",
        ],
    }
    return Therapy(**params)


# Test drug that has > 20 aliases
@pytest.fixture(scope="module")
def d_methamphetamine():
    """Create a D-methamphetamine drug fixture."""
    params = {
        "label": "D-methamphetamine",
        "concept_id": "wikidata:Q191924",
        "aliases": [],
        "approval_ratings": [],
        "xrefs": [
            "chembl:CHEMBL1201201",
            "drugbank:DB01577",
            "rxcui:6816",
            "chemidplus:537-46-2",
        ],
        "associated_with": [
            "pubchem.compound:10836",
        ],
        "trade_names": [],
    }
    return Therapy(**params)


# Test removing duplicate (case sensitive) aliases
@pytest.fixture(scope="module")
def atropine():
    """Create an atropine drug fixture."""
    params = {
        "label": "atropine",
        "concept_id": "wikidata:Q26272",
        "aliases": [
            "dl-Hyoscyamine",
            "dl-tropyltropate",
            "Mydriasine",
            "Tropine tropate",
            "(±)-atropine",
            "(±)-hyoscyamine",
            "(+-)-Atropine",
            "(+-)-Hyoscyamine",
            "(+,-)-Tropyl tropate",
            "(3-Endo)-8-methyl-8-azabicyclo[3.2.1]oct-3-yl tropate",
            "[(1S,5R)-8-Methyl-8-azabicyclo[3.2.1]oct-3-yl] "
            "3-hydroxy-2-phenyl-propanoate",
            "8-Methyl-8-azabicyclo[3.2.1]oct-3-yl " "3-hydroxy-2-phenylpropanoate",
            "8-Methyl-8-azabicyclo[3.2.1]oct-3-yl tropate",
        ],
        "approval_ratings": [],
        "xrefs": [
            "drugbank:DB00572",
            "chemidplus:51-55-8",
            "rxcui:1223",
            "chembl:CHEMBL517712",
            "iuphar.ligand:320",
        ],
        "associated_with": [
            "pubchem.compound:174174",
        ],
        "trade_names": [],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def basiliximab():
    """Create basiliximab test fixture to demonstrate rule-based filtering."""
    return Therapy(
        label="basiliximab",
        concept_id="wikidata:Q418702",
        aliases=["SDZ-CHI-621", "CHI-621", "CHI621", "chimeric mouse-human antiCD25"],
        xrefs=[
            "drugbank:DB00074",
            "rxcui:196102",
            "chembl:CHEMBL1201439",
            "chemidplus:179045-86-4",
            "iuphar.ligand:6879",
        ],
    )


def test_cisplatin(cisplatin, wikidata, compare_records):
    """Test that cisplatin drug normalizes to correct drug concept."""
    response = wikidata.search("wikidata:Q412415")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)

    response = wikidata.search("wiKIdata:Q412415")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)

    response = wikidata.search("wiKIdata:q412415")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)

    response = wikidata.search("cisplatin")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)

    response = wikidata.search("Cisplatin")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)

    response = wikidata.search("CDDP")
    assert response.match_type == MatchType.ALIAS
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)

    response = wikidata.search("cddp")
    assert response.match_type == MatchType.ALIAS
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)

    response = wikidata.search("chembl:CHEMBL11359")
    assert response.match_type == MatchType.XREF
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)

    response = wikidata.search("drugbank:DB00515")
    assert response.match_type == MatchType.XREF
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)

    response = wikidata.search("rxcui:2555")
    assert response.match_type == MatchType.XREF
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)

    response = wikidata.search("chemidplus:15663-27-1")
    assert response.match_type == MatchType.XREF
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)

    response = wikidata.search("pubchem.compound:5702198")
    assert response.match_type == MatchType.XREF
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)


def test_atropine(atropine, wikidata, compare_records):
    """Test that atropine drug normalizes to correct drug concept."""
    response = wikidata.search("wikidata:Q26272")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], atropine)

    response = wikidata.search("wiKIdata:Q26272")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], atropine)

    response = wikidata.search("wiKIdata:q26272")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], atropine)

    response = wikidata.search("atropine")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], atropine)

    response = wikidata.search("Atropine")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], atropine)

    response = wikidata.search("Mydriasine")
    assert response.match_type == MatchType.ALIAS
    assert len(response.records) == 1
    compare_records(response.records[0], atropine)

    response = wikidata.search("(±)-Hyoscyamine")
    assert response.match_type == MatchType.ALIAS
    assert len(response.records) == 1
    compare_records(response.records[0], atropine)

    response = wikidata.search("chemidplus:51-55-8")
    assert response.match_type == MatchType.XREF
    assert len(response.records) == 1
    compare_records(response.records[0], atropine)

    response = wikidata.search("pubchem.compound:174174")
    assert response.match_type == MatchType.ASSOCIATED_WITH
    assert len(response.records) == 1
    compare_records(response.records[0], atropine)


def test_interferon_alfacon_1(interferon_alfacon_1, wikidata, compare_records):
    """Test that interferon alfacon-1 drug normalizes to correct concept"""
    response = wikidata.search("wikidata:Q15353101")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], interferon_alfacon_1)

    response = wikidata.search("wiKIdata:Q15353101")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], interferon_alfacon_1)

    response = wikidata.search("wiKIdata:q15353101")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], interferon_alfacon_1)

    response = wikidata.search("Interferon alfacon-1")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], interferon_alfacon_1)

    response = wikidata.search("Interferon Alfacon-1")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], interferon_alfacon_1)


def test_d_methamphetamine(d_methamphetamine, wikidata, compare_records):
    """Test that d_methamphetamine drug normalizes to correct concept."""
    response = wikidata.search("wikidata:Q191924")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], d_methamphetamine)

    response = wikidata.search("wiKIdata:Q191924")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], d_methamphetamine)

    response = wikidata.search("wiKIdata:q191924")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], d_methamphetamine)

    response = wikidata.search("D-methamphetamine")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], d_methamphetamine)

    response = wikidata.search("d-methamphetamine")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], d_methamphetamine)

    response = wikidata.search("rxcui:6816")
    assert response.match_type == MatchType.XREF
    assert len(response.records) == 1
    compare_records(response.records[0], d_methamphetamine)


def test_basiliximab(basiliximab, wikidata, compare_records):
    """Test that basiliximab terms resolve correctly"""
    response = wikidata.search("wikidata:Q418702")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], basiliximab)

    response = wikidata.search("basiliximab")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], basiliximab)

    response = wikidata.search("CHI621")
    assert response.match_type == MatchType.ALIAS
    assert len(response.records) == 1
    compare_records(response.records[0], basiliximab)

    response = wikidata.search("Ig gamma-1 chain C region")
    assert response.match_type == MatchType.NO_MATCH


def test_case_no_match(wikidata):
    """Test that a term normalizes to NO match."""
    response = wikidata.search("cisplati")
    assert response.match_type == MatchType.NO_MATCH
    assert len(response.records) == 0

    # Test white space in between label
    response = wikidata.search("Interferon alfacon - 1")
    assert response.match_type == MatchType.NO_MATCH

    # Test empty query
    response = wikidata.search("")
    assert response.match_type == MatchType.NO_MATCH
    assert len(response.records) == 0


def test_meta_info(wikidata):
    """Test that the meta field is correct."""
    response = wikidata.search("cisplatin")
    assert response.source_meta_.data_license == "CC0 1.0"
    assert (
        response.source_meta_.data_license_url
        == "https://creativecommons.org/publicdomain/zero/1.0/"
    )
    assert isodate.parse_date(response.source_meta_.version)
    assert response.source_meta_.data_url is None
    assert not response.source_meta_.rdp_url
    assert response.source_meta_.data_license_attributes == {
        "non_commercial": False,
        "share_alike": False,
        "attribution": False,
    }
