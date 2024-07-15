"""Test ChemIDplus ETL methods."""

import isodate
import pytest

from therapy.etl.chemidplus import ChemIDplus
from therapy.schemas import MatchType, Therapy


@pytest.fixture(scope="module")
def chemidplus(test_source):
    """Provide test ChemIDplus query endpoint"""
    return test_source(ChemIDplus)


@pytest.fixture(scope="module")
def penicillin_v():
    """Build test fixture for Penicillin V drug."""
    return Therapy(
        concept_id="chemidplus:87-08-1",
        label="Penicillin V",
        aliases=["Phenoxymethylpenicillin"],
        trade_names=[],
        xrefs=[
            "drugbank:DB00417",
        ],
        associated_with=[
            "unii:Z61I075U2W",
        ],
    )


@pytest.fixture(scope="module")
def imatinib():
    """Build test fixture for Imatinib."""
    return Therapy(
        concept_id="chemidplus:152459-95-5",
        label="Imatinib",
        aliases=[],
        trade_names=[],
        xrefs=[
            "drugbank:DB00619",
        ],
        associated_with=[
            "unii:BKJ8M8G5HI",
        ],
    )


@pytest.fixture(scope="module")
def other_imatinib():
    """Build test fixture for imatinib mesylate."""
    return Therapy(
        concept_id="chemidplus:220127-57-1",
        label="Imatinib mesylate",
        aliases=[],
        xrefs=["drugbank:DB00619"],
        associated_with=["unii:8A1O1M485B"],
        trade_names=[],
    )


@pytest.fixture(scope="module")
def cisplatin():
    """Build test fixture for cisplatin."""
    return Therapy(
        concept_id="chemidplus:15663-27-1",
        label="Cisplatin",
        aliases=[
            "cis-Diaminedichloroplatinum",
            "1,2-Diaminocyclohexaneplatinum II citrate",
        ],
        trade_names=[],
        xrefs=["drugbank:DB00515"],
        associated_with=["unii:Q20Q21Q62J"],
    )


@pytest.fixture(scope="module")
def glycopyrronium_bromide():
    """Provide fixture for chemidplus:51186-83-5"""
    return Therapy(
        concept_id="chemidplus:51186-83-5",
        label="Glycopyrronium bromide",
        xrefs=["drugbank:DB00986"],
        associated_with=["unii:V92SO9WP2I"],
    )


def test_penicillin(chemidplus, compare_records, penicillin_v):
    """Test record retrieval for Penicillin V."""
    response = chemidplus.search("chemidplus:87-08-1")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], penicillin_v)

    response = chemidplus.search("CHemidplus:87-08-1")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], penicillin_v)

    response = chemidplus.search("Penicillin V")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], penicillin_v)

    response = chemidplus.search("Phenoxymethylpenicillin")
    assert response.match_type == MatchType.ALIAS
    assert len(response.records) == 1
    compare_records(response.records[0], penicillin_v)

    response = chemidplus.search("drugbank:DB00417")
    assert response.match_type == MatchType.XREF
    assert len(response.records) == 1
    compare_records(response.records[0], penicillin_v)

    response = chemidplus.search("unii:Z61I075U2W")
    assert response.match_type == MatchType.ASSOCIATED_WITH
    assert len(response.records) == 1
    compare_records(response.records[0], penicillin_v)

    response = chemidplus.search("87081")
    assert response.match_type == MatchType.NO_MATCH

    response = chemidplus.search("PenicillinV")
    assert response.match_type == MatchType.NO_MATCH


def test_imatinib(chemidplus, compare_records, imatinib, other_imatinib):
    """Test record retrieval for imatinib."""
    response = chemidplus.search("Imatinib")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], imatinib)

    response = chemidplus.search("imatiniB")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], imatinib)

    response = chemidplus.search("drugbank:DB00619")
    assert response.match_type == MatchType.XREF
    assert len(response.records) == 2
    for record in response.records:
        if record.concept_id == "chemidplus:152459-95-5":
            compare_records(record, imatinib)
        elif record.concept_id == "chemidplus:220127-57-1":
            compare_records(record, other_imatinib)
        else:
            pytest.fail("retrieved incorrect record")

    response = chemidplus.search("unii:BKJ8M8G5HI")
    assert response.match_type == MatchType.ASSOCIATED_WITH
    assert len(response.records) == 1
    compare_records(response.records[0], imatinib)


def test_cisplatin(chemidplus, compare_records, cisplatin):
    """Test record retrieval for cisplatin."""
    response = chemidplus.search("chemidplus:15663-27-1")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)

    response = chemidplus.search("Cisplatin")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)

    response = chemidplus.search("cis-Diaminedichloroplatinum")
    assert response.match_type == MatchType.ALIAS
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)

    response = chemidplus.search("drugbank:DB00515")
    assert response.match_type == MatchType.XREF
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)

    response = chemidplus.search("Cisplatine")
    assert response.match_type == MatchType.NO_MATCH


def test_glycopyrronium_bromide(chemidplus, compare_records, glycopyrronium_bromide):
    """Check input QC. This drug was processed with incorrect xref formatting."""
    response = chemidplus.search("chemidplus:51186-83-5")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], glycopyrronium_bromide)


def test_meta(chemidplus):
    """Test correctness of source metadata."""
    response = chemidplus.search("incoherent-string-of-text")
    assert response.source_meta_.data_license == "custom"
    assert (
        response.source_meta_.data_license_url
        == "https://www.nlm.nih.gov/databases/download/terms_and_conditions.html"
    )
    assert isodate.parse_date(response.source_meta_.version)
    assert (
        response.source_meta_.data_url == "ftp://ftp.nlm.nih.gov/nlmdata/.chemidlease/"
    )
    assert response.source_meta_.rdp_url is None
    assert response.source_meta_.data_license_attributes == {
        "non_commercial": False,
        "share_alike": False,
        "attribution": True,
    }
