"""Test ChemIDplus ETL methods."""
from therapy.query import Normalizer
from therapy.schemas import Drug, MatchType
import pytest
from typing import Dict


@pytest.fixture(scope='module')
def chemidplus():
    """Build ETL test fixture."""
    class QueryGetter:

        def __init__(self):
            self.normalizer = Normalizer()

        def normalize(self, query_str):
            resp = self.normalizer.normalize(query_str, keyed=True,
                                             incl='chemidplus')
            return resp['source_matches']['ChemIDplus']
    return QueryGetter()


@pytest.fixture(scope='module')
def penicillin_v():
    """Build test fixture for Penicillin V drug.

    ChemIDplus stores a long list of aliases, but the DB record should
    truncate them to 0.
    """
    return Drug(**{
        "concept_id": "chemidplus:87-08-1",
        "label": "Penicillin V",
        "aliases": [],
        "trade_names": [],
        "other_identifiers": [
            "drugbank:DB00417",
        ],
        "xrefs": [
            "fda:Z61I075U2W",
        ]
    })


@pytest.fixture(scope='module')
def imatinib():
    """Build test fixture for Imatinib."""
    return Drug(**{
        "concept_id": "chemidplus:152459-95-5",
        "label": "Imatinib",
        "aliases": [
            "GGP 57148", "CCRIS 9076", "UNII-BKJ8M8G5HI", "ST1571", "ST 1571",
            "CGP-57148"
        ],
        "trade_names": [],
        "other_identifiers": [
            "drugbank:DB00619",
        ],
        "xrefs": [
            "fda:BKJ8M8G5HI",
        ]
    })


def compare_records(actual_record: Dict, fixture_record: Drug):
    """Check that records are identical."""
    assert actual_record.concept_id == fixture_record.concept_id
    assert actual_record.label == fixture_record.label
    assert set(actual_record.trade_names) == set(fixture_record.trade_names)
    assert set(actual_record.other_identifiers) == \
        set(fixture_record.other_identifiers)
    assert set(actual_record.xrefs) == set(fixture_record.xrefs)
    assert actual_record.approval_status == fixture_record.approval_status


def test_concept_id_match(chemidplus, penicillin_v):
    """Test that records are retrieved by concept ID correctly."""
    response = chemidplus.normalize('chemidplus:87-08-1')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], penicillin_v)

    response = chemidplus.normalize('CHemidplus:87-08-1')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], penicillin_v)

    response = chemidplus.normalize('87-08-1')
    assert response['match_type'] == MatchType.NO_MATCH

    response = chemidplus.normalize('87081')
    assert response['match_type'] == MatchType.NO_MATCH


def test_label_match(chemidplus, imatinib, penicillin_v):
    """Test that records are retrieved by label correctly."""
    response = chemidplus.normalize('Penicillin V')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], penicillin_v)

    response = chemidplus.normalize('Imatinib')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], imatinib)

    response = chemidplus.normalize('imatiniB')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], imatinib)

    response = chemidplus.normalize('PenicillinV')
    assert response['match_type'] == MatchType.NO_MATCH


def test_alias_match(chemidplus, imatinib):
    """Test that records are retrieved by alias correctly."""
    response = chemidplus.normalize('CCRIS 9076')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], imatinib)

    response = chemidplus.normalize('CGP-57148')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], imatinib)


def test_meta(chemidplus):
    """Test correctness of source metadata."""
    response = chemidplus.normalize('incoherent-string-of-text')
    assert response['meta_'].data_license == 'custom'
    assert response['meta_'].data_license_url == 'https://www.nlm.nih.gov/databases/download/terms_and_conditions.html'  # noqa: E501
    assert response['meta_'].version == '20200327'
    assert response['meta_'].data_url == 'ftp://ftp.nlm.nih.gov/nlmdata/.chemidlease/'  # noqa: E501
    assert response['meta_'].rdp_url is None
    assert response['meta_'].data_license_attributes == {
        "non_commercial": False,
        "share_alike": False,
        "attribution": True
    }
