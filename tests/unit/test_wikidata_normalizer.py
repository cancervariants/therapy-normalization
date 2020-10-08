"""Test that the therapy normalizer works as intended."""
import pytest
from therapy.normalizers import Wikidata
from therapy.models import Drug
from therapy.normalizers.base import MatchType


@pytest.fixture(scope='module')
def wikidata():
    """Create a Wikidata normalizer instance."""
    w = Wikidata()
    return w


@pytest.fixture(scope='module')
def cisplatin():
    """Create a cisplatin drug fixture."""
    params = {
        'label': 'cisplatin',
        'concept_identifier': 'wikidata:Q412415',
        'aliases': list(),
        'other_identifiers': list(),
    }
    return Drug(**params)


def test_wikidata_normalize(cisplatin, wikidata):
    """Test that cisplatin term normalizes to correct drug concept."""
    normalizer_response = wikidata.normalize('cisplatin')
    assert normalizer_response.match_type == MatchType.PRIMARY
    assert len(normalizer_response.records) == 1
    normalized_drug = normalizer_response.records[0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier


def test_non_breaking_space(cisplatin, wikidata, caplog):
    """Test that leading and trailing whitespace are removed."""
    normalizer_response = wikidata.normalize('    \u0020\xa0cisplatin\xa0 \t')
    assert normalizer_response.match_type == MatchType.PRIMARY

    normalizer_response = wikidata.normalize(' \xa0CISplatin\xa0 \t')
    assert normalizer_response.match_type == MatchType.CASE_INSENSITIVE_PRIMARY
    assert 'Query contains non breaking space characters.' not in caplog.text

    normalizer_response = wikidata.normalize('\u0020CIS\u00A0platin\xa0')
    assert normalizer_response.match_type == MatchType.NO_MATCH
    assert 'Query contains non breaking space characters.' in caplog.text
