"""Test that the therapy normalizer works as intended."""
import pytest
from therapy.normalizers import Wikidata
from therapy.models import Drug


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
    assert normalizer_response.match_type == 'match'
    assert len(normalizer_response.therapy_records) == 1
    normalized_drug = normalizer_response.therapy_records[0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier
    assert normalized_drug.fda_approved == cisplatin.fda_approved
