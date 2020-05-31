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
        'antineoplastic': True
    }
    return Drug(**params)


def test_wikidata_normalize(cisplatin, wikidata):
    """Test that cisplatin term normalizes to correct drug concept."""
    normalized_drug = wikidata.normalize('cisplatin')
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.wikidata_identifier == cisplatin.wikidata_identifier
    assert normalized_drug.antineoplastic == cisplatin.antineoplastic
