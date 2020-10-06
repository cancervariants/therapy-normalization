"""Test therapy querying method"""
from therapy.query import normalize


def test_query():
    """Test that query returns properly-structured response"""
    resp = normalize('cisplatin', keyed=False)
    assert resp['query'] == 'cisplatin'
    matches = resp['normalizer_matches']
    assert isinstance(matches, list)
    assert len(matches) == 2
    wikidata = list(filter(lambda m: m['normalizer'] == 'Wikidata',
                           matches))[0]
    assert len(wikidata['records']) == 1
    wikidata_record = wikidata['records'][0]
    assert wikidata_record.label == 'cisplatin'


def test_query_keyed():
    """Test that query structures matches as dict when requested"""
    resp = normalize('cisplatin', keyed=True)
    assert resp['query'] == 'cisplatin'
    matches = resp['normalizer_matches']
    assert isinstance(matches, dict)
    assert len(matches) == 2
    wikidata = matches['Wikidata']
    assert len(wikidata['records']) == 1
    wikidata_record = wikidata['records'][0]
    assert wikidata_record.label == 'cisplatin'
