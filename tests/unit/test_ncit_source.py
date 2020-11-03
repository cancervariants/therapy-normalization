"""Test that NCI Thesaurus source matches normalize correctly"""
import pytest
from therapy.schemas import Drug, MatchType
from therapy import query


@pytest.fixture(scope='module')
def ncit():
    """Build NCIt test fixture"""
    class QueryGetter:
        def normalize(self, query_str, incl='chembl'):
            resp = query.normalize(query_str, keyed=True, incl=incl)
            return resp['source_matches']['NCIt']

    c = QueryGetter()
    return c


@pytest.fixture(scope='module')
def voglibose():
    """Create voglibose drug fixture"""
    params = {
        'label': 'Voglibose',
        'concept_identifier': 'ncit:C95221',
        'aliases': ['3,4-Dideoxy-4-((2-Hydroxy-1-(Hydroxymethyl)Ethyl)Amino)-2-C-(Hydroxymethyl)-D-Epi-Inositol',  # noqa F401
                    'A-71100', 'AO-128', 'Basen',
                    'N-(1,3-Dihydroxy-2-Propyl)Valiolamine', 'VOGLIBOSE'],
        'other_identifiers': ['chemidplus:83480-29-9', 'fda:S77P977AG8'],
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def apricoxib():
    """Create aprocoxib drug fixture"""
    params = {
        'label': 'Apricoxib',
        'concept_identifier': 'C74021',
        'aliases': ['APRICOXIB', 'COX-2 Inhibitor TG01', 'CS-706', 'R-109339',
                    'TG01', 'TP2001'],
        'other_identifiers': ['chemidplus:197904-84-0', 'fda:5x5HB3Vz3Z'],
    }
    return Drug(**params)


def test_concept_id_voglibose(voglibose, ncit):
    """Test that Voglibose successfully matches to concept ID queries"""
    response = ncit.normalize('ncit:C95221')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    response_drug = response['records'][0]
    assert response_drug.concept_identifier == voglibose.concept_identifier
