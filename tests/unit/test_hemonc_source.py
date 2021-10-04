"""Test that the therapy normalizer works as intended for the HemOnc.org
source.
"""
import re

import pytest
from tests.conftest import compare_response

from therapy.query import QueryHandler
from therapy.schemas import Drug, MatchType, ApprovalStatus


@pytest.fixture(scope='module')
def hemonc():
    """Build HemOnc normalizer test fixture."""
    class QueryGetter:

        def __init__(self):
            self.query_handler = QueryHandler()

        def search(self, query_str):
            resp = self.query_handler.search_sources(query_str, keyed=True,
                                                     incl='hemonc')
            return resp['source_matches']['HemOnc']

        def fetch_meta(self):
            return self.query_handler._fetch_meta('HemOnc')

    return QueryGetter()


@pytest.fixture(scope='module')
def cisplatin():
    """Construct cisplatin fixture."""
    return Drug(**{
        "concept_id": "hemonc:105",
        "label": "Cisplatin",
        "aliases": [
            "cis-diamminedichloroplatinum III",
            "DDP",
            "cisplatinum",
            "cis-platinum",
            "DACP",
            "NSC 119875",
            "CDDP"
        ],
        "trade_names": [],
        "xrefs": ["rxcui:2555"],
        "associated_with": [],
        "approval_status": ApprovalStatus.APPROVED,
        "approval_year": [1978],
        "has_indication": [
            {
                "disease_id": "hemonc:671",
                "disease_label": "Testicular cancer",
                "normalized_disease_id": "ncit:C7251"
            },
            {
                "disease_id": "hemonc:645",
                "disease_label": "Ovarian cancer",
                "normalized_disease_id": "ncit:C7431"
            },
            {
                "disease_id": "hemonc:569",
                "disease_label": "Bladder cancer",
                "normalized_disease_id": "ncit:C9334"
            }
        ]
    })


@pytest.fixture(scope='module')
def bendamustine():
    """Construct bendamustine fixture."""
    return Drug(**{
        "concept_id": "hemonc:65",
        "label": "Bendamustine",
        "aliases": [
            "CEP-18083",
            "cytostasan hydrochloride",
            "SyB L-0501",
            "SDX-105",
            "bendamustine hydrochloride",
            "bendamustin hydrochloride"
        ],
        "xrefs": ["rxcui:134547"],
        "associated_with": [],
        "trade_names": [
            "Bendamax",
            "Bendawel",
            "Bendeka",
            "Bendit",
            "Innomustine",
            "Leuben",
            "Levact",
            "Maxtorin",
            "MyMust",
            "Purplz",
            "Ribomustin",
            "Treakisym",
            "Treanda",
            "Xyotin"
        ],
        "approval_status": ApprovalStatus.APPROVED,
        "approval_year": ["2008", "2015"],
        "has_indication": [
            {
                "disease_id": "hemonc:581",
                "disease_label": "Chronic lymphocytic leukemia",
                "normalized_disease_id": "ncit:C3163"
            },
            {
                "disease_id": "hemonc:46094",
                "disease_label": "Indolent lymphoma",
                "normalized_disease_id": "ncit:C8504"
            }
        ]
    })


@pytest.fixture(scope='module')
def degarelix():
    """Create fixture for degarelix drug."""
    return Drug(**{
        "label": "Degarelix",
        "concept_id": "hemonc:151",
        "aliases": [
            "degarelix acetate",
            "FE200486",
            "ASP3550"
        ],
        "xrefs": ["rxcui:475230"],
        "associated_with": [],
        "trade_names": ["Firmagon"],
        "approval_status": ApprovalStatus.APPROVED,
        "approval_year": ["2008"],
        "has_indication": [
            {
                "disease_id": "hemonc:658",
                "disease_label": "Prostate cancer",
                "normalized_disease_id": "ncit:C7378"
            }
        ],
    })


def test_concept_id_match(hemonc, cisplatin, bendamustine, degarelix):
    """Test that concept ID queries resolve to correct record."""
    response = hemonc.search('hemonc:105')
    compare_response(response, MatchType.CONCEPT_ID, cisplatin)

    response = hemonc.search('hemonc:65')
    compare_response(response, MatchType.CONCEPT_ID, bendamustine)

    response = hemonc.search('hemonc:151')
    compare_response(response, MatchType.CONCEPT_ID, degarelix)


def test_label_match(hemonc, cisplatin, bendamustine, degarelix):
    """Test that label queries resolve to correct record."""
    response = hemonc.search('cisplatin')
    compare_response(response, MatchType.LABEL, cisplatin)

    response = hemonc.search('Bendamustine')
    compare_response(response, MatchType.LABEL, bendamustine)

    response = hemonc.search('DEGARELIX')
    compare_response(response, MatchType.LABEL, degarelix)


def test_alias_match(hemonc, cisplatin, bendamustine, degarelix):
    """Test that alias queries resolve to correct record."""
    response = hemonc.search('ddp')
    compare_response(response, MatchType.ALIAS, cisplatin)

    response = hemonc.search('dacp')
    compare_response(response, MatchType.ALIAS, cisplatin)

    response = hemonc.search('nsc 119875')
    compare_response(response, MatchType.ALIAS, cisplatin)

    response = hemonc.search('cep-18083')
    compare_response(response, MatchType.ALIAS, bendamustine)

    response = hemonc.search('bendamustine hydrochloride')
    compare_response(response, MatchType.ALIAS, bendamustine)

    response = hemonc.search('asp3550')
    compare_response(response, MatchType.ALIAS, degarelix)


def test_trade_name(hemonc, bendamustine, degarelix):
    """Test that trade name queries resolve to correct record."""
    response = hemonc.search('bendamax')
    compare_response(response, MatchType.TRADE_NAME, bendamustine)

    response = hemonc.search('purplz')
    compare_response(response, MatchType.TRADE_NAME, bendamustine)

    response = hemonc.search('firmagon')
    compare_response(response, MatchType.TRADE_NAME, degarelix)

    # no trade names for records with > 20
    response = hemonc.search('platinol')
    assert response['match_type'] == MatchType.NO_MATCH


def test_xref_match(hemonc, cisplatin, bendamustine, degarelix):
    """Test that xref query resolves to correct record."""
    response = hemonc.search('rxcui:2555')
    compare_response(response, MatchType.XREF, cisplatin)

    response = hemonc.search('rxcui:134547')
    compare_response(response, MatchType.XREF, bendamustine)

    response = hemonc.search('rxcui:475230')
    compare_response(response, MatchType.XREF, degarelix)


def test_metadata(hemonc):
    """Test that source metadata returns correctly."""
    response = hemonc.fetch_meta()
    assert response.data_license == "CC BY 4.0"
    assert response.data_license_url == "https://creativecommons.org/licenses/by/4.0/legalcode"  # noqa: E501
    assert re.match(r'202[0-9][01][0-9][0-3][0-9]', response.version)
    assert response.data_url == "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/9CY9C6"  # noqa: E501
    assert response.rdp_url is None
    assert response.data_license_attributes == {
        "non_commercial": False,
        "attribution": True,
        "share_alike": False,
    }
