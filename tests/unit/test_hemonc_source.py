"""Test that the therapy normalizer works as intended for the HemOnc.org
source.
"""
import pytest

from tests.conftest import compare_response
from therapy.query import QueryHandler
from therapy.schemas import Drug, MatchType, ApprovalStatus
import re


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
        "other_identifiers": ["RxNorm:2555"],
        "xrefs": [],
        "approval_status": ApprovalStatus.APPROVED,
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
        "other_identifiers": ["RxNorm:134547"],
        "xrefs": [],
        "approval_status": ApprovalStatus.APPROVED,
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
        "other_identifiers": ["RxNorm:475230"],
        "xrefs": [],
        "approval_status": ApprovalStatus.APPROVED,
        "trade_names": ["Firmagon"]
    })


def test_concept_id_match(hemonc, cisplatin, bendamustine, degarelix):
    """Test that concept ID queries resolve to correct record."""
    response = hemonc.search('hemonc:105')
    compare_response(response, cisplatin, MatchType.CONCEPT_ID)

    response = hemonc.search('hemonc:65')
    compare_response(response, bendamustine, MatchType.CONCEPT_ID)

    response = hemonc.search('hemonc:151')
    compare_response(response, degarelix, MatchType.CONCEPT_ID)


def test_label_match(hemonc, cisplatin, bendamustine, degarelix):
    """Test that label queries resolve to correct record."""
    response = hemonc.search('cisplatin')
    compare_response(response, cisplatin, MatchType.LABEL)

    response = hemonc.search('Bendamustine')
    compare_response(response, bendamustine, MatchType.LABEL)

    response = hemonc.search('DEGARELIX')
    compare_response(response, degarelix, MatchType.LABEL)


def test_alias_match(hemonc, cisplatin, bendamustine, degarelix):
    """Test that alias queries resolve to correct record."""
    response = hemonc.search('ddp')
    compare_response(response, cisplatin, MatchType.ALIAS)

    response = hemonc.search('dacp')
    compare_response(response, cisplatin, MatchType.ALIAS)

    response = hemonc.search('nsc 119875')
    compare_response(response, cisplatin, MatchType.ALIAS)

    response = hemonc.search('cep-18083')
    compare_response(response, bendamustine, MatchType.ALIAS)

    response = hemonc.search('bendamustine hydrochloride')
    compare_response(response, bendamustine, MatchType.ALIAS)

    response = hemonc.search('asp3550')
    compare_response(response, degarelix, MatchType.ALIAS)


def test_trade_name(hemonc, bendamustine, degarelix):
    """Test that trade name queries resolve to correct record."""
    response = hemonc.search('bendamax')
    compare_response(response, bendamustine, MatchType.TRADE_NAME)

    response = hemonc.search('purplz')
    compare_response(response, bendamustine, MatchType.TRADE_NAME)

    response = hemonc.search('firmagon')
    compare_response(response, degarelix, MatchType.TRADE_NAME)

    # no trade names for records with > 20
    response = hemonc.search('platinol')
    assert response['match_type'] == MatchType.NO_MATCH


def test_other_id_match(hemonc, cisplatin, bendamustine, degarelix):
    """Test that other_id query resolves to correct record."""
    response = hemonc.search('rxnorm:2555')
    compare_response(response, cisplatin, MatchType.OTHER_ID)

    response = hemonc.search('rxnorm:134547')
    compare_response(response, bendamustine, MatchType.OTHER_ID)

    response = hemonc.search('RXNORM:475230')
    compare_response(response, degarelix, MatchType.OTHER_ID)


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
        "attribution": False,
        "share_alike": True,
    }
