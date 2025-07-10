"""Perform basic tests of endpoint branches.

We already have tests and data validation to ensure correctness of the underlying
response objects -- here, we're checking for bad branch logic and for basic assurances
that routes integrate correctly with query methods.
"""

import re
from datetime import datetime

import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from therapy.main import app
from therapy.schemas import ServiceEnvironment


@pytest_asyncio.fixture
async def async_app():
    async with LifespanManager(app) as manager:
        yield manager.app


@pytest_asyncio.fixture
async def api_client(async_app):
    async with AsyncClient(
        transport=ASGITransport(async_app), base_url="http://tests"
    ) as client:
        yield client


@pytest.mark.asyncio
async def test_search(api_client: AsyncClient):
    """Test /search endpoint."""
    response = await api_client.get("/therapy/search?q=trastuzumab")
    assert response.status_code == 200
    assert (
        response.json()["source_matches"]["NCIt"]["records"][0]["concept_id"]
        == "ncit:C1647"
    )

    response = await api_client.get("/therapy/search?q=trastuzumab&incl=sdkl")
    assert response.status_code == 422

    response = await api_client.get("/therapy/search")
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_normalize(api_client: AsyncClient):
    """Test /normalize endpoint."""
    response = await api_client.get("/therapy/normalize?q=cisplatin")
    assert response.status_code == 200
    assert response.json()["therapy"]["primaryCoding"] == {
        "code": "2555",
        "id": "rxcui:2555",
        "system": "https://mor.nlm.nih.gov/RxNav/search?searchBy=RXCUI&searchTerm=",
    }

    response = await api_client.get("/therapy/normalize")
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_service_info(api_client: AsyncClient):
    response = await api_client.get("/service_info")
    assert response.status_code == 200
    expected_version_pattern = (
        r"\d+\.\d+\.?"  # at minimum, should be something like "0.1"
    )
    response_json = response.json()
    assert response_json["id"] == "org.genomicmedlab.thera_py"
    assert response_json["name"] == "thera_py"
    assert response_json["type"]["group"] == "org.genomicmedlab"
    assert response_json["type"]["artifact"] == "Thera-Py API"
    assert re.match(expected_version_pattern, response_json["type"]["version"])
    assert (
        response_json["description"]
        == "Resolve ambiguous references and descriptions of drugs and therapies to consistently-structured, normalized terms"
    )
    assert response_json["organization"] == {
        "name": "Genomic Medicine Lab at Nationwide Children's Hospital",
        "url": "https://www.nationwidechildrens.org/specialties/institute-for-genomic-medicine/research-labs/wagner-lab",
    }
    assert response_json["contactUrl"] == "Alex.Wagner@nationwidechildrens.org"
    assert (
        response_json["documentationUrl"]
        == "https://github.com/cancervariants/therapy-normalization"
    )
    assert datetime.fromisoformat(response_json["createdAt"])
    assert ServiceEnvironment(response_json["environment"])
    assert re.match(expected_version_pattern, response_json["version"])
