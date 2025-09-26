"""Perform basic tests of endpoint branches.

We already have tests and data validation to ensure correctness of the underlying
response objects -- here, we're checking for bad branch logic and for basic assurances
that routes integrate correctly with query methods.
"""

from pathlib import Path

import jsonschema
import pytest
import pytest_asyncio
import yaml
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from therapy.main import app


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
async def test_service_info(api_client: AsyncClient, test_data: Path):
    response = await api_client.get("/therapy/service-info")
    response.raise_for_status()

    with (test_data / "service_info_openapi.yaml").open() as f:
        spec = yaml.safe_load(f)

    resp_schema = spec["paths"]["/service-info"]["get"]["responses"]["200"]["content"][
        "application/json"
    ]["schema"]

    resolver = jsonschema.RefResolver.from_schema(spec)
    data = response.json()
    jsonschema.validate(instance=data, schema=resp_schema, resolver=resolver)
