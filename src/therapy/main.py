"""Main application for FastAPI"""

import html
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from enum import Enum
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query, Request

from therapy import __version__
from therapy.config import config
from therapy.database.database import create_db
from therapy.log import configure_logs
from therapy.query import InvalidParameterError, QueryHandler
from therapy.schemas import (
    APP_DESCRIPTION,
    LAB_EMAIL,
    LAB_WEBPAGE_URL,
    NormalizationService,
    SearchService,
    ServiceInfo,
    ServiceOrganization,
    ServiceType,
    UnmergedNormalizationService,
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """Perform operations that interact with the lifespan of the FastAPI instance.

    See https://fastapi.tiangolo.com/advanced/events/#lifespan.

    :param app: FastAPI instance
    """
    if config.debug:
        configure_logs(log_level=logging.DEBUG)
    else:
        configure_logs(log_level=logging.INFO)
    db = create_db()
    query_handler = QueryHandler(db)
    app.state.query_handler = query_handler
    yield
    db.close_connection()


app = FastAPI(
    title="Thera-Py",
    description=APP_DESCRIPTION,
    version=__version__,
    contact={
        "name": "Alex H. Wagner",
        "email": LAB_EMAIL,
        "url": LAB_WEBPAGE_URL,
    },
    license={
        "name": "MIT",
        "url": "https://github.com/cancervariants/therapy_normalization/blob/main/LICENSE",
    },
    docs_url="/therapy",
    openapi_url="/therapy/openapi.json",
    swagger_ui_parameters={"tryItOutEnabled": True},
    lifespan=lifespan,
)


class _Tag(str, Enum):
    """Define tag names for endpoints."""

    SEARCH = "Search"
    NORMALIZE = "Normalize"
    META = "Meta"


# endpoint description text
get_matches_summary = "Given query, provide highest matches from each source."
response_descr = "A response to a validly-formed query."
q_descr = "Therapy to search."
incl_descr = (
    "Comma-separated list of source names to include in response. Will "
    "exclude all other sources. Will return HTTP status code 422: "
    'Unprocessable Entity if both "incl" and "excl" parameters are given.'
)
excl_descr = (
    "Comma-separated list of source names to exclude in response. Will "
    "include all other sources. Will return HTTP status code 422: "
    'Unprocessable Entity if both "incl" and "excl" parameters are given.'
)
infer_descr = (
    "If true, attempt namespace inference when queries match known "
    "Local Unique Identifier patterns."
)
search_description = (
    "For each source, return strongest-match concepts for query string provided by user"
)
normalize_description = (
    "Return merged strongest-match concept for query string provided by user."
)
merged_matches_summary = (
    "Given query, provide merged normalized record as a Therapy Mappable Concept."
)
merged_response_descr = "A response to a validly-formed query."
normalize_q_descr = "Therapy to normalize."
unmerged_matches_summary = (
    "Given query, provide source records corresponding to normalized concept."
)
unmerged_response_descr = (
    "Response containing source records contained within normalized concept."
)
unmerged_normalize_description = (
    "Return unmerged records associated with the "
    "normalized result of the user-provided query "
    "string."
)


@app.get(
    "/therapy/search",
    summary=get_matches_summary,
    operation_id="getQueryResponse",
    response_description=response_descr,
    response_model_exclude_none=True,
    description=search_description,
    tags=[_Tag.SEARCH],
)
def search(
    request: Request,
    q: Annotated[str, Query(description=q_descr)],
    incl: Annotated[str | None, Query(description=incl_descr)] = "",
    excl: Annotated[str | None, Query(description=excl_descr)] = "",
    infer_namespace: Annotated[bool, Query(description=infer_descr)] = True,
) -> SearchService:
    """For each source, return strongest-match concepts for query string
    provided by user.

    :param q: therapy search term
    :param str incl: comma-separated list of sources to include, with all others
        excluded. Raises HTTPException if both `incl` and `excl` are given.
    :param str excl: comma-separated list of sources exclude, with all others included.
        Raises HTTPException if both `incl` and `excl` are given.
    :param bool infer_namespace: if True, try to infer namespace from query term.
    :returns: JSON response with matched records and source metadata
    """
    query_handler = request.app.state.query_handler
    try:
        response = query_handler.search(
            html.unescape(q),
            incl=incl,  # type: ignore[arg-type]
            excl=excl,  # type: ignore[arg-type]
            infer=infer_namespace,
        )
    except InvalidParameterError as e:
        raise HTTPException(status_code=422, detail=str(e)) from None
    return response


@app.get(
    "/therapy/normalize",
    summary=merged_matches_summary,
    operation_id="getMergedRecord",
    response_description=merged_response_descr,
    response_model_exclude_none=True,
    description=normalize_description,
    tags=[_Tag.NORMALIZE],
)
def normalize(
    request: Request,
    q: Annotated[str, Query(description=normalize_q_descr)],
    infer_namespace: Annotated[bool, Query(description=infer_descr)] = True,
) -> NormalizationService:
    """Return merged strongest-match concept for query string provided by
    user.

    :param q: therapy search term
    :param bool infer_namespace: if True, try to infer namespace from query term.
    :returns: JSON response with matching normalized record provided as a
    Therapy Mappable Concept, and source metadata
    """
    query_handler = request.app.state.query_handler
    try:
        response = query_handler.normalize(html.unescape(q), infer_namespace)
    except InvalidParameterError as e:
        raise HTTPException(status_code=422, detail=str(e)) from None
    return response


@app.get(
    "/therapy/normalize_unmerged",
    summary=unmerged_matches_summary,
    operation_id="getUnmergedRecords",
    response_description=unmerged_response_descr,
    description=unmerged_normalize_description,
    tags=[_Tag.NORMALIZE],
)
def normalize_unmerged(
    request: Request,
    q: Annotated[str, Query(description=normalize_q_descr)],
    infer_namespace: Annotated[bool, Query(description=infer_descr)] = True,
) -> UnmergedNormalizationService:
    """Return all individual records associated with a normalized concept.

    :param q: therapy search term
    :param bool infer_namespace: if True, try to infer namespace from query term.
    :returns: JSON response with matching normalized record and source metadata
    """
    query_handler = request.app.state.query_handler
    try:
        response = query_handler.normalize_unmerged(html.unescape(q), infer_namespace)
    except InvalidParameterError as e:
        raise HTTPException(status_code=422, detail=str(e)) from None
    return response


@app.get(
    "/service_info",
    summary="Get basic service information",
    description="Retrieve service metadata, such as versioning and contact info. Structured in conformance with the [GA4GH service info API specification](https://www.ga4gh.org/product/service-info/)",
    tags=[_Tag.META],
)
def service_info() -> ServiceInfo:
    """Provide service info per GA4GH Service Info spec
    :return: conformant service info description
    """
    return ServiceInfo(
        organization=ServiceOrganization(), type=ServiceType(), environment=config.env
    )
