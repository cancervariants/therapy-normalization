"""Main application for FastAPI"""
from therapy.query import QueryHandler, InvalidParameterException
from therapy.schemas import Service, MergedService
from fastapi import FastAPI, HTTPException, Query
from fastapi.openapi.utils import get_openapi
import html
from typing import Optional


query_handler = QueryHandler()
app = FastAPI(docs_url='/therapy', openapi_url='/therapy/openapi.json')


def custom_openapi():
    """Generate custom fields for OpenAPI response"""
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="The VICC Therapy Normalizer",
        version="0.1.0",
        openapi_version="3.0.3",
        description="Normalize drugs and other therapy terms.",
        routes=app.routes
    )
#    openapi_schema['info']['license'] = {  # TODO
#        "name": "Name-of-license",
#        "url": "http://www.to-be-determined.com"
#    }
    openapi_schema['info']['contact'] = {  # TODO
        "name": "Alex H. Wagner",
        "email": "Alex.Wagner@nationwidechildrens.org"
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi

# endpoint description text
get_matches_summary = """Given query, provide highest matches from
                     each source."""
response_descr = "A response to a validly-formed query."
q_descr = "Therapy to normalize."
keyed_descr = """Optional. If true, return response as key-value pairs of
              sources to source matches. False by default"""
incl_descr = """Optional. Comma-separated list of source names to include in
             response. Will exclude all other sources. Will return HTTP
             status code 422: Unprocessable Entity if both 'incl' and
             'excl' parameters are given."""
excl_descr = """Optional. Comma-separated list of source names to exclude in
             response. Will include all other sources. Will return HTTP
             status code 422: Unprocessable Entity if both 'incl' and
             'excl' parameters are given."""


@app.get("/therapy/search",
         summary=get_matches_summary,
         operation_id="getQueryResponse",
         response_description=response_descr,
         response_model=Service)
def get_matches(q: str = Query(..., description=q_descr),
                keyed: Optional[bool] = Query(False, description=keyed_descr),
                incl: Optional[str] = Query('', description=incl_descr),
                excl: Optional[str] = Query('', description=excl_descr)):
    """For each source, return strongest-match concepts for query string
    provided by user.

    :param q: therapy search term
    :param keyed: if true, response is structured as key/value pair of
        sources to source match lists.
    :param incl: comma-separated list of sources to include, with all others
        excluded. Raises HTTPException if both `incl` and `excl` are given.
    :param excl: comma-separated list of sources exclude, with all others
        included. Raises HTTPException if both `incl` and `excl` are given.

    :returns: JSON response with matched records and source metadata
    """
    try:
        resp = query_handler.search_sources(html.unescape(q), keyed=keyed,
                                            incl=incl, excl=excl)
    except InvalidParameterException as e:
        raise HTTPException(status_code=422, detail=str(e))

    return resp


merged_matches_summary = """Given query, provide merged record from given
                            sources."""
merged_response_descr = ""
merged_q_descr = ""


@app.get("/therapy/normalize",
         summary=merged_matches_summary,
         operation_id="getQuerymergedResponse",
         response_description=merged_response_descr,
         response_model=MergedService)
def get_merged_matches(q: str = Query(..., description=merged_q_descr)):
    """Return merged strongest-match concept for query string provided by
    user.

    :param q: therapy search term
    """
    try:
        response = query_handler.search_groups(q)
    except InvalidParameterException as e:
        raise HTTPException(status_code=422, detail=str(e))
    return response
