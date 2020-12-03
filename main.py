"""Main application for FastAPI"""
# from therapy.query import normalize, InvalidParameterException
from therapy.query import Normalizer, InvalidParameterException
from therapy.schemas import Service
from fastapi import FastAPI, HTTPException, Query
from fastapi.openapi.utils import get_openapi
import html
from typing import Optional


normalizer = Normalizer()
app = FastAPI(docs_url='/')


def custom_openapi():
    """Generate custom fields for OpenAPI response"""
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Therapy Normalizer",
        version="0.1.0",
        description="Normalize therapy terms.",
        routes=app.routes
    )
    openapi_schema['info']['license'] = {  # TODO
        "name": "name of license",
        "url": "link"
    }
    openapi_schema['info']['contact'] = {  # TODO
        "name": "Alex Wagner",
        "email": "email@email.com"
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi

# endpoint parameter description text
read_query_summary = "Given query, provide highest matches from "\
                     "aggregated sources."
response_description = "A response to a validly-formed query."
q_descr = "Therapy to normalize."
keyed_descr = "Optional. If true, return response as key-value pairs of "\
              "sources to source matches. False by default"
incl_descr = "Optional. Comma-separated list of source names to include in "\
             "response. Will exclude all other sources. Will return HTTP "\
             "status code 422: Unprocessable Entity if both 'incl' and "\
             "'excl' parameters are given."
excl_descr = "Optional. Comma-separated list of source names to exclude in "\
             "response. Will include all other sources. Will return HTTP "\
             "status code 422: Unprocessable Entity if both 'incl' and "\
             "'excl' parameters are given."


@app.get("/search",
         summary=read_query_summary,
         operation_id="getQueryResponse",
         response_description=response_description,
         response_model=Service)
def read_query(q: str = Query(..., description=q_descr),  # noqa: D103
               keyed: Optional[bool] = Query(False, description=keyed_descr),
               incl: Optional[str] = Query('', description=incl_descr),
               excl: Optional[str] = Query('', description=excl_descr)):
    try:
        resp = normalizer.normalize(html.unescape(q), keyed=keyed, incl=incl,
                                    excl=excl)
    except InvalidParameterException as e:
        raise HTTPException(status_code=422, detail=str(e))

    return resp
