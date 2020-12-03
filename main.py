"""Main application for FastAPI"""
# from therapy.query import normalize, InvalidParameterException
from therapy.query import Normalizer, InvalidParameterException
from therapy.schemas import Service
from fastapi import FastAPI, HTTPException
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
        description="TODO Description here",  # TODO
        routes=app.routes
    )
    # TODO: Figure out how to add these back
    # openapi_schema['info']['license'] = {  # TODO
    #     "name": "name of our license",
    #     "url": "link to it"
    # }
    # openapi_schema['info']['contact'] = {  # TODO
    #     "name": "alex wagner",
    #     "email": "his email"
    # }
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi


@app.get("/search",
         summary="Given query, provide matches and match ratings from "
                 "aggregated sources",
         operation_id="getQueryResponse",
         response_description="A response to a validly-formed query",  # TODO
         response_model=Service
         )
def read_query(q: str,  # noqa: D103
               keyed: Optional[bool] = False,
               incl: Optional[str] = '',
               excl: Optional[str] = ''):
    try:
        resp = normalizer.normalize(html.unescape(q), keyed=keyed, incl=incl,
                                    excl=excl)
    except InvalidParameterException as e:
        raise HTTPException(status_code=422, detail=str(e))

    return resp
