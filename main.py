"""Main application for FastAPI"""
from therapy.query import normalize, InvalidParameterException
from fastapi import FastAPI, HTTPException
import html
from typing import Optional

app = FastAPI()


@app.get("/")
def read_root():
    """Endpoint that returns default example of API root endpoint"""
    return {"Hello": "World"}


@app.get("/search")
def read_query(q: Optional[str] = '',
               keyed: Optional[bool] = False,
               incl: Optional[str] = '',
               excl: Optional[str] = ''):
    """Endpoint to return normalized responses for a query"""
    try:
        resp = normalize(html.unescape(q), keyed=keyed, incl=incl, excl=excl)
    except InvalidParameterException as e:
        raise HTTPException(status_code=422, detail=str(e))
    return resp
