"""Main application for FastAPI"""
from therapy.query import normalize
from fastapi import FastAPI
import html
from typing import Optional
import re
from fastapi.responses import JSONResponse

app = FastAPI()


@app.get("/")
def read_root():
    """Endpoint that returns default example of API root endpoint"""
    return {"Hello": "World"}


@app.get("/search")
def read_query(q: Optional[str] = ''):
    """Endpoint to return normalized responses for a query"""
    resp = normalize(html.unescape(q))

    nbsp = re.search('\u0020|\xa0|\u00A0|&nbsp;', q)
    if nbsp:
        return JSONResponse(
            {'WARNING': 'QUERY CONTAINS NON BREAKING SPACE CHARACTERS'}
        )
    return resp
