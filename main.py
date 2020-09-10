"""Main application for FastAPI"""
from therapy.query import normalize
from fastapi import FastAPI
import html
from typing import Optional

app = FastAPI()


@app.get("/")
def read_root():
    """Endpoint that returns default example of API root endpoint"""
    return {"Hello": "World"}


@app.get("/search")
def read_query(q: Optional[str] = ''):
    """Endpoint to return normalized responses for a query"""
    resp = normalize(html.unescape(q))
    return resp
