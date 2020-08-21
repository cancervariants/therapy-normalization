from typing import Optional
from therapy.normalizers.query import normalize_query
from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/query/{q_string}")
def read_query(q_string: str):
    resp = normalize_query(q_string)
    return resp
