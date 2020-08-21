from typing import Optional

from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/query/{q_string}")
def read_query(q_string: str):
    resp = {'query': q_string}
    return resp
