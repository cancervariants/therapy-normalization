"""Construct static mapping of disease normalizer input.
Assumes complete and functioning disease normalizer endpoint is available.
"""
import json
from pathlib import Path
from typing import Dict

from disease.database import create_db as create_disease_db
from disease.query import QueryHandler as DiseaseQueryHandler
from disease.schemas import NormalizationService as DiseaseNormalizationService

from therapy.database.dynamodb import DynamoDatabase
from therapy.etl import ChEMBL, HemOnc

TEST_ROOT = Path(__file__).resolve().parents[1]
TEST_DATA_DIRECTORY = TEST_ROOT / "data"


class ReadOnlyDatabase(DynamoDatabase):
    """Provide read-only instance of database for security's sake"""

    def add_record(self, record: Dict, record_type: str = "identity") -> None:
        """Add new record to database"""
        pass


db = ReadOnlyDatabase()
disease_normalizer_table = {}


class SaveQueryHandler(DiseaseQueryHandler):
    """Disease query class which saves lookups and results"""

    def normalize(self, query: str) -> DiseaseNormalizationService:
        """Normalize query term"""
        response = super().normalize(query)
        result = response.normalized_id
        disease_normalizer_table[query.lower()] = result
        return response


disease_query_handler = SaveQueryHandler(create_disease_db())

ch = ChEMBL(database=db, data_path=TEST_DATA_DIRECTORY)
ch.disease_normalizer = disease_query_handler
ch.perform_etl(use_existing=True)

h = HemOnc(database=db, data_path=TEST_DATA_DIRECTORY)
h.disease_normalizer = disease_query_handler
h.perform_etl(use_existing=True)


with open(TEST_DATA_DIRECTORY / "disease_normalization.json", "w") as f:
    # for consistency/easier diffing
    sorted_dict = {
        key: value for key, value in sorted(disease_normalizer_table.items())
    }
    json.dump(sorted_dict, f, indent=2)
