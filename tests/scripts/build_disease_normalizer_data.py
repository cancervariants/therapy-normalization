"""Construct static mapping of disease normalizer input.
Assumes complete and functioning disease normalizer endpoint is available.
"""
import json
from pathlib import Path
from typing import Dict

from disease.database import create_db as create_disease_db
from disease.query import QueryHandler as DiseaseQueryHandler
from disease.schemas import NormalizationService as DiseaseNormalizationService

from therapy.database import create_db
from therapy.etl import ChEMBL, HemOnc
from therapy.schemas import SourceMeta, SourceName

TEST_ROOT = Path(__file__).resolve().parents[1]
TEST_DATA_DIRECTORY = TEST_ROOT / "data"


def add_record(record: Dict, src_name: SourceName) -> None:
    """Mock method. Provided here to ensure therapy DB is read-only"""


def add_source_metadata(src_name: SourceName, data: SourceMeta) -> None:
    """Mock method. Provided here to ensure therapy DB is read-only"""


therapy_db = create_db()
therapy_db.add_record = add_record  # type: ignore
therapy_db.add_source_metadata = add_source_metadata  # type: ignore


disease_normalizer_table = {}


class SaveQueryHandler(DiseaseQueryHandler):
    """Disease query class which saves lookups and results"""

    def normalize(self, query: str) -> DiseaseNormalizationService:
        """Normalize query term"""
        response = super().normalize(query)
        if response.disease_descriptor:
            result = response.disease_descriptor.disease_id
        else:
            result = None
        disease_normalizer_table[query.lower()] = result
        return response


disease_query_handler = SaveQueryHandler(create_disease_db())

ch = ChEMBL(database=therapy_db, data_path=TEST_DATA_DIRECTORY)
ch.disease_normalizer = disease_query_handler
ch.perform_etl(use_existing=True)

h = HemOnc(database=therapy_db, data_path=TEST_DATA_DIRECTORY)
h.disease_normalizer = disease_query_handler
h.perform_etl(use_existing=True)


with open(TEST_DATA_DIRECTORY / "disease_normalization.json", "w") as f:
    json.dump(disease_normalizer_table, f, indent=2)
