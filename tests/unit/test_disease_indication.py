"""Module for testing DiseaseIndicationBase. This is needed so we know if
disease-normalizer introduced breaking changes. Should only run during CI tests.
"""
import os

import pytest
from disease.database import AWS_ENV_VAR_NAME, create_db as create_disease_db
# from disease.schemas import SourceName as DiseaseSourceName, \
#     SourceMeta as DiseaseSourceMeta

from therapy.etl.chembl import ChEMBL
from therapy.schemas import DatabaseType


@pytest.fixture(scope="module")
def disease_database(database):
    """Provide pseudo instance of disease database.

    Because `test_normalize_disease` should only ever run in CI, we can make some
    assumptions about how the therapy DB is being defined (ie via env var), so we'll
    similarly cheat a bit to stand up corresponding disease DB instances.
    """
    if database.db_type == DatabaseType.DYNAMODB:
        disease_db = create_disease_db(os.environ.get("THERAPY_NORM_DB_URL"))
    elif database.db_type == DatabaseType.POSTGRESQL:
        disease_db = create_disease_db(os.environ.get("DISEASE_NORM_DB_URL"))
    else:
        raise ValueError("unrecognized DB instance")
    disease_db.drop_db()
    disease_db.initialize_db()
    # disease_db.add_source_metadata(
    #     DiseaseSourceName.MONDO,
    #     DiseaseSourceMeta(
    #         data_license="CC BY 4.0",
    #         data_license_url="https://creativecommons.org/licenses/by/4.0/legalcode",
    #         data_url="https://mondo.monarchinitiative.org/pages/download",
    #         rdp_url="http://reusabledata.org/monarch.html",
    #         version="2022-10-11",
    #         data_license_attributes={
    #             "non_commercial": False,
    #             "share_alike": False,
    #             "attribution": True
    #         }
    #     )
    # )
    # disease_db.add_record(
    #     record={
    #         "concept_id": "mondo:0700110",
    #         "label": "pneumonia, non-human animal",
    #     },
    #     src_name=DiseaseSourceName.MONDO
    # )
    # disease_db.add_merged_record({
    #     "concept_id": "mondo:0700110",
    #     "label": "pneumonia, non-human animal",
    # })
    # disease_db.complete_write_transaction()
    # disease_db.update_merge_ref("mondo:0700110", "mondo:0700110")
    # disease_db.complete_write_transaction()
    # yield disease_db
    # disease_db.close_connection()


RUN_TEST = os.environ.get("THERAPY_TEST", "").lower() == "true" and AWS_ENV_VAR_NAME not in os.environ  # noqa: E501


@pytest.mark.skipif(not RUN_TEST, reason="only run in CI")
def test_normalize_disease(database, disease_database):
    """Test that DiseaseIndicationBase works correctly when normalizing diseases"""
    ch = ChEMBL(database=database, disease_db=disease_database)
    norm_disease = ch._normalize_disease("mondo:0700110")
    assert norm_disease == "mondo:0700110"
