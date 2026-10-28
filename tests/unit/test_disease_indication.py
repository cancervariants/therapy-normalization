"""Module for testing DiseaseIndicationBase. This is needed so we know if
disease-normalizer introduced breaking changes.
"""

import os

import pytest
from disease.schemas import (
    SourceName as DiseaseSourceName,
)
from ga4gh.core.models import Coding, Extension, MappableConcept
from ga4gh.va_spec.base import DataSet

from therapy.database.database import AbstractDatabase
from therapy.etl import ChEMBL


def test_normalize_disease(is_test_env: bool, database: AbstractDatabase):
    """Test that DiseaseIndicationBase works correctly when normalizing diseases"""
    if not is_test_env:
        pytest.skip(
            "only perform direct check on Disease Normalizer in testing environment"
        )

    # set up normalizer
    os.environ["DISEASE_DYNAMO_TABLE"] = "disease_normalizer_therapy_test"
    chembl = ChEMBL(database)
    chembl.disease_normalizer.db.drop_db()
    chembl.disease_normalizer.db.initialize_db()
    chembl.disease_normalizer.db.add_source_metadata(
        DiseaseSourceName.MONDO,
        DataSet(
            name=DiseaseSourceName.MONDO.value,
            version="2022-10-11",
            license=MappableConcept(
                name="CC BY 4.0",
                primaryCoding=Coding(
                    code="CC-BY-4.0",
                    system="https://spdx.org/licenses/",
                    iris=["https://creativecommons.org/licenses/by/4.0/legalcode"],
                ),
                extensions=[
                    Extension(
                        name="license_attributes",
                        value={
                            "non_commercial": False,
                            "attribution": True,
                            "share_alike": False,
                        },
                    ),
                ],
            ),
            extensions=[
                Extension(
                    name="data_url",
                    value="https://mondo.monarchinitiative.org/pages/download/",
                ),
                Extension(name="rdp_url", value="http://reusabledata.org/monarch.html"),
            ],
        ),
    )
    chembl.disease_normalizer.db.add_record(
        {
            "concept_id": "mondo:0700110",
            "label": "pneumonia, non-human animal",
        },
        DiseaseSourceName.MONDO,
    )
    chembl.disease_normalizer.db.complete_write_transaction()
    result = chembl._normalize_disease("mondo:0700110")
    assert result == "mondo:0700110"
