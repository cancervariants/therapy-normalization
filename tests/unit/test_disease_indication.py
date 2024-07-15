"""Module for testing DiseaseIndicationBase. This is needed so we know if
disease-normalizer introduced breaking changes.
"""

import os

import pytest
from disease.schemas import (
    SourceMeta as DiseaseSourceMeta,
)
from disease.schemas import (
    SourceName as DiseaseSourceName,
)

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
        DiseaseSourceMeta(
            data_url="https://mondo.monarchinitiative.org/pages/download/",
            data_license="CC BY 4.0",
            data_license_url="https://creativecommons.org/licenses/by/4.0/legalcode",
            version="2022-10-11",
            rdp_url="http://reusabledata.org/monarch.html",
            data_license_attributes={
                "non_commercial": False,
                "attribution": True,
                "share_alike": False,
            },
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
