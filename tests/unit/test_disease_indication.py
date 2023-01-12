"""Module for testing DiseaseIndicationBase. This is needed so we know if
disease-normalizer introduced breaking changes. Should only run during CI tests.
"""
import os

from disease.database import AWS_ENV_VAR_NAME
from disease.query import QueryHandler as DiseaseQueryHandler

from therapy.etl.base import DiseaseIndicationBase


RUN_TEST = os.environ.get("THERAPY_TEST", "").lower() == "true" and AWS_ENV_VAR_NAME not in os.environ  # noqa: E501


class TestDiseaseIndication(DiseaseIndicationBase):
    """Test instance for DiseaseIndicationBase"""

    def __init__(self):
        """Initialize test TestDiseaseIndication"""
        self.disease_normalizer = DiseaseQueryHandler()

        if RUN_TEST:
            self.load_disease_test_data()

    def load_disease_test_data(self):
        """Load dummy disease data"""
        disease = {
            "label_and_type": "ncit:c1##identity",
            "item_type": "identity",
            "src_name": "NCIt",
            "merge_ref": "ncit:C1",
            "concept_id": "ncit:C1",
            "label": "test disease identity",
        }
        self.disease_normalizer.db.diseases.put_item(Item=disease)

        disease_merger = {
            "label_and_type": "ncit:c1##merger",
            "concept_id": "ncit:C1",
            "item_type": "merger",
            "label": "dummy disease merger",
        }
        self.disease_normalizer.db.diseases.put_item(Item=disease_merger)

        source = {
            "src_name": "NCIt",
            "data_license": "CC BY 4.0",
            "data_license_url": "https://creativecommons.org/licenses/by/4.0/legalcode",
            "version": "1.0.0",
            "data_license_attributes": {
                "non_commercial": False,
                "share_alike": False,
                "attribution": True
            }
        }
        self.disease_normalizer.db.metadata.put_item(Item=source)

    def _download_data():
        """Implement abstract method"""
        pass

    def _http_download():
        """Implement abstract method"""
        pass

    def _transform_data():
        """Implement abstract method"""
        pass

    def _load_meta():
        """Implement abstract method"""
        pass


def test_normalize_disease():
    """Test that DiseaseIndicationBase works correctly when normalizing diseases"""
    if RUN_TEST:
        dib = TestDiseaseIndication()
        norm_disease = dib._normalize_disease("ncit:C1")
        assert norm_disease == "ncit:C1"
