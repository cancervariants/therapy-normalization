"""Module for testing DiseaseIndicationBase. This is needed so we know if
disease-normalizer introduced breaking changes. Should only run during CI tests.
"""
# import os
# from pathlib import Path
#
# import pytest
# from disease.database import AWS_ENV_VAR_NAME, create_db as create_disease_db
# from disease.query import QueryHandler as DiseaseQueryHandler
# from therapy.database.database import AbstractDatabase
#
# from therapy.etl.base import DEFAULT_DATA_PATH, DiseaseIndicationBase
#
#
# RUN_TEST = os.environ.get("THERAPY_TEST", "").lower() == "true" and AWS_ENV_VAR_NAME not in os.environ  # noqa: E501
#
#
# @pytest.fixture(scope="module")
#
# class TestDiseaseIndication(DiseaseIndicationBase):
#     """Test instance for DiseaseIndicationBase"""
#
#     def __init__(self, database: AbstractDatabase, data_path: Path = DEFAULT_DATA_PATH):  # noqa: E501
#         """Initialize test TestDiseaseIndication"""
#         super().__init__(database, data_path)
#
#         if RUN_TEST:
#             self.load_disease_test_data()
#
#     def load_disease_test_data(self):
#         """Load disease data"""
#         disease = {
#             "label_and_type": "mondo:0700110##identity",
#             "item_type": "identity",
#             "src_name": "Mondo",
#             "concept_id": "mondo:0700110",
#             "label": "pneumonia, non-human animal",
#             "merge_ref": "mondo:07001110##merger"
#         }
#         self.disease_normalizer.db.diseases.put_item(Item=disease)
#
#         merge = {
#             "label_and_type": "mondo:0700110##merger",
#             "item_type": "merger",
#             "src_name": "Mondo",
#             "concept_id": "mondo:0700110",
#             "label": "pneumonia, non-human animal",
#         }
#         self.disease_normalizer.db.diseases.put_item(Item=merge)
#
#         source = {
#             "src_name": "Mondo",
#             "data_license": "CC BY 4.0",
#             "data_license_url": "https://creativecommons.org/licenses/by/4.0/legalcode",  # noqa: E501
#             "version": "2022-10-11",
#             "data_license_attributes": {
#                 "non_commercial": False,
#                 "share_alike": False,
#                 "attribution": True
#             }
#         }
#         self.disease_normalizer.db.metadata.put_item(Item=source)
#
#     def _download_data():
#         """Implement abstract method"""
#         pass
#
#     def _http_download():
#         """Implement abstract method"""
#         pass
#
#     def _transform_data():
#         """Implement abstract method"""
#         pass
#
#     def _load_meta():
#         """Implement abstract method"""
#         pass
#
#
# def test_normalize_disease():
#     """Test that DiseaseIndicationBase works correctly when normalizing diseases"""
#     dib = TestDiseaseIndication()
#     norm_disease = dib._normalize_disease("mondo:0700110")
#     assert norm_disease == "mondo:0700110"
