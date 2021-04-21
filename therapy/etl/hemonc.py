"""Provide ETL methods for HemOnc.org data."""
from therapy import DownloadException, PROJECT_ROOT
from therapy.etl.base import Base
from therapy.schemas import NamespacePrefix, SourceMeta, SourceName, \
    ApprovalStatus
from disease.query import QueryHandler as DiseaseNormalizer
from pathlib import Path
from typing import List
import csv
import logging


logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class HemOnc(Base):
    """Docstring"""

    def __init__(self, database, data_path: Path = PROJECT_ROOT / 'data'):
        """Initialize HemOnc instance.

        :param therapy.database.Database database: application database
        :param Path data_path: path to normalizer data directory
        """
        super().__init__(database, data_path)
        self.disease_normalizer = DiseaseNormalizer(self.database.endpoint_url)

    def perform_etl(self) -> List[str]:
        """Public-facing method to begin ETL procedures on given data.

        Returned concept IDs can be passed to Merge method for computing
        merged concepts.

        :return: list of concept IDs which were successfully processed and
            uploaded.
        """
        self._extract_data()
        self._load_meta()
        self._transform_data()
        return self._added_ids

    def _download_data(self):
        """Download HemOnc.org source data.

        Raises download exception for now -- HTTP authorization may be
        possible?
        """
        raise DownloadException("No download for HemOnc data available.")

    def _extract_data(self):
        """Get source files from data directory."""
        self._src_data_dir.mkdir(exist_ok=True, parents=True)
        src_file_prefix = 'hemonc_'
        self._src_files = []
        for item_type in ('concepts', 'rels', 'synonyms'):
            src_file_prefix = f'hemonc_{item_type}_'
            dir_files = [f for f in self._src_data_dir.iterdir()
                         if f.name.startswith(src_file_prefix)]
            if len(dir_files) == 0:
                self._download_data(item_type)
                dir_files = [f for f in self._src_data_dir.iterdir()
                             if f.name.startswith(src_file_prefix)]
            self._src_files.append(sorted(dir_files, reverse=True)[0])
        self._version = self._src_files[0].stem.split('_', 2)[-1]

    def _load_meta(self):
        """Add HemOnc metadata."""
        meta = {
            'data_license': 'CC BY 4.0',
            'data_license_url': 'https://creativecommons.org/licenses/by/4.0/legalcode',  # noqa: E501
            'version': self._version,
            'data_url': 'https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/9CY9C6',  # noqa: E501
            'rdp_url': None,
            'data_license_attributes': {
                'non_commercial': False,
                'share_alike': True,
                'attribution': False,
            },
        }
        assert SourceMeta(**meta)
        meta['src_name'] = SourceName.HEMONC.value
        self.database.metadata.put_item(Item=meta)

    def _transform_data(self):
        """Prepare dataset for loading into normalizer database."""
        concepts = {}  # hemonc id -> record
        brand_names = {}  # hemonc id -> brand name
        conditions = {}  # hemonc id -> condition name

        concepts_file = open(self._src_files[0], 'r')
        concepts_reader = csv.reader(concepts_file)
        next(concepts_reader)  # skip header
        for row in concepts_reader:
            if row[6]:
                continue

            row_type = row[2]
            if row_type == 'Component':
                concept_id = f'{NamespacePrefix.HEMONC.value}:{row[3]}'
                concepts[row[3]] = {
                    'concept_id': concept_id,
                    'label': row[0],
                    'trade_names': [],
                    'aliases': [],
                    'other_identifiers': [],
                }
            elif row_type == 'Brand Name':
                brand_names[row[3]] = row[0]
            elif row_type == 'Condition':
                conditions[row[3]] = row[0]
        concepts_file.close()

        rels_file = open(self._src_files[1], 'r')
        rels_reader = csv.reader(rels_file)
        next(rels_reader)
        for row in rels_reader:
            rel_type = row[4]
            record = concepts.get(row[0])
            if record is None:
                continue  # skip non-drug items
            if rel_type == "Maps to":
                src_raw = row[3]
                if src_raw == "RxNorm":
                    other_id = f'{NamespacePrefix.RXNORM.value}:{row[1]}'
                    record['other_identifiers'].append(other_id)
            elif rel_type == "Was FDA approved yr":
                status = ApprovalStatus.APPROVED
                record['approval_status'] = status
            elif rel_type == "Has brand name":
                record['trade_names'].append(brand_names[row[1]])
            elif rel_type == "Has FDA indication":
                disease_raw = conditions[row[1]]
                response = self.disease_normalizer.search_groups(disease_raw)
                if response['match_type'] > 0:
                    disease = response['value_object_descriptor']['value']['id']  # noqa: E501
                    if 'fda_indication' in record:
                        record['fda_indication'].append(disease)
                    else:
                        record['fda_indication'] = [disease]
                else:
                    logger.warning(f'Normalization of condition id: {row[1]}'
                                   f' , {disease_raw}, failed.')
        rels_file.close()

        synonyms_file = open(self._src_files[2], 'r')
        synonyms_reader = csv.reader(synonyms_file)
        next(synonyms_reader)
        for row in synonyms_reader:
            concept_code = row[1]
            if concept_code in concepts:
                concept = concepts[concept_code]
                alias = row[0]
                if alias != concept.get('label'):
                    concepts[concept_code]['aliases'].append(row[0])
        synonyms_file.close()

        # load therapy for each in concepts
        for therapy in concepts.values():
            self._load_therapy(therapy)
