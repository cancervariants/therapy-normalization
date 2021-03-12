"""This module defines the RxNorm ETL methods.

"This product uses publicly available data courtesy of the U.S. National
Library of Medicine (NLM), National Institutes of Health, Department of Health
 and Human Services; NLM is not responsible for the product and does not
 endorse or recommend this or any other product."
"""
from .base import Base
from therapy import PROJECT_ROOT, DownloadException, OTHER_IDENTIFIERS, XREFS
import therapy
from therapy.database import Database
from therapy.schemas import SourceName, NamespacePrefix, Meta, Drug, \
    ApprovalStatus
import csv
import datetime
import logging
import botocore
from typing import List
from os import environ, remove
import subprocess
import shutil
import zipfile
import re
import yaml
from pathlib import Path

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)

# Designated Alias, Designated Syn, Tall Man Syn, Machine permutation
# Generic Drug Name, Designated Preferred Name, Preferred Entry Term,
# Clinical Drug, Entry Term, Rxnorm Preferred
ALIASES = ['SYN', 'SY', 'TMSY', 'PM',
           'GN', 'PT', 'PEP', 'CD', 'ET', 'RXN_PT']

# Fully-specified drug brand name that can be prescribed
# Fully-specified drug brand name that can not be prescribed,
# Semantic branded drug
TRADE_NAMES = ['BD', 'BN', 'SBD']

# Allowed rxnorm xrefs that have Source Level Restriction 0 or 1
RXNORM_XREFS = ['ATC', 'CVX', 'DRUGBANK', 'MMSL', 'MSH', 'MTHCMSFRF', 'MTHSPL',
                'RXNORM', 'USP', 'VANDF']


class RxNorm(Base):
    """Extract, transform, and load the RxNorm source."""

    def __init__(self,
                 database: Database,
                 data_url='https://www.nlm.nih.gov/research/umls/'
                          'rxnorm/docs/rxnormfiles.html'):
        """Initialize the RxNorm ETL class.

        :param Database database: Access to DynamoDB.
        :param str data_url: URL to RxNorm data files.
        """
        self._data_url = data_url
        self.database = database
        self._added_ids = []

    def perform_etl(self) -> List[str]:
        """Load the RxNorm source into database.

        :return: List of concept IDs which were successfully processed and
            uploaded.
        """
        self._extract_data()
        self._load_meta()
        self._transform_data()
        return self._added_ids

    def _extract_data(self, *args, **kwargs):
        """Extract data from the RxNorm source."""
        logger.info('Extracting RxNorm...')
        if 'data_path' in kwargs:
            self._data_src = kwargs['data_path']
        else:
            rxn_dir = PROJECT_ROOT / 'data' / 'rxnorm'
            rxn_dir.mkdir(exist_ok=True, parents=True)
            rxn_files = list(rxn_dir.iterdir())
            if len(rxn_files) == 0:
                self._download_data(rxn_dir)

            # file might already exist
            files = sorted([fn for fn in rxn_dir.iterdir() if fn.name.
                           startswith('rxnorm_') and fn.name.endswith('.RRF')],
                           reverse=True)
            file_found = False
            for file in files:
                version = str(file).split('_')[-1].split('.')[0]
                try:
                    datetime.datetime.strptime(version, '%Y%m%d')
                    self._data_src = file
                    self._version = version
                    file_found = True
                    break
                except ValueError:
                    pass
            if not file_found:
                self._download_data(rxn_dir)
            self._create_drug_form_yaml(rxn_dir)
        logger.info('Successfully extracted RxNorm.')

    def _download_data(self, rxn_dir):
        """Download RxNorm data file.

        :param Path rxn_dir: Path to RxNorm data directory.
        """
        logger.info('Downloading RxNorm...')
        if 'RXNORM_API_KEY' in environ.keys():
            uri = 'https://download.nlm.nih.gov/umls/kss/' \
                  'rxnorm/RxNorm_full_current.zip'

            rxnorm_path = str(Path(therapy.__file__).resolve().parents[0] / 'data' / 'rxnorm' / 'RxNorm_full_current.zip')  # noqa: E501
            environ['RXNORM_PATH'] = rxnorm_path

            # Source:
            # https://documentation.uts.nlm.nih.gov/automating-downloads.html
            subprocess.call(['bash', f'{PROJECT_ROOT}/etl/'
                                     f'rxnorm_download.sh', uri])

            with zipfile.ZipFile(rxnorm_path, 'r') as zf:
                zf.extractall(rxn_dir)

            remove(rxnorm_path)
            shutil.rmtree(rxn_dir / 'prescribe')
            shutil.rmtree(rxn_dir / 'scripts')

            readme = sorted([fn for fn in rxn_dir.iterdir() if fn.name.
                            startswith('Readme')])[0]

            # get version
            version = str(readme).split('/')[-1].split('.')[0].split('_')[-1]

            self._version = datetime.datetime.strptime(
                version, '%m%d%Y').strftime('%Y%m%d')
            remove(readme)

            temp_file = rxn_dir / 'rrf' / 'RXNCONSO.RRF'
            self._data_src = rxn_dir / f"rxnorm_{self._version}.RRF"
            shutil.move(temp_file, self._data_src)
            shutil.rmtree(rxn_dir / 'rrf')
        else:
            logger.error('Could not find RXNORM_API_KEY in environment '
                         'variables.')
            raise DownloadException("RXNORM_API_KEY not found.")

    def _create_drug_form_yaml(self, rxn_dir):
        """Create a YAML file containing RxNorm drug form values.

        :param Path rxn_dir: Path to RxNorm data directory.
        """
        self._drug_forms_file = rxn_dir / 'drug_forms.yaml'
        if not self._drug_forms_file.exists():
            dfs = list()
            with open(self._data_src) as f:
                data = csv.reader(f, delimiter='|')
                for row in data:
                    if row[12] == 'DF' and row[11] == 'RXNORM':
                        if row[14] not in dfs:
                            dfs.append(row[14])

            with open(self._drug_forms_file, 'w') as file:
                yaml.dump(dfs, file)

    def _transform_data(self):
        """Transform the RxNorm source."""
        with open(self._drug_forms_file, 'r') as file:
            drug_forms = yaml.safe_load(file)

        with open(self._data_src) as f:
            rff_data = csv.reader(f, delimiter='|')
            ingredient_brands = dict()  # Link ingredient to brand
            precise_ingredient = dict()  # Link precise ingredient to get brand
            data = dict()  # Transformed therapy records
            sbdfs = dict()  # Link ingredient to brand
            brands = dict()  # Get RXNORM|BN to concept_id
            for row in rff_data:
                if row[11] in RXNORM_XREFS:
                    concept_id = f"{NamespacePrefix.RXNORM.value}:{row[0]}"
                    if row[12] == 'BN' and row[11] == 'RXNORM':
                        brands[row[14]] = concept_id
                    if row[12] == 'SBDC' and row[11] == 'RXNORM':
                        # Semantic Branded Drug Component
                        self._get_brands(row, ingredient_brands)
                    else:
                        if concept_id not in data.keys():
                            params = dict()
                            params['concept_id'] = concept_id
                            self._add_str_field(params, row,
                                                precise_ingredient,
                                                drug_forms, sbdfs)
                            self._add_other_ids_xrefs(params, row)
                            data[concept_id] = params
                        else:
                            # Concept already created
                            params = data[concept_id]
                            self._add_str_field(params, row,
                                                precise_ingredient,
                                                drug_forms, sbdfs)
                            self._add_other_ids_xrefs(params, row)

            with self.database.therapies.batch_writer() as batch:
                for key, value in data.items():
                    if 'label' in value:
                        self._get_trade_names(value, precise_ingredient,
                                              ingredient_brands, sbdfs)
                        self._load_brand_concepts(value, brands, batch)

                        params = Drug(
                            concept_id=value['concept_id'],
                            label=value['label'] if 'label' in value else None,
                            approval_status=value[
                                'approval_status'] if 'approval_status'
                                                      in value else None,
                            aliases=value['aliases'] if 'aliases' in
                                                        value else [],
                            other_identifiers=value[
                                'other_identifiers'] if 'other_identifiers'
                                                        in value else [],
                            xrefs=value['xrefs'] if 'xrefs' in value else [],
                            trade_names=value['trade_names'] if 'trade_names'
                                                                in value else []  # noqa: E501
                        )
                        self._load_therapy(params, batch)

    def _load_therapy(self, params, batch):
        """Load therapy record into the database.

        :param Drug params: A Therapy object.
        :param BatchWriter batch: Object to write data to DynamoDB.
        """
        params = dict(params)
        for label_type in ['label', 'aliases', 'other_identifiers', 'xrefs',
                           'trade_names']:
            if not params[label_type]:
                del params[label_type]

        for attr in ['trade_names', 'aliases']:
            # Remove duplicates
            if attr in params:
                if 'label' in params:
                    params[attr] = list(set(params[attr]) - {params['label']})

                if attr == 'aliases' and 'trade_names' in params:
                    params[attr] = \
                        list(set(params[attr]) - set(params['trade_names']))

                # 20 cutoff for aliases / trade names
                if len(params[attr]) > 20:
                    logger.debug(f"{params['concept_id']} has more than 20"
                                 f" {attr}.")
                    del params[attr]

        self._load_label_types(params, batch)
        params['src_name'] = SourceName.RXNORM.value
        params['label_and_type'] = f"{params['concept_id'].lower()}##identity"
        try:
            batch.put_item(Item=params)
            self._added_ids.append(params['concept_id'])
        except botocore.exceptions.ClientError:
            if (len((params['label_and_type']).encode('utf-8')) >= 2048) or \
                    (len((params['concept_id']).encode('utf-8')) >= 1024):
                logger.info(f"{params['concept_id']}: An error occurred "
                            "(ValidationException) when calling the "
                            "BatchWriteItem operation: Hash primary key "
                            "values must be under 2048 bytes, and range"
                            " primary key values must be under 1024"
                            " bytes.")

    def _load_label_types(self, params, batch):
        """Insert aliases, trade_names, and label data into the database.

        :param dict params: A transformed therapy record.
        :param BatchWriter batch: Object to write data to DynamoDB
        """
        if 'label' in params:
            self._load_label_type(params, batch, 'label', 'label')
        if 'trade_names' in params:
            self._load_label_type(params, batch, 'trade_name', 'trade_names')
        if 'aliases' in params:
            self._load_label_type(params, batch, 'alias', 'aliases')
        if 'other_identifiers' in params:
            self._load_label_type(params, batch, 'other_id',
                                  'other_identifiers')

    def _load_label_type(self, params, batch, label_type_sing, label_type_pl):
        """Insert alias, trade_name, or label data into the database.

        :param dict params: A transformed therapy record.
        :param BatchWriter batch: Object to write data to DynamoDB
        :param str label_type_sing: The singular label type
        :param str label_type_pl: The plural label type
        """
        if isinstance(params[label_type_pl], list):
            terms = {t.casefold(): t for t in params[label_type_pl]}.values()
        else:
            terms = [params[label_type_pl]]
        for t in terms:
            t = {
                'label_and_type': f"{t.lower()}##{label_type_sing}",
                'concept_id': f"{params['concept_id'].lower()}",
                'src_name': SourceName.RXNORM.value
            }
            try:
                batch.put_item(Item=t)
            except botocore.exceptions.ClientError:
                if (len((t['label_and_type']).encode('utf-8')) >= 2048) or \
                        (len((t['concept_id']).encode('utf-8')) >= 1024):
                    logger.info(f"{params['concept_id']}: An error occurred "
                                "(ValidationException) when calling the "
                                "BatchWriteItem operation: Hash primary key "
                                "values must be under 2048 bytes, and range"
                                " primary key values must be under 1024"
                                " bytes.")
        return terms

    def _get_brands(self, row, ingredient_brands):
        """Add ingredient and brand to ingredient_brands.

        :param list row: A row in the RxNorm data file.
        :param dict ingredient_brands: Store brands for each ingredient
        """
        # SBDC: Ingredient(s) + Strength + [Brand Name]
        term = row[14]
        ingredients_brand = \
            re.sub(r"(\d*)(\d*\.)?\d+ (MG|UNT|ML)?(/(ML|HR|MG))?",
                   "", term)
        brand = term.split('[')[-1].split(']')[0]
        ingredients = ingredients_brand.replace(f"[{brand}]", '')
        if '/' in ingredients:
            ingredients = ingredients.split('/')
            for ingredient in ingredients:
                self._add_term(ingredient_brands, brand,
                               ingredient.strip())
        else:
            self._add_term(ingredient_brands, brand,
                           ingredients.strip())

    def _get_trade_names(self, value, precise_ingredient, ingredient_brands,
                         sbdfs):
        """Get trade names for a given ingredient.

        :param dict value: Therapy attributes
        :param dict precise_ingredient: Brand names for precise ingredient
        :param dict ingredient_brands: Brand names for ingredient
        :param dict sbdfs: Brand names for ingredient from SBDF row
        """
        record_label = value['label'].lower()
        labels = [record_label]

        if 'PIN' in value and value['PIN'] \
                in precise_ingredient:
            for pin in precise_ingredient[value['PIN']]:
                labels.append(pin.lower())

        for label in labels:
            trade_names = \
                [val for key, val in ingredient_brands.items()
                 if label == key.lower()]
            trade_names = {val for sublist in trade_names
                           for val in sublist}
            for tn in trade_names:
                self._add_term(value, tn, 'trade_names')

        if record_label in sbdfs:
            for tn in sbdfs[record_label]:
                self._add_term(value, tn, 'trade_names')

    def _load_brand_concepts(self, value, brands, batch):
        """Connect brand names to a concept and load into the database.

        :params dict value: A transformed therapy record
        :params dict brands: Connects brand names to concept records
        :param BatchWriter batch: Object to write data to DynamoDB.
        """
        if 'trade_names' in value:
            for tn in value['trade_names']:
                if brands.get(tn):
                    batch.put_item(Item={
                        'label_and_type':
                            f"{brands.get(tn)}##rx_brand",
                        'concept_id': value['concept_id'],
                        'src_name': SourceName.RXNORM.value
                    })

    def _add_str_field(self, params, row, precise_ingredient, drug_forms,
                       sbdfs):
        """Differentiate STR field.

        :param dict params: A transformed therapy record.
        :param list row: A row in the RxNorm data file.
        :param dict precise_ingredient: Precise ingredient information
        :param list drug_forms: RxNorm Drug Form values
        :param dict precise_ingredient: Brand names for precise ingredient
        """
        term = row[14]
        term_type = row[12]
        source = row[11]

        if (term_type == 'IN' or term_type == 'PIN') and source == 'RXNORM':
            params['label'] = term
            if row[17] == '4096':
                params['approval_status'] = ApprovalStatus.APPROVED.value
        elif term_type in ALIASES:
            self._add_term(params, term, 'aliases')
        elif term_type in TRADE_NAMES:
            self._add_term(params, term, 'trade_names')

        if source == 'RXNORM':
            if term_type == 'SBDF':
                brand = term.split('[')[-1].split(']')[0]
                ingredient_strength = term.replace(f"[{brand}]", '')
                for df in drug_forms:
                    if df in ingredient_strength:
                        ingredient = \
                            ingredient_strength.replace(df, '').strip()
                        self._add_term(sbdfs, brand, ingredient.lower())
                        break

        if source == 'MSH':
            if term_type == 'MH':
                # Get ID for accessing precise ingredient
                params['PIN'] = row[13]
            elif term_type == 'PEP':
                self._add_term(precise_ingredient, term, row[13])

    def _add_term(self, params, term, label_type):
        """Add a single term to a therapy record in an associated field.

        :param dict params: A transformed therapy record.
        :param str term: The term to be added
        :param str label_type: The type of term
        """
        if label_type in params and params[label_type]:
            if term not in params[label_type]:
                params[label_type].append(term)
        else:
            params[label_type] = [term]

    def _add_other_ids_xrefs(self, params, row):
        """Add other identifier or xref to therapy.

        :param dict params: A transformed therapy record.
        :param list row: A row in the RxNorm data file.
        """
        if row[11]:
            other_id_xref = row[11].upper()
            if other_id_xref in OTHER_IDENTIFIERS:
                source_id =\
                    f"{NamespacePrefix[other_id_xref].value}:{row[13]}"
                if source_id != params['concept_id']:
                    # Sometimes concept_id is included in the source field
                    self._add_term(params, source_id, 'other_identifiers')
            elif other_id_xref in XREFS:
                source_id = f"{NamespacePrefix[other_id_xref].value}:{row[13]}"
                self._add_term(params, source_id, 'xrefs')
            else:
                logger.info(f"{other_id_xref} not in NameSpacePrefix.")

    def _load_meta(self):
        """Add RxNorm metadata."""
        meta = Meta(data_license='UMLS Metathesaurus',
                    data_license_url='https://www.nlm.nih.gov/research/umls/'
                                     'rxnorm/docs/termsofservice.html',
                    version=self._version,
                    data_url=self._data_url,
                    rdp_url=None,
                    data_license_attributes={
                        'non_commercial': False,
                        'share_alike': False,
                        'attribution': True
                    })
        params = dict(meta)
        params['src_name'] = SourceName.RXNORM.value
        self.database.metadata.put_item(Item=params)
