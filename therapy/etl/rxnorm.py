"""This module defines the RxNorm ETL methods."""
from .base import Base
from therapy import PROJECT_ROOT
from therapy.database import Database
from therapy.schemas import SourceName, NamespacePrefix, Meta, Drug
import csv
import requests
from bs4 import BeautifulSoup
import datetime
import logging
import botocore

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class RxNorm(Base):
    """Extract, transform, and load the RxNorm source."""

    def __init__(self,
                 database: Database,
                 data_url='https://www.nlm.nih.gov/research/umls/'
                          'rxnorm/docs/rxnormfiles.html'):
        """Initialize the RxNorm ETL class.

        :param Database database: Access to DynamoDB.
        """
        self._other_id_srcs = {src for src in SourceName.__members__}
        self._xref_srcs = {src for src in NamespacePrefix.__members__}
        self._data_url = data_url
        self.database = database
        self._load_data()

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
            else:
                # Want to use the most recent version
                if not str(sorted(rxn_files)[-1]).endswith('.RRF'):
                    self._download_data(rxn_dir)
            self._data_src = sorted(rxn_files)[-1]
            if not str(self._data_src).endswith('.RRF'):
                raise FileNotFoundError("Could not find RxNorm RFF file.")
            try:
                files = [fn for fn in rxn_dir.iterdir()
                         if fn.name.startswith('rxnorm_')]
                self._data_src = sorted(files)[-1]
                self._version = \
                    str(self._data_src).split('_')[-1].split('.')[0]
            except IndexError:
                raise FileNotFoundError

    def _download_data(self, rxn_dir, *args, **kwargs):
        """Download RxNorm data file."""
        logger.info('Downloading RxNorm...')
        self._get_file_url_version()
        # TODO

    def _get_file_url_version(self):
        """Get RxNorm's latest file and version."""
        r = requests.get(self._data_url)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            releases_soup = soup.find('table', {"class": "current"})
            self._data_file_url =\
                releases_soup.find_all('td')[0].find_all('a')[0]['href']
            version = self._data_file_url.split('_')[-1].split('.')[0]
            self._version = datetime.datetime.strptime(
                version, '%m%d%Y').strftime('%Y%m%d')

    def _transform_data(self):
        """Transform the RxNorm source."""
        with open(self._data_src) as f:
            rff_data = csv.reader(f, delimiter='|')
            data = dict()
            for row in rff_data:
                concept_id = f"{NamespacePrefix.RXNORM.value}:{row[0]}"
                if concept_id in data.keys():
                    params = data[concept_id]
                    self._add_str_field(params, row)
                    self._add_other_ids_xrefs(params, row)
                else:
                    # Create new gene record
                    params = dict()
                    params['concept_id'] = concept_id
                    self._add_str_field(params, row)
                    self._add_other_ids_xrefs(params, row)
                    data[concept_id] = params

            with self.database.therapies.batch_writer() as batch:
                for key, value in data.items():
                    params = Drug(
                        concept_id=value['concept_id'],
                        label=value['label'] if 'label' in value else None,
                        aliases=value['aliases'] if 'aliases' in value else [],
                        other_identifiers=value[
                            'other_identifiers'] if 'other_identifiers'
                                                    in value else [],
                        xrefs=value['xrefs'] if 'xrefs' in value else [],
                        trade_names=value['trade_names'] if 'trade_names'
                                                            in value else []
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

        self._load_label_types(params, batch)
        params['src_name'] = SourceName.RXNORM.value
        params['label_and_type'] = f"{params['concept_id'].lower()}##identity"
        batch.put_item(Item=params)

    def _load_label_types(self, params, batch):
        """Insert aliases, trade_names, and label data into the database.

        :param dict params: A transformed therapy record.
        :param BatchWriter batch: Object to write data to DynamoDB
        """
        if 'aliases' in params:
            self._load_label_type(params, batch, 'alias', 'aliases')
        if 'trade_names' in params:
            self._load_label_type(params, batch, 'trade_name', 'trade_names')
        if 'label' in params:
            self._load_label_type(params, batch, 'label', 'label')

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
                if len((t['label_and_type']).encode('utf-8')) > 1024:
                    logger.info(f"{params['concept_id']}: An error occurred "
                                "(ValidationException) when calling the "
                                "BatchWriteItem operation: Hash primary key "
                                "values must be under 2048 bytes, and range"
                                " primary key values must be under 1024"
                                " bytes.")

    def _add_str_field(self, params, row):
        """Differentiate STR field.

        :param dict params: A transformed therapy record.
        :param list row: A row in the RxNorm data file.
        """
        term = row[14]
        term_type = row[12]

        # TODO: Include Foreign Syn, Tall Man Syn, British Syn?
        # Foreign Syn, Designated Alias, Designated Syn, Tall Man Syn,
        # British Syn, Full form descriptor?, Clinical drug name in abbreviated
        # format, Clinical drug name in concatenated format,
        # Clinical drug name in delimited format
        aliases = ['FSY', 'SYN', 'SY', 'TMSY', 'SYGB', 'FN', 'CDA', 'CDC',
                   'CDD']

        # ['AB' 'BPCK' 'CE' 'DEV' 'DF' 'DFG' 'DP'
        #  'DSV' 'GPCK' 'IN' 'MH' 'MIN' 'MS' 'MTH_RXN_BD'
        #  'MTH_RXN_CD' 'MTH_RXN_CDC' 'MTH_RXN_DP' 'N1' 'NM' 'PCE' 'PIN' 'PM'
        #  'PSN' 'PTGB' 'RXN_IN' 'RXN_PT' 'SBD' 'SBDC' 'SBDF' 'SBDG' 'SC' 'SCD'
        #  'SCDC' 'SCDF' 'SCDG' 'SU' ]

        # Fully-specified drug brand name that can be prescribed
        # Fully-specified drug brand name that can not be prescribed,
        trade_names = ['BD', 'BN']

        # Designated Preferred Name, Preferred Entry Term, Clinical Drug,
        # Entry Term,
        other_labels = ['PT', 'PEP', 'CD', 'ET']

        if term_type == 'GN':
            # Generic Drug Name
            params['label'] = term
        if term_type in other_labels:
            params['label'] = term
        elif term_type in aliases:
            self._add_term(params, term, 'aliases')
        elif term_type in trade_names:
            self._add_term(params, term, 'trade_names')

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
            if other_id_xref in self._other_id_srcs:
                other_id =\
                    f"{NamespacePrefix[other_id_xref].value}:" \
                    f"{row[13].lower()}"
                if other_id != params['concept_id']:
                    # Sometimes concept_id is included in the source field
                    self._add_term(params, other_id, 'other_identifiers')
            elif other_id_xref in self._xref_srcs:
                xref = f"{NamespacePrefix[other_id_xref].value}:" \
                       f"{row[13].lower()}"
                self._add_term(params, xref, 'xrefs')
            else:
                logger.info(f"{other_id_xref} not in NameSpacePrefix.")

    def _add_meta(self):
        """Add RxNorm metadata."""
        meta = Meta(data_license='NLM',
                    data_license_url='https://www.nlm.nih.gov/research/umls/'
                                     'rxnorm/docs/termsofservice.html',
                    version=self._version,
                    data_url=self._data_url,
                    rdp_url=None,
                    data_license_attributes={
                        'non_commercial': False,  # TODO: Check
                        'share_alike': False,  # TODO: Check
                        'attribution': True  # TODO: Check
                    })
        params = dict(meta)
        params['src_name'] = SourceName.RXNORM.value
        self.database.metadata.put_item(Item=params)

    def _load_data(self):
        """Load the RxNorm source into database."""
        self._extract_data()
        self._add_meta()
        self._transform_data()
