"""ETL methods for ChemIDPlus source."""
from .base import Base
from therapy import PROJECT_ROOT
from therapy.database import Database
from therapy.schemas import Drug, NamespacePrefix, Meta, SourceName, \
    DataLicenseAttributes
from pathlib import Path
import xml.etree.ElementTree as ET
import requests
import logging


logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class ChemIDplus(Base):
    """Core ChemIDplus ETL class."""

    def __init__(self,
                 database: Database,
                 # TODO figure out data repo info
                 src_dir: str = 'ftp://ftp.nlm.nih.gov/nlmdata/.chemidlease/',  # noqa: E501
                 src_fname: str = 'CurrentChemID.xml',
                 data_path: Path = PROJECT_ROOT / 'data' / 'chemidplus'):
        """Initialize class instance.

        :param therapy.database.Database database: database instance to use
        :param str src_dir: URL to directory containing source file
        :param str src_fname: name of file as stored in src_dir
        """
        self.database = database
        self._src_dir = src_dir
        self._src_fname = src_fname
        self._added_ids = set()
        self._extract_data(data_path)
        self._transform_data()
        self._add_meta()

    def _download_data(self):
        logger.info('Downloading ChemIDplus data...')
        url = self._src_dir + self._src_fname
        out_path = PROJECT_ROOT / 'data' / 'chemidplus' / self._src_fname
        response = requests.get(url, stream=True)
        handle = open(out_path, "wb")
        for chunk in response.iter_content(chunk_size=512):
            if chunk:
                handle.write(chunk)
        version = ET.parse(out_path).getroot().attrib['date'].replace('-', '')
        out_path.rename(f'chemidplus_{version}.xml')
        logger.info('Finished downloading ChemIDplus data')

    def _extract_data(self, data_dir):
        """Acquire ChemIDplus dataset.

        :arg pathlib.Path data_dir: directory containing source data
        """
        data_dir.mkdir(exist_ok=True, parents=True)
        dir_files = list(data_dir.iterdir())
        if len(dir_files) == 0:
            self._download_data()
            dir_files = list(data_dir.iterdir())
        self._data_src = sorted(dir_files)[-1]
        self._version = self._data_src.stem.split('_')[1]

    def _transform_data(self):
        """Open dataset and prepare for loading into database."""
        tree = ET.parse(self._data_src)
        root = tree.getroot()
        with self.database.therapies.batch_writer() as batch:
            for chemical in root:
                # get concept ID
                reg_no = chemical.find('NumberList').find("CASRegistryNumber")
                if not reg_no:
                    continue
                concept_id = f'{NamespacePrefix.CASREGISTRY.value}:{reg_no.text}'  # noqa: E501
                # get names
                name_element = chemical.find('NameList')
                if name_element.find('NameOfSubstance'):
                    label = name_element.find('NameOfSubstance').text
                elif name_element.find('SystematicName'):
                    label = name_element.find('SystematicName').text
                elif 'displayName' in chemical.attrib:
                    label = chemical.attrib['displayName']
                else:
                    label = None
                trade_names = set()
                mixture_names = name_element.findall('MixtureName')
                if mixture_names:
                    trade_names = {n.text for n in mixture_names}
                alias_elms = [e for e in name_element if e.tag == 'Synonyms']
                if len(alias_elms) > 20:
                    aliases = set()
                else:
                    aliases = {e.text for e in alias_elms if e != label}
                # get other_ids and xrefs
                other_identifiers = set()
                xrefs = set()
                locator_list = chemical.find('LocatorList')
                if locator_list:
                    urls = [loc.attrib['url'] for loc in locator_list
                            if 'url' in loc.attrib]
                    for url in urls:
                        if 'drugbank' in url.lower():
                            db = f'{NamespacePrefix.DRUGBANK.value}:{url.split("/")[-1]}'  # noqa: E501
                            other_identifiers.add(db)
                        elif 'fdasis' in url.lower():
                            fda = f'{NamespacePrefix.FDA.value}:{url.split("/")[-1]}'  # noqa: E501
                            xrefs.add(fda)
                # load completed record
                record = Drug(concept_id=concept_id,
                              aliases=list(aliases),
                              trade_names=list(trade_names),
                              label=label,
                              other_identifiers=list(other_identifiers),
                              xrefs=list(xrefs))
                self._load_record(batch, record)

    def _load_record(self, batch, record: Drug):
        """Load individual record into database.

        :param batch: dynamodb batch writer
        :param therapy.schemas.Drug record: complete drug record to upload
        """
        for alias in record.aliases:
            batch.put_item(Item={
                'label_and_type': f'{alias}##alias',
                'concept_id': record.concept_id.lower(),
                'src_name': SourceName.CHEMIDPLUS.value
            })
        for trade_name in record.trade_names:
            batch.put_item(Item={
                'label_and_type': f'{trade_name.lower()}##trade_name',
                'concept_id': record.concept_id.lower(),
                'src_name': SourceName.CHEMIDPLUS.value
            })
        if record.label:
            batch.put_item(Item={
                'label_and_type': f'{record.label.lower()}##label',
                'concept_id': record.concept_id.lower(),
                'src_name': SourceName.CHEMIDPLUS.value
            })
        id_record = dict(record)
        id_record['src_name'] = SourceName.CHEMIDPLUS.value
        id_record['label_and_type'] = f'{record.concept_id.lower()}##identity'
        batch.put_item(Item=id_record)

    def _add_meta(self):
        """Add source metadata."""
        meta = Meta(data_license="custom",
                    data_license_url="https://www.nlm.nih.gov/databases/download/terms_and_conditions.html",  # noqa: E501
                    version=self._version,
                    data_url="ftp://ftp.nlm.nih.gov/nlmdata/.chemidlease/",
                    rdp_url="",
                    data_license_attributes=DataLicenseAttributes(
                        non_commercial=False,
                        share_alike=False,
                        attribution=True
                    ))
        item = dict(meta)
        item['src_name'] = SourceName.CHEMIDPLUS.value
        self.database.metadata.put_item(Item=item)
