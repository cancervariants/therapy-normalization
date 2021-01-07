"""ETL methods for ChemIDPlus source."""
from .base import Base
from therapy import PROJECT_ROOT
from therapy.database import Database
from therapy.schemas import Drug, NamespacePrefix, Meta, SourceName, \
    DataLicenseAttributes
from pathlib import Path
import xml.etree.ElementTree as ET


class ChemIDplus(Base):
    """Core ChemIDplus ETL class."""

    def __init__(self,
                 database: Database,
                 # TODO figure out data repo info
                 src_dir: str = 'ftp://ftp.nlm.nih.gov/nlmdata/.chemidlease/',  # noqa: E501
                 src_fname: str = None,
                 data_path: Path = PROJECT_ROOT / 'data' / 'chemidplus'):
        """Initialize class instance.

        :param therapy.database.Database database: database instance to use
        :param str src_dir: URL to directory containing source file
        :param str src_fname: name of file as stored in src_dir
        """
        self.database = database
        self.src_dir = src_dir
        self.src_fname = src_fname
        self._extract_data(data_path)
        self._transform_data()

    def _download_data(self):
        raise NotImplementedError

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

                name_element = chemical.find('NameList')
                label = name_element.get('NameOfSubstance').text
                aliases = [e.text for e in name_element if e.tag == 'Synonyms']
                id_element = chemical.find('NumberList')
                concept_id = f'{NamespacePrefix.CASREGISTRY}:{id_element.find("CASRegistryNumber").text}'  # noqa: E501

                record = Drug(concept_id=concept_id,
                              aliases=aliases,
                              label=label,
                              other_identifiers=[],
                              xrefs=[])
                self._load_record(batch, record)

    def _load_record(self, batch, record: Drug):
        """Load individual record into database."""
        pass

    def _load_meta(self):
        """Add source metadata."""
        meta = Meta(data_license="custom",
                    data_license_url="https://www.nlm.nih.gov/databases/download/terms_and_conditions.html",  # noqa: E501
                    version="20200107",
                    data_url="ftp://ftp.nlm.nih.gov/nlmdata/.chemidlease/",
                    rdp_url="",
                    data_license_attributes=DataLicenseAttributes(
                        non_commercial=False,
                        share_alike=False,
                        attribution=True
                    ))
        item = dict(meta)
        item['src_name'] = SourceName.CHEMIDPLUS
        self.database.metadata.put_item(Item=item)
