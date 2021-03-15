"""The VICC library for normalizing therapies."""
from pathlib import Path
import logging

PROJECT_ROOT = Path(__file__).resolve().parents[0]
logging.basicConfig(
    filename='therapy.log',
    format='[%(asctime)s] %(levelname)s : %(message)s')
logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)

# TODO: Fix so that we don't have to change in setup.cfg
__version__ = "0.2.8"


class DownloadException(Exception):
    """Exception for failures relating to source file downloads."""

    def __init__(self, *args, **kwargs):
        """Initialize exception."""
        super().__init__(*args, **kwargs)


from therapy.schemas import SourceName, NamespacePrefix, SourceIDAfterNamespace, ProhibitedSources  # noqa: E402, E501
# Sources we import directly
SOURCES = {source.value.lower(): source.value
           for source in SourceName.__members__.values()}

# use to fetch source name from schema based on concept id namespace
# e.g. {'chembl': 'ChEMBL'}
PREFIX_LOOKUP = {v.value: SourceName[k].value
                 for k, v in NamespacePrefix.__members__.items()
                 if k in SourceName.__members__.keys()}

# use to generate namespace prefix from source ID value
# e.g. {'q': 'wikidata'}
NAMESPACE_LOOKUP = {v.value.lower(): NamespacePrefix[k].value
                    for k, v in SourceIDAfterNamespace.__members__.items()
                    if v.value != ''}

# Sources that are not allowed in normalize endpoint due to license
PROHIBITED_SOURCES = {s.value.lower()
                      for s in ProhibitedSources.__members__.values()}

# Sources that are allowed in normalize endpoint
ACCEPTED_SOURCES = SOURCES.keys() - PROHIBITED_SOURCES

# Sources that we import directly
OTHER_IDENTIFIERS = {source for source in SourceName.__members__}

# Sources that are found in data from imported sources
XREFS = {source for source in NamespacePrefix.__members__} - OTHER_IDENTIFIERS

from therapy.etl import ChEMBL, Wikidata, DrugBank, NCIt, ChemIDplus, RxNorm  # noqa: F401, E402, E501
# used to get source class name from string
SOURCES_CLASS = \
    {s.value.lower(): eval(s.value) for s in SourceName.__members__.values()}
