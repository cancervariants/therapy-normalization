"""The VICC library for normalizing therapies."""
from pathlib import Path
import logging

from .version import __version__  # noqa: F401

PROJECT_ROOT = Path(__file__).resolve().parents[0]
logging.basicConfig(
    filename="therapy.log",
    format="[%(asctime)s] - %(name)s - %(levelname)s : %(message)s")
logger = logging.getLogger("therapy")
logger.setLevel(logging.DEBUG)


class DownloadException(Exception):
    """Exception for failures relating to source file downloads."""

    def __init__(self, *args, **kwargs):
        """Initialize exception."""
        super().__init__(*args, **kwargs)


from therapy.schemas import SourceName, NamespacePrefix, SourceIDAfterNamespace, ItemTypes  # noqa: E402, E501, I100, I202
ITEM_TYPES = {k.lower(): v.value for k, v in ItemTypes.__members__.items()}

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
                    if v.value != ""}

# Sources that we import directly
XREF_SOURCES = {source for source in SourceName.__members__}

# Sources that are found in data from imported sources
ASSOC_WITH_SOURCES = {source for source in NamespacePrefix.__members__} - XREF_SOURCES  # noqa: E501

from therapy.etl import ChEMBL, Wikidata, DrugBank, NCIt, ChemIDplus, RxNorm, HemOnc, GuideToPHARMACOLOGY  # noqa: F401, E402, E501, I202
# used to get source class name from string
SOURCES_CLASS = \
    {s.value.lower(): eval(s.value) for s in SourceName.__members__.values()}
