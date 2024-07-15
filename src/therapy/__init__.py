"""The VICC library for normalizing therapies."""

import re
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

APP_ROOT: Path = Path(__file__).resolve().parents[0]


try:
    __version__ = version("thera-py")
except PackageNotFoundError:
    __version__ = "unknown"
finally:
    del version, PackageNotFoundError


from therapy.schemas import (  # noqa: E402
    NamespacePrefix,
    RefType,
    SourceName,
)

# map plural to singular form
# eg {"label": "label", "trade_names": "trade_name"}
# key is the field name in the record object, value is the item_type value
# in reference objects
ITEM_TYPES = {k.lower(): v.value for k, v in RefType.__members__.items()}

# Sources we import directly
SOURCES = {
    source.value.lower(): source.value for source in SourceName.__members__.values()
}

# use to fetch source name from schema based on concept id namespace
# e.g. {'chembl': 'ChEMBL'}
PREFIX_LOOKUP = {
    v.value: SourceName[k].value
    for k, v in NamespacePrefix.__members__.items()
    if k in SourceName.__members__
}

# Namespace LUI patterns. Use for namespace inference.
NAMESPACE_LUIS = (
    (re.compile(r"^CHEMBL\d+$", re.IGNORECASE), SourceName.CHEMBL.value),
    (re.compile(r"^\d+\-\d+\-\d+$", re.IGNORECASE), SourceName.CHEMIDPLUS.value),
    (re.compile(r"^(Q|P)\d+$", re.IGNORECASE), SourceName.WIKIDATA.value),
    (re.compile(r"^C\d+$", re.IGNORECASE), SourceName.NCIT.value),
    (re.compile(r"^DB\d{5}$", re.IGNORECASE), SourceName.DRUGBANK.value),
    (re.compile(r"^(A?NDA)(\d+)$", re.IGNORECASE), SourceName.DRUGSATFDA.value),
)

# Sources that we import directly
XREF_SOURCES = set(SourceName.__members__)

# Sources that are found in data from imported sources
ASSOC_WITH_SOURCES = set(NamespacePrefix.__members__) - XREF_SOURCES
