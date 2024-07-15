"""ETL methods for ChemIDPlus® source.

Courtesy of the U.S. National Library of Medicine.
"""

import re
import xml.etree.ElementTree as ElTree
from collections.abc import Generator
from pathlib import Path

from rich.console import Console

from therapy.etl.base import Base
from therapy.schemas import (
    DataLicenseAttributes,
    NamespacePrefix,
    RecordParams,
    SourceMeta,
    SourceName,
)

TAGS_REGEX = r" \[.*\]"


class ChemIDplus(Base):
    """Class for ChemIDplus ETL methods."""

    @staticmethod
    def parse_xml(path: Path, tag: str) -> Generator:
        """Parse XML file and retrieve elements with matching tag value.
        :param Path path: path to XML file
        :param str tag: XML tag
        :return: generator yielding elements of corresponding tag
        """
        context = iter(ElTree.iterparse(path, events=("start", "end")))  # noqa: S314
        _, root = next(context)
        for event, elem in context:
            if event == "end" and elem.tag == tag:
                yield elem
                root.clear()

    def _transform_data(self) -> None:
        """Open dataset and prepare for loading into database."""
        parser = self.parse_xml(self._data_file, "Chemical")  # type: ignore
        # there's no elegant way to get a row total in advance from parsed XML --
        # use a rich Console to provide a static spinner instead
        with Console(color_system=None).status(
            "Loading ChemIDplus records...", spinner="dots"
        ):
            for chemical in parser:  # type: ignore
                if "displayName" not in chemical.attrib:
                    continue

                # initial setup and get label
                display_name = chemical.attrib["displayName"]
                if not display_name or not re.search(TAGS_REGEX, display_name):
                    continue
                label = re.sub(TAGS_REGEX, "", display_name)
                params: RecordParams = {"label": label}

                # get concept ID
                reg_no = chemical.find("NumberList").find("CASRegistryNumber")
                if not reg_no:
                    continue
                params["concept_id"] = (
                    f"{NamespacePrefix.CASREGISTRY.value}:{reg_no.text}"
                )

                # get aliases
                aliases = []
                label_l = label.lower()
                name_list = chemical.find("NameList")
                if name_list:
                    for name in name_list.findall("NameOfSubstance"):
                        text = name.text
                        if text != display_name and text.lower() != label_l:
                            aliases.append(re.sub(TAGS_REGEX, "", text))
                params["aliases"] = aliases

                # get xrefs and associated_with
                params["xrefs"] = []
                params["associated_with"] = []
                locator_list = chemical.find("LocatorList")
                if locator_list:
                    for loc in locator_list.findall("InternetLocator"):
                        if loc.text == "DrugBank":
                            db = (
                                f"{NamespacePrefix.DRUGBANK.value}:"
                                f"{loc.attrib['url'].split('/')[-1]}"
                            )
                            params["xrefs"].append(db)  # type: ignore
                        elif loc.text == "FDA SRS":
                            unii = (
                                f"{NamespacePrefix.UNII.value}:"
                                f"{loc.attrib['url'].split('/')[-1]}"
                            )
                            params["associated_with"].append(unii)  # type: ignore

                self._load_therapy(params)

    def _load_meta(self) -> None:
        """Add source metadata."""
        meta = SourceMeta(
            data_license="custom",
            data_license_url="https://www.nlm.nih.gov/databases/download/terms_and_conditions.html",
            version=self._version,
            data_url="ftp://ftp.nlm.nih.gov/nlmdata/.chemidlease/",
            rdp_url=None,
            data_license_attributes=DataLicenseAttributes(
                non_commercial=False, share_alike=False, attribution=True
            ).model_dump(),
        )
        self.database.add_source_metadata(SourceName.CHEMIDPLUS, meta)
