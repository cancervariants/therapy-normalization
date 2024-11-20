"""Module for Guide to PHARMACOLOGY ETL methods."""

import csv
import html
import logging
import re
from typing import Any

from tqdm import tqdm
from wags_tails.guide_to_pharmacology import GtoPLigandPaths

from therapy.etl.base import Base, SourceFormatError
from therapy.schemas import ApprovalRating, NamespacePrefix, SourceMeta, SourceName

_logger = logging.getLogger(__name__)

TAG_PATTERN = re.compile("</?[a-zA-Z]+>")
PMID_PATTERN = re.compile(r"\[PMID:[ ]?\d+\]")


class GuideToPHARMACOLOGY(Base):
    """Class for Guide to PHARMACOLOGY ETL methods."""

    def _extract_data(self, use_existing: bool) -> None:
        """Acquire source data.

        This method is responsible for initializing an instance of a data handler and
        setting ``self._data_files`` and ``self._version``.

        :param bool use_existing: if True, don't try to fetch latest source data
        """
        data_files, self._version = self._data_source.get_latest(
            from_local=use_existing
        )
        self._data_files: GtoPLigandPaths = data_files  # type: ignore

    def _transform_data(self) -> None:
        """Transform Guide To PHARMACOLOGY data."""
        data: dict[str, Any] = {}
        self._transform_ligands(data)
        self._transform_ligand_id_mappings(data)

        for param in tqdm(data.values(), ncols=80, disable=self._silent):
            self._load_therapy(param)

    @staticmethod
    def _process_name(name: str) -> str:
        """Remove tags from GtoP name object.
        :param name: raw drug referent
        :return: cleaned name (may be unchanged)
        """
        return re.sub(TAG_PATTERN, "", name)

    def _transform_ligands(self, data: dict) -> None:
        """Transform ligands data file and add this data to `data`.

        :param dict data: Transformed data
        """
        with self._data_files.ligands.open() as f:
            rows = csv.reader(f, delimiter="\t")

            # check that file structure is the same
            next(rows)
            if next(rows) != [
                "Ligand ID",
                "Name",
                "Species",
                "Type",
                "Approved",
                "Withdrawn",
                "Labelled",
                "Radioactive",
                "PubChem SID",
                "PubChem CID",
                "UniProt ID",
                "Ensembl ID",
                "ChEMBL ID",
                "Ligand Subunit IDs",
                "Ligand Subunit Name",
                "Ligand Subunit UniProt IDs",
                "Ligand Subunit Ensembl IDs",
                "IUPAC name",
                "INN",
                "Synonyms",
                "SMILES",
                "InChIKey",
                "InChI",
                "GtoImmuPdb",
                "GtoMPdb",
                "Antibacterial",
            ]:
                msg = "GtoP ligands file contains missing or unrecognized columns. See FAQ in README for suggested resolution."
                raise SourceFormatError(msg)

            for row in rows:
                params: dict[str, list[str] | str] = {
                    "concept_id": f"{NamespacePrefix.GUIDETOPHARMACOLOGY.value}:{row[0]}",
                    "label": self._process_name(row[1]),
                    "src_name": SourceName.GUIDETOPHARMACOLOGY.value,
                }

                approval_rating = self._set_approval_rating(row[4], row[5])
                if approval_rating:
                    params["approval_ratings"] = [approval_rating]

                associated_with = []
                aliases = []
                if row[8]:
                    associated_with.append(
                        f"{NamespacePrefix.PUBCHEMSUBSTANCE.value}:{row[8]}"
                    )
                if row[9]:
                    associated_with.append(
                        f"{NamespacePrefix.PUBCHEMCOMPOUND.value}:{row[9]}"
                    )
                if row[10]:
                    associated_with.append(f"{NamespacePrefix.UNIPROT.value}:{row[10]}")
                if row[17]:
                    aliases.append(self._process_name(row[17]))  # IUPAC
                if row[18]:
                    # International Non-proprietary Name assigned by the WHO
                    aliases.append(self._process_name(row[18]))
                if row[19]:
                    # synonyms
                    synonyms = row[19].split("|")
                    for s in synonyms:
                        if "&" in s and ";" in s:
                            name_code = s[s.index("&") : s.index(";") + 1]
                            if name_code.lower() in ["&reg;", "&trade;"]:
                                # Remove trademark symbols to allow for search
                                s = s.replace(name_code, "")
                            s = html.unescape(s)
                        aliases.append(self._process_name(s))
                if row[21]:
                    associated_with.append(
                        f"{NamespacePrefix.INCHIKEY.value}:{row[21]}"
                    )

                if associated_with:
                    params["associated_with"] = associated_with
                if aliases:
                    params["aliases"] = aliases

                data[params["concept_id"]] = params

    @staticmethod
    def _get_xrefs(ref: str, namespace: str) -> list[str]:
        """Construct xrefs from raw string.
        :param ref: raw ref value (may need to be separated)
        :param namspace: namespace prefix to use
        :return: List (usually with just one member) of xref IDs
        """
        xrefs = []
        for split_ref in ref.split("|"):
            xref = f"{namespace}:{split_ref}"
            xrefs.append(xref)
        return xrefs

    def _transform_ligand_id_mappings(self, data: dict) -> None:
        """Transform ligand_id_mappings and add this data to `data`
        All ligands found in this file should already be in data

        :param dict data: Transformed data
        """
        with self._data_files.ligand_id_mapping.open() as f:
            rows = csv.reader(f, delimiter="\t")
            next(rows)
            if next(rows) != [
                "Ligand id",
                "Name",
                "Species",
                "Type",
                "PubChem SID",
                "PubChem CID",
                "ChEMBl ID",
                "Chebi ID",
                "UniProt id",
                "Ensembl ID",
                "IUPAC name",
                "INN",
                "CAS",
                "DrugBank ID",
                "Drug Central ID",
            ]:
                msg = "GtoP ligand mapping file contains missing or unrecognized columns. See FAQ in README for suggested resolution."
                raise SourceFormatError(msg)

            for row in rows:
                concept_id = f"{NamespacePrefix.GUIDETOPHARMACOLOGY.value}:{row[0]}"

                if concept_id not in data:
                    _logger.debug("%s not in ligands", concept_id)
                    continue
                params = data[concept_id]
                xrefs = []
                associated_with = params.get("associated_with", [])
                if row[6]:
                    xrefs += self._get_xrefs(row[6], NamespacePrefix.CHEMBL.value)
                if row[7]:
                    # CHEBI IDs are already namespaced
                    for chebi_id in row[7].split("|"):
                        associated_with.append(chebi_id)
                if row[8]:
                    associated_with += self._get_xrefs(
                        row[8], NamespacePrefix.UNIPROT.value
                    )
                if row[12]:
                    xrefs += self._get_xrefs(row[12], NamespacePrefix.CASREGISTRY.value)
                if row[13]:
                    xrefs += self._get_xrefs(row[13], NamespacePrefix.DRUGBANK.value)
                if row[14]:
                    associated_with += self._get_xrefs(
                        row[14], NamespacePrefix.DRUGCENTRAL.value
                    )
                if xrefs:
                    params["xrefs"] = xrefs
                if associated_with:
                    params["associated_with"] = associated_with

    def _set_approval_rating(self, approved: str, withdrawn: str) -> str | None:
        """Set approval rating value.

        :param str approved: The drug is or has in the past been approved for human
            clinical use by a regulatory agency
        :param str withdrawn: The drug is no longer approved for its original clinical
            use in one or more countries
        :return: Approval rating
        """
        if approved and not withdrawn:
            approval_rating: str | None = ApprovalRating.GTOPDB_APPROVED.value
        elif withdrawn:
            approval_rating = ApprovalRating.GTOPDB_WITHDRAWN.value
        else:
            approval_rating = None
        return approval_rating

    def _load_meta(self) -> None:
        """Load Guide to PHARMACOLOGY metadata to database."""
        meta = SourceMeta(
            data_license="CC BY-SA 4.0",
            data_license_url="https://creativecommons.org/licenses/by-sa/4.0/",
            version=self._version,
            data_url="https://www.guidetopharmacology.org/download.jsp",
            rdp_url=None,
            data_license_attributes={
                "non_commercial": False,
                "share_alike": True,
                "attribution": True,
            },
        )
        self.database.add_source_metadata(SourceName.GUIDETOPHARMACOLOGY, meta)
