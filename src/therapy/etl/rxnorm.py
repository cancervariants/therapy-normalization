"""Defines the RxNorm ETL methods.

"This product uses publicly available data courtesy of the U.S. National
Library of Medicine (NLM), National Institutes of Health, Department of Health
 and Human Services; NLM is not responsible for the product and does not
 endorse or recommend this or any other product."
"""

import csv
import logging
import re
from pathlib import Path

import yaml
from tqdm import tqdm
from wags_tails import CustomData, RxNormData

from therapy import ASSOC_WITH_SOURCES, ITEM_TYPES, XREF_SOURCES
from therapy.etl.base import Base
from therapy.schemas import (
    ApprovalRating,
    NamespacePrefix,
    RecordParams,
    SourceMeta,
    SourceName,
)

_logger = logging.getLogger(__name__)

# Designated Alias, Designated Syn, Tall Man Syn, Machine permutation
# Generic Drug Name, Designated Preferred Name, Preferred Entry Term,
# Clinical Drug, Entry Term, Rxnorm Preferred
ALIASES = ["SYN", "SY", "TMSY", "PM", "GN", "PT", "PEP", "CD", "ET", "RXN_PT"]

# Fully-specified drug brand name that can be prescribed
# Fully-specified drug brand name that can not be prescribed,
# Semantic branded drug
TRADE_NAMES = ["BD", "BN", "SBD"]

# Allowed rxnorm xrefs that have Source Level Restriction 0 or 1
RXNORM_XREFS = [
    "ATC",
    "CVX",
    "DRUGBANK",
    "MMSL",
    "MSH",
    "MTHCMSFRF",
    "MTHSPL",
    "RXNORM",
    "USP",
    "VANDF",
]


class RxNorm(Base):
    """Class for RxNorm ETL methods."""

    @staticmethod
    def _create_drug_form_yaml(drug_forms_file: Path, rxnorm_file: Path) -> None:
        """Create a YAML file containing RxNorm drug form values.

        :param drug_forms_file: location to save output file to
        :param rxnorm_file: source RRF file to extract drug form values from
        """
        dfs = []
        with rxnorm_file.open() as f:
            data = csv.reader(f, delimiter="|")
            for row in data:
                if ((row[12] == "DF") and (row[11] == "RXNORM")) and (
                    row[14] not in dfs
                ):
                    dfs.append(row[14])
        with drug_forms_file.open("w") as f:
            yaml.dump(dfs, f)

    _DataSourceClass = RxNormData

    def _extract_data(self, use_existing: bool) -> None:
        """Acquire source data.

        This method is responsible for initializing an instance of a data handler and,
        in most cases, setting ``self._data_file`` and ``self._version``.

        :param bool use_existing: if True, don't try to fetch latest source data
        """
        self._data_file, self._version = self._data_source.get_latest(
            from_local=use_existing
        )
        drug_form_data_handler = CustomData(
            src_name="rxnorm_drug_forms",
            filetype="yaml",
            latest_version_cb=lambda: self._version,
            download_cb=lambda version, file: self._create_drug_form_yaml(  # noqa: ARG005
                file,
                self._data_file,  # type: ignore
            ),
            data_dir=self._data_source.data_dir,
            file_name="rxnorm_drug_forms",
        )
        self._drug_forms_file, _ = drug_form_data_handler.get_latest()

    def _transform_data(self) -> None:
        """Transform the RxNorm source."""
        with self._drug_forms_file.open() as file:
            drug_forms = yaml.safe_load(file)

        with self._data_file.open() as f:  # type: ignore
            rff_data = csv.reader(f, delimiter="|")
            # Link ingredient to brand
            ingredient_brands: dict[str, str] = {}
            # Link precise ingredient to get brand
            precise_ingredient: dict[str, str] = {}
            # Transformed therapy records
            data: dict[str, dict] = {}
            # Link ingredient to brand
            sbdfs: dict[str, str] = {}
            # Get RXNORM|BN to concept_id
            brands: dict[str, str] = {}
            for row in rff_data:
                if row[11] in RXNORM_XREFS:
                    concept_id = f"{NamespacePrefix.RXNORM.value}:{row[0]}"
                    if row[12] == "BN" and row[11] == "RXNORM":
                        brands[row[14]] = concept_id
                    if row[12] == "SBDC" and row[11] == "RXNORM":
                        # Semantic Branded Drug Component
                        self._get_brands(row, ingredient_brands)
                    else:
                        if concept_id not in data:
                            params: RecordParams = {}
                            params["concept_id"] = concept_id
                            self._add_str_field(
                                params, row, precise_ingredient, drug_forms, sbdfs
                            )
                            self._add_xref_assoc(params, row)
                            data[concept_id] = params
                        else:
                            # Concept already created
                            params = data[concept_id]
                            self._add_str_field(
                                params, row, precise_ingredient, drug_forms, sbdfs
                            )
                            self._add_xref_assoc(params, row)

            for value in tqdm(data.values(), ncols=80, disable=self._silent):
                if "label" in value:
                    self._get_trade_names(
                        value, precise_ingredient, ingredient_brands, sbdfs
                    )
                    self._load_brand_concepts(value, brands)

                    params = {"concept_id": value["concept_id"]}

                    for field in [*list(ITEM_TYPES.keys()), "approval_ratings"]:
                        field_value = value.get(field)
                        if field_value:
                            params[field] = field_value
                    self._load_therapy(params)

    def _get_brands(self, row: list, ingredient_brands: dict) -> None:
        """Add ingredient and brand to ingredient_brands.

        :param List row: A row in the RxNorm data file.
        :param Dict ingredient_brands: Store brands for each ingredient
        """
        # SBDC: Ingredient(s) + Strength + [Brand Name]
        term = row[14]
        ingredients_brand = re.sub(
            r"(\d*)(\d*\.)?\d+ (MG|UNT|ML)?(/(ML|HR|MG))?", "", term
        )
        brand = term.split("[")[-1].split("]")[0]
        ingredients = ingredients_brand.replace(f"[{brand}]", "")
        if "/" in ingredients:
            ingredients_list = ingredients.split("/")
            for ingredient in ingredients_list:
                self._add_term(ingredient_brands, brand, ingredient.strip())
        else:
            self._add_term(ingredient_brands, brand, ingredients.strip())

    def _get_trade_names(
        self,
        value: dict,
        precise_ingredient: dict,
        ingredient_brands: dict,
        sbdfs: dict,
    ) -> None:
        """Get trade names for a given ingredient.

        :param Dict value: Therapy attributes
        :param Dict precise_ingredient: Brand names for precise ingredient
        :param Dict ingredient_brands: Brand names for ingredient
        :param Dict sbdfs: Brand names for ingredient from SBDF row
        """
        record_label = value["label"].lower()
        labels = [record_label]

        if "PIN" in value and value["PIN"] in precise_ingredient:
            for pin in precise_ingredient[value["PIN"]]:
                labels.append(pin.lower())  # noqa: PERF401

        for label in labels:
            trade_names: list[str] = [
                val for key, val in ingredient_brands.items() if label == key.lower()
            ]
            trade_names_uq = {val for sublist in trade_names for val in sublist}
            for tn in trade_names_uq:
                self._add_term(value, tn, "trade_names")

        if record_label in sbdfs:
            for tn in sbdfs[record_label]:
                self._add_term(value, tn, "trade_names")

    def _load_brand_concepts(self, rxnorm_record: dict, brands: dict) -> None:
        """Connect brand names to a concept and load into the database.

        :params rxnorm_record: A transformed therapy record
        :params brands: Connects brand names to concept records
        """
        if "trade_names" in rxnorm_record:
            for tn in rxnorm_record["trade_names"]:
                brand = brands.get(tn)
                if brand:
                    self.database.add_rxnorm_brand(brand, rxnorm_record["concept_id"])

    def _add_str_field(
        self,
        params: dict,
        row: list,
        precise_ingredient: dict,
        drug_forms: list,
        sbdfs: dict,
    ) -> None:
        """Differentiate STR field.

        :param Dict params: A transformed therapy record.
        :param List row: A row in the RxNorm data file.
        :param Dict precise_ingredient: Precise ingredient information
        :param List drug_forms: RxNorm Drug Form values
        :param Dict sbdfs: Brand names for precise ingredient
        """
        term = row[14]
        term_type = row[12]
        source = row[11]

        if (term_type == "IN" or term_type == "PIN") and source == "RXNORM":
            params["label"] = term
            if row[17] == "4096":
                params["approval_ratings"] = [ApprovalRating.RXNORM_PRESCRIBABLE.value]
        elif term_type in ALIASES:
            self._add_term(params, term, "aliases")
        elif term_type in TRADE_NAMES:
            self._add_term(params, term, "trade_names")

        if source == "RXNORM" and term_type == "SBDF":
            brand = term.split("[")[-1].split("]")[0]
            ingredient_strength = term.replace(f"[{brand}]", "")
            for df in drug_forms:
                if df in ingredient_strength:
                    ingredient = ingredient_strength.replace(df, "").strip()
                    self._add_term(sbdfs, brand, ingredient.lower())
                    break

        if source == "MSH":
            if term_type == "MH":
                # Get ID for accessing precise ingredient
                params["PIN"] = row[13]
            elif term_type == "PEP":
                self._add_term(precise_ingredient, term, row[13])

    @staticmethod
    def _add_term(params: dict, term: str, label_type: str) -> None:
        """Add a single term to a therapy record in an associated field.

        :param Dict params: A transformed therapy record.
        :param Str term: The term to be added
        :param Str label_type: The type of term
        """
        if params.get(label_type):
            if term not in params[label_type]:
                params[label_type].append(term)
        else:
            params[label_type] = [term]

    def _add_xref_assoc(self, params: dict, row: list) -> None:
        """Add xref or associated_with to therapy.

        :param Dict params: A transformed therapy record.
        :param List row: A row in the RxNorm data file.
        """
        ref = row[11]
        lui = row[13]
        if ref and lui != "NOCODE":
            xref_assoc = "UNII" if ref == "MTHSPL" else row[11].upper()

            if xref_assoc in XREF_SOURCES:
                source_id = f"{NamespacePrefix[xref_assoc].value}:{lui}"
                if source_id != params["concept_id"]:
                    # Sometimes concept_id is included in the source field
                    self._add_term(params, source_id, "xrefs")
            elif xref_assoc in ASSOC_WITH_SOURCES:
                source_id = f"{NamespacePrefix[xref_assoc].value}:{lui}"
                self._add_term(params, source_id, "associated_with")
            else:
                _logger.info("%s not in NameSpacePrefix.", xref_assoc)

    def _load_meta(self) -> None:
        """Add RxNorm metadata."""
        meta = SourceMeta(
            data_license="UMLS Metathesaurus",
            data_license_url="https://www.nlm.nih.gov/research/umls/rxnorm/docs/termsofservice.html",
            version=self._version,
            data_url="https://download.nlm.nih.gov/umls/kss/rxnorm/",
            rdp_url=None,
            data_license_attributes={
                "non_commercial": False,
                "share_alike": False,
                "attribution": True,
            },
        )
        self.database.add_source_metadata(SourceName.RXNORM, meta)
