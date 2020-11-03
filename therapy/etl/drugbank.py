"""This module defines the DrugBank ETL methods."""
from sqlalchemy.orm import Session
from therapy.etl.base import Base
from therapy import PROJECT_ROOT
import logging
from therapy import database, models, schemas  # noqa: F401
from therapy.models import Meta, Therapy, Alias, OtherIdentifier, TradeName
from therapy.schemas import SourceName, NamespacePrefix, Drug
from therapy.database import Base as B, engine, SessionLocal  # noqa: F401
from therapy.etl.base import IDENTIFIER_PREFIXES
from sqlalchemy import create_engine, event  # noqa: F401
from lxml import etree

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class DrugBank(Base):
    """ETL the DrugBank source into therapy.db."""

    def _extract_data(self, *args, **kwargs):
        """Extract data from the DrugBank source."""
        if 'data_path' in kwargs:
            self._data_src = kwargs['data_path']
        else:
            wd_dir = PROJECT_ROOT / 'data' / 'drugbank'
            try:
                self._data_src = sorted(list(wd_dir.iterdir()))[-1]
            except IndexError:
                raise FileNotFoundError  # TODO drugbank update function here

    def _transform_data(self, db):
        """Transform the DrugBank source."""
        xmlns = "{http://www.drugbank.ca}"

        tree = etree.parse(f"{self._data_src}")
        root = tree.getroot()

        for drug in root:
            params = {
                'concept_id': None,
                'label': None,
                'src_name': SourceName.DRUGBANK,
                'approval_status': None,
                'aliases': [],
                'other_identifiers': [],
                'trade_names': []
            }
            for element in drug:
                if element.tag == f"{xmlns}drugbank-id":
                    # Concept ID
                    if 'primary' in element.attrib:
                        params['concept_id'] = \
                            f"{NamespacePrefix.DRUGBANK.value}:{element.text}"
                    else:
                        # Aliases
                        params['aliases'].append(element.text)
                # Label
                if element.tag == f"{xmlns}name":
                    params['label'] = element.text
                # Aliases
                if element.tag == f"{xmlns}synonyms":
                    for alias in element:
                        if alias.text not in params['aliases'] and \
                                alias.attrib['language'] == 'english':
                            params['aliases'].append(alias.text)
                # Trade Names
                if element.tag == f"{xmlns}products":
                    for product in element:
                        for el in product:
                            if el.tag == f"{xmlns}name":
                                if el.text not in params['trade_names']:
                                    params['trade_names'].append(el.text)
                # Other Identifiers
                if element.tag == f"{xmlns}cas-number":
                    if element.text:
                        params['other_identifiers'].append(
                            f"{IDENTIFIER_PREFIXES['casRegistry']}:"
                            f"{element.text}")
                # Approval status
                if element.tag == f"{xmlns}groups":
                    group_type = []
                    for group in element:
                        group_type.append(group.text)
                    if "withdrawn" in group_type:
                        params['approval_status'] = "withdrawn"
                    elif "approved" in group_type:
                        params['approval_status'] = "approved"
                    elif "investigational" in group_type:
                        params['approval_status'] = "investigational"

            drug_obj = schemas.Drug(
                label=params['label'],
                approval_status=params['approval_status'],
                trade_name=params['trade_names'],
                aliases=params['aliases'],
                concept_identifier=params['concept_id'],
                other_identifiers=params['other_identifiers']
            )

            self._load_therapy(drug_obj, db)
            self._load_aliases(drug_obj, db)
            self._load_other_identifiers(drug_obj, db)
            self._load_trade_names(drug_obj, db)

    def _load_data(self, *args, **kwargs):
        """Load the DrugBank source into normalized database."""
        B.metadata.create_all(bind=engine)
        db: Session = SessionLocal()
        self._extract_data()
        self._transform_data(db)
        self._add_meta(db)
        db.commit()
        db.close()

    def _load_therapy(self, drug: Drug, db: Session):
        """Load a DrugBank therapy row into normalized database."""
        therapy = Therapy(
            concept_id=drug.concept_identifier,
            label=drug.label,
            approval_status=drug.approval_status,
            src_name=SourceName.DRUGBANK.value
        )
        db.add(therapy)

    def _load_aliases(self, drug: Drug, db: Session):
        """Load a DrugBank alias row into normalized database."""
        for alias in drug.aliases:
            alias_object = \
                Alias(concept_id=drug.concept_identifier, alias=alias)
            db.add(alias_object)

    def _load_other_identifiers(self, drug: Drug, db: Session):
        """Load a DrugBank other identifier row into normalized database."""
        for other_identifier in drug.other_identifiers:
            other_identifier_object = OtherIdentifier(
                concept_id=drug.concept_identifier, other_id=other_identifier)
            db.add(other_identifier_object)

    def _load_trade_names(self, drug: Drug, db: Session):
        """Load a DrugBank trade name row into normalized database."""
        for trade_name in drug.trade_name:
            trade_name_object = TradeName(
                concept_id=drug.concept_identifier, trade_name=trade_name)
            db.add(trade_name_object)

    def _add_meta(self, db: Session):
        """Add DrugBank metadata."""
        meta_object = Meta(src_name=SourceName.DRUGBANK.value,
                           data_license='CC BY-NC 4.0',
                           data_license_url='https://creativecommons.org/licenses/by-nc/4.0/legalcode',  # noqa E501
                           version='5.1.7',
                           data_url='https://go.drugbank.com/releases/5-1-7/downloads/all-full-database'  # noqa E501
                           )
        db.add(meta_object)
