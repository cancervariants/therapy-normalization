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

        for d in root:
            params = {
                'concept_id': None,
                'label': None,
                'max_phase': None,
                'withdrawn_flag': None,
                'src_name': SourceName.DRUGBANK,
                'aliases': [],
                'other_identifiers': [],
                'trade_names': []
            }
            for child in d:
                if child.tag == f"{xmlns}drugbank-id":
                    # Concept ID
                    if 'primary' in child.attrib:
                        params['concept_id'] = \
                            f"{NamespacePrefix.DRUGBANK.value}:{child.text}"
                    else:
                        # Aliases
                        params['aliases'].append(child.text)
                # Label
                if child.tag == f"{xmlns}name":
                    params['label'] = child.text
                # Aliases
                if child.tag == f"{xmlns}synonyms":
                    for alias in child:
                        if alias.text not in params['aliases'] and \
                                alias.attrib['language'] == 'english':
                            params['aliases'].append(alias.text)
                # Trade Names TODO check that these are actually trade names
                if child.tag == f"{xmlns}products":
                    for products in child:
                        for product in products:
                            if product.tag == f"{xmlns}name":
                                if product.text not in params['trade_names']:
                                    params['trade_names'].append(product.text)
                # Other Identifiers
                if child.tag == f"{xmlns}cas-number":
                    if child.text:
                        params['other_identifiers'].append(
                            f"{IDENTIFIER_PREFIXES['casRegistry']}:"
                            f"{child.text}")
                # Withdrawn flag
                if child.tag == f"{xmlns}groups":
                    for groups in child:
                        for group in groups:
                            if group.text == "withdrawn":
                                params['withdrawn_flag'] = True

            drug = schemas.Drug(
                label=params['label'],
                max_phase=None,
                withdrawn=params['withdrawn_flag'],
                trade_name=params['trade_names'],
                aliases=params['aliases'],
                concept_identifier=params['concept_id'],
                other_identifiers=params['other_identifiers']
            )

            self._load_therapy(drug, db)
            self._load_aliases(drug, db)
            self._load_other_identifiers(drug, db)
            self._load_trade_names(drug, db)

    def _load_data(self, *args, **kwargs):
        """Load the DrugBank source into therapy.db."""
        B.metadata.create_all(bind=engine)
        db: Session = SessionLocal()
        self._extract_data()
        self._transform_data(db)
        self._add_meta(db)
        db.commit()
        db.close()

    def _load_therapy(self, drug: Drug, db: Session):
        therapy = Therapy(
            concept_id=drug.concept_identifier,
            label=drug.label,
            max_phase=drug.max_phase,
            withdrawn_flag=drug.withdrawn,
            src_name=SourceName.DRUGBANK.value
        )
        db.add(therapy)

    def _load_aliases(self, drug: Drug, db: Session):
        for alias in drug.aliases:
            alias_object = \
                Alias(concept_id=drug.concept_identifier, alias=alias)
            db.add(alias_object)

    def _load_other_identifiers(self, drug: Drug, db: Session):
        for other_identifier in drug.other_identifiers:
            other_identifier_object = OtherIdentifier(
                concept_id=drug.concept_identifier, other_id=other_identifier)
            db.add(other_identifier_object)

    def _load_trade_names(self, drug: Drug, db: Session):
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
