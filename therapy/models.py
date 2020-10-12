"""Define models"""
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from .database import Base


class Therapy(Base):
    """Therapy table"""

    __tablename__ = "therapies"

    concept_id = Column(String, primary_key=True, index=True)
    label = Column(String, index=True)
    max_phase = Column(Integer)
    withdrawn_flag = Column(Boolean)
    trade_name = Column(String)


class OtherIdentifier(Base):
    """Other Identifier table"""

    __tablename__ = "other_identifiers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    concept_id = Column(String, ForeignKey('therapies.concept_id'))
    chembl_id = Column(String)
    wikidata_id = Column(String)
    ncit_id = Column(String)
    drugbank_id = Column(String)


class Alias(Base):
    """Alias table"""

    __tablename__ = "aliases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    alias = Column(String, index=True)
    concept_id = Column(String, ForeignKey='therapies.concept_id')
