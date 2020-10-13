"""Define models"""
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from .database import Base


class Therapy(Base):
    """Therapy table"""

    __tablename__ = "therapies"

    concept_id = Column(String, primary_key=True, index=True)
    label = Column(String, index=True)
    max_phase = Column(Integer)
    withdrawn_flag = Column(Boolean)
    trade_name = Column(String)
    src_name = Column(String, ForeignKey('meta_data.src_name'))

    aliases = relationship("Alias", back_populates="record")
    other_identifiers = relationship("OtherIdentifier",
                                     back_populates="record")
    src_meta_data = relationship("Meta", back_populates="record")


class OtherIdentifier(Base):
    """Other Identifier table"""

    __tablename__ = "other_identifiers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    concept_id = Column(String, ForeignKey('therapies.concept_id'))
    chembl_id = Column(String)
    wikidata_id = Column(String)
    ncit_id = Column(String)
    drugbank_id = Column(String)

    record = relationship("Therapy", back_populates="other_identifiers")


class Alias(Base):
    """Alias table"""

    __tablename__ = "aliases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    alias = Column(String, index=True)
    concept_id = Column(String, ForeignKey='therapies.concept_id')

    record = relationship("Therapy", back_populates="aliases")


class Meta(Base):
    """Meta info for each source"""

    __tablename__ = "meta_data"

    src_name = Column(String, primary_key=True)
    data_license = Column(String)
    data_license_url = Column(String)
    version = Column(String)
    data_url = Column(String)

    record = relationship("Therapy", back_populates="src_meta_data")
