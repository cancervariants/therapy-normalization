"""Define models."""
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from .database import Base


class Therapy(Base):
    """Table that lists compounds/biotherapeutics
    with associated identifiers.
    """

    __tablename__ = "therapies"

    concept_id = Column(String, index=True, primary_key=True)
    label = Column(String, index=True)
    max_phase = Column(Integer)
    withdrawn_flag = Column(Boolean)
    src_name = Column(String, ForeignKey('meta_data.src_name'))

    aliases = relationship("Alias", back_populates="record",
                           passive_deletes=True)
    other_identifiers = relationship("OtherIdentifier",
                                     back_populates="record",
                                     passive_deletes=True)
    trade_names = relationship("TradeName",
                               back_populates="record",
                               passive_deletes=True)
    src_meta_data = relationship("Meta", back_populates="record")


class OtherIdentifier(Base):
    """Table that lists other identifiers for a compound."""

    __tablename__ = "other_identifiers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    concept_id = Column(String, ForeignKey('therapies.concept_id',
                                           ondelete='CASCADE'))
    chembl_id = Column(String)
    wikidata_id = Column(String)
    ncit_id = Column(String)
    drugbank_id = Column(String)

    record = relationship("Therapy", back_populates="other_identifiers")


class Alias(Base):
    """Table that lists synonyms for the compound."""

    __tablename__ = "aliases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    alias = Column(String, index=True)
    concept_id = Column(String, ForeignKey('therapies.concept_id',
                                           ondelete='CASCADE'))

    record = relationship("Therapy", back_populates="aliases")


class TradeName(Base):
    """Table that lists the trade name for the product."""

    __tablename__ = "trade_names"

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_name = Column(String, index=True)
    concept_id = Column(String, ForeignKey('therapies.concept_id',
                                           ondelete='CASCADE'))

    record = relationship("Therapy", back_populates="trade_names")


class Meta(Base):
    """Table that lists meta info for each source."""

    __tablename__ = "meta_data"

    src_name = Column(String, primary_key=True)
    data_license = Column(String)
    data_license_url = Column(String)
    version = Column(String)
    data_url = Column(String)

    record = relationship("Therapy", back_populates="src_meta_data")
