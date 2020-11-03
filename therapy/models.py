"""This module defines the database models."""
from sqlalchemy import Column, ForeignKey, Integer, String, Index, func
from sqlalchemy.orm import relationship
from .database import Base


class Therapy(Base):
    """Table that lists compounds/biotherapeutics
    with associated identifiers.
    """

    __tablename__ = "therapies"

    concept_id = Column(String, index=True, primary_key=True)
    label = Column(String, index=True)
    approval_status = Column(String)
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
                                           ondelete='CASCADE'), index=True)
    other_id = Column(String, index=True, nullable=False)

    record = relationship("Therapy", back_populates="other_identifiers")


class Alias(Base):
    """Table that lists synonyms for the compound."""

    __tablename__ = "aliases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    alias = Column(String, index=True, nullable=False)
    concept_id = Column(String, ForeignKey('therapies.concept_id',
                                           ondelete='CASCADE'), index=True)

    record = relationship("Therapy", back_populates="aliases")


class TradeName(Base):
    """Table that lists the trade name for the product."""

    __tablename__ = "trade_names"

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_name = Column(String, index=True, nullable=False)
    concept_id = Column(String, ForeignKey('therapies.concept_id',
                                           ondelete='CASCADE'), index=True)

    record = relationship("Therapy", back_populates="trade_names")


class Meta(Base):
    """Table that lists meta info for each source."""

    __tablename__ = "meta_data"

    src_name = Column(String, primary_key=True)
    data_license = Column(String, nullable=False)
    data_license_url = Column(String, nullable=False)
    version = Column(String, nullable=False)
    data_url = Column(String)

    record = relationship("Therapy", back_populates="src_meta_data")


Index('lower_therapies_label', func.lower(Therapy.label))
Index('lower_therapies_c_id', func.lower(Therapy.concept_id))
Index('lower_other_identifiers_c_id', func.lower(OtherIdentifier.concept_id))
Index('lower_alias', func.lower(Alias.alias))
Index('lower_trade_name', func.lower(TradeName.trade_name))
