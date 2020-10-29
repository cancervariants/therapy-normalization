"""This module creates the database session."""
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from therapy import PROJECT_ROOT
import os

test = os.environ.get("THERAPY_DB_TEST")
if not test:
    uri = f"sqlite:///{PROJECT_ROOT}/data/therapy.db"
elif test == "TEST":
    uri = f"sqlite:///{PROJECT_ROOT}/tests/unit/data/test_therapy.db"


engine = create_engine(
    uri, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

convention = {
    "ix": 'ix_%(column_0_label)s',
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}
meta = MetaData(naming_convention=convention)
Base = declarative_base(meta)
