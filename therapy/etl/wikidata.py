"""This module defines the Wikidata ETL methods."""
from .base import Base, IDENTIFIER_PREFIXES
from therapy import PROJECT_ROOT, database, schemas
from therapy.database import Base as B
import json
from therapy.schemas import Drug, SourceName, NamespacePrefix
import logging
from sqlalchemy import create_engine, event  # noqa F401

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class Wikidata(Base):
    """ETL the Wikidata source into therapy.db."""

    SPARQL_QUERY = """
SELECT ?item ?itemLabel ?casRegistry ?pubchemCompound ?pubchemSubstance ?chembl
  ?rxnorm ?drugbank ?alias WHERE {
  ?item (wdt:P31/(wdt:P279*)) wd:Q12140.
  OPTIONAL {
    ?item skos:altLabel ?alias.
    FILTER((LANG(?alias)) = "en")
  }
  OPTIONAL { ?item p:P231 ?wds1.
             ?wds1 ps:P231 ?casRegistry.
           }
  OPTIONAL { ?item p:P662 ?wds2.
             ?wds2 ps:P662 ?pubchemCompound.
           }
  OPTIONAL { ?item p:P2153 ?wds3.
             ?wds3 ps:P2153 ?pubchemSubstance.
           }
  OPTIONAL { ?item p:P592 ?wds4.
             ?wds4 ps:P592 ?chembl
           }
  OPTIONAL { ?item p:P3345 ?wds5.
             ?wds5 ps:P3345 ?rxnorm.
           }
  OPTIONAL { ?item p:P715 ?wds6.
             ?wds6 ps:P715 ?drugbank
           }
  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en".
  }
}
"""

    def __init__(self, *args, **kwargs):
        """Initialize wikidata ETL class"""
        self._concept_ids = set()
        self._alias_pairs = set()
        self._other_id_pairs = set()
        B.metadata.create_all(bind=database.engine)
        self._get_db()
        self._extract_data(*args, **kwargs)
        self._add_meta()
        self._transform_data()

    def _extract_data(self, *args, **kwargs):
        """Extract data from the Wikidata source."""
        if 'data_path' in kwargs:
            self._data_src = kwargs['data_path']
        else:
            wd_dir = PROJECT_ROOT / 'data' / 'wikidata'
            wd_dir.mkdir(exist_ok=True, parents=True)  # TODO needed?
            try:
                self._data_src = sorted(list(wd_dir.iterdir()))[-1]
            except IndexError:
                raise FileNotFoundError  # TODO wikidata update function here
        self._version = self._data_src.stem.split('_')[1]

    def _transform_data(self):
        """Transform the Wikidata source."""
        with open(self._data_src, 'r') as f:
            records = json.load(f)

        database.engine.connect()
        for record in records:
            record_id = record['item'].split('/')[-1]
            concept_id = f"{NamespacePrefix.WIKIDATA.value}:{record_id}"
            if concept_id not in self._concept_ids:
                if 'itemLabel' in record.keys():
                    label = record['itemLabel']
                else:
                    label = 'NULL'
                drug = schemas.Drug(label=label,
                                    max_phase=None,
                                    withdrawn=None,
                                    trade_name=[],
                                    aliases=[],
                                    concept_identifier=concept_id,
                                    other_identifiers=[])

                self._concept_ids.add(concept_id)
                self._load_therapy(concept_id, drug)

            if 'alias' in record.keys():
                alias = record['alias']
                if (concept_id, alias) not in self._alias_pairs:
                    self._alias_pairs.add((concept_id, alias))
                    self._load_alias(concept_id, alias)

            for key in IDENTIFIER_PREFIXES.keys():
                if key in record.keys():
                    other_id = record[key]
                    fmted_other_id = f"{IDENTIFIER_PREFIXES[key]}:{other_id}"
                    if (concept_id, fmted_other_id) not in \
                            self._other_id_pairs:
                        self._other_id_pairs.add((concept_id, fmted_other_id))
                        self._load_other_id(concept_id, fmted_other_id)

    def _sqlite_str(self, string):
        """Sanitizes string to use as value in SQL statement.

        Some wikidata entries include items with single quotes,
        like wikidata:Q80863 alias: 5'-(Tetrahydrogen triphosphate) Adenosine
        """
        if string == "NULL":
            return "NULL"
        else:
            sanitized = string.replace("'", "''")
            return f"{sanitized}"

    def _load_alias(self, concept_id: str, alias: str):
        """Load alias."""
        alias_clean = self._sqlite_str(alias)
        statement = f"""INSERT INTO aliases(alias, concept_id)
            VALUES (
                '{alias_clean}',
                '{concept_id}');
        """
        database.engine.execute(statement)

    def _load_other_id(self, concept_id: str, other_id: str):
        """Load individual other_id row.
        Args:
            concept_id: a str corresponding to full namespaced Wikidata
                concept ID
            other_id: a str corresponding to other identifier
        """
        statement = f"""
            INSERT INTO other_identifiers(concept_id, other_id)
            VALUES ('{concept_id}', '{other_id}');
        """
        database.engine.execute(statement)

    def _load_therapy(self, concept_id: str, drug: Drug):
        """Load an individual therapy row."""
        statement = f"""INSERT INTO therapies(concept_id, label, src_name)
                    VALUES(
                        '{concept_id}',
                        '{self._sqlite_str(drug.label)}',
                        '{SourceName.WIKIDATA.value}');"""
        database.engine.execute(statement)

    def _get_db(self):
        """Create a new SQLAlchemy session that will be used in a single
        request, and then close it once the request is finished.
        """
        db = database.SessionLocal()
        try:
            yield db
        finally:
            print("closed")
            db.close()

    def _add_meta(self):
        """Add Wikidata metadata."""
        insert_meta = f"""
            INSERT INTO meta_data(src_name, data_license, data_license_url,
                version, data_url)
            SELECT
                '{SourceName.WIKIDATA.value}',
                'CC0 1.0',
                'https://creativecommons.org/publicdomain/zero/1.0/',
                '{self._version}',
                NULL
            WHERE NOT EXISTS (
                SELECT * FROM meta_data
                WHERE src_name = '{SourceName.WIKIDATA.value}'
            );
        """
        database.engine.execute(insert_meta)
