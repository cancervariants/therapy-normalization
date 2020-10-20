"""This module defines the Wikidata normalizer"""
from .base import Base, IDENTIFIER_PREFIXES
from therapy import PROJECT_ROOT, database, models, schemas  # noqa: F401
from therapy.database import Base as B  # noqa: F401
import json
from therapy.schemas import Drug
import logging
from sqlalchemy import create_engine, event  # noqa: F401
from collections import defaultdict

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class Wikidata(Base):
    """A normalizer using the Wikidata resource."""

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

    def __init__(self):
        """Initialize wikidata ETL class"""
        self._concept_ids = []
        super().__init__()

    def _extract_data(self, *args, **kwargs):
        """Extract data"""
        if 'data_path' in kwargs:
            self._data_src = kwargs['data_path']
        else:
            wd_dir = PROJECT_ROOT / 'data' / 'wikidata'
            wd_dir.mkdir(exist_ok=True, parents=True)  # TODO needed?
            try:
                self._data_src = sorted(list(wd_dir.iterdir()))[-1]
            except TypeError:
                raise FileNotFoundError  # TODO wikidata update function here
        self._version = self._data_src.stem.split('_')[1]

    def _entry_exists(self, concept_id: str):
        """Check if ETL methods have already encountered concept_id. This
        should mean that there is no need to insert rows for the aliases
        or other_identifiers tables.

        At present, checks the arg `concept_id` against a list held in a class
        data field; in the future, could consider querying the DB to confirm.
        """
        return concept_id in self._concept_ids

    def _transform_data(self, *args, **kwargs):
        """Transform data"""
        with open(self._data_src, 'r') as f:
            records = json.load(f)

        database.engine.connect()
        for record in records:
            record_id = record['item'].split('/')[-1]
            concept_id = f"wikidata:{record_id}"
            if 'itemLabel' in record.keys():
                label = record['itemLabel']
            else:
                label = 'NULL'
            drug = schemas.Drug(label=label,
                                max_phase=None,
                                withdrawn=None,
                                trade_name=None,
                                aliases=[],
                                concept_identifier=concept_id,
                                other_identifiers=[])
            if self._entry_exists(concept_id):
                entry_exists = True
            else:
                self._concept_ids.append(concept_id)
                entry_exists = False
            if not entry_exists:
                self._load_therapy(concept_id, drug)
                other_ids = defaultdict(lambda: "NULL")
                if 'casRegistry' in record.keys():
                    id = record['casRegistry']
                    fmted = f"{IDENTIFIER_PREFIXES['casRegistry']}:{id}"
                    other_ids['casRegistry'] = fmted
                if 'pubchemCompound' in record.keys():
                    id = record['pubchemCompound']
                    fmted = f"{IDENTIFIER_PREFIXES['pubchemCompound']}:{id}"
                    other_ids['pubchemCompound'] = fmted
                if 'pubchemSubstance' in record.keys():
                    id = record['pubchemSubstance']
                    fmted = f"{IDENTIFIER_PREFIXES['pubchemSubstance']}:{id}"
                    other_ids['pubchemSubstance'] = fmted
                if 'rxnorm' in record.keys():
                    id = record['rxnorm']
                    fmted = f"{IDENTIFIER_PREFIXES['rxnorm']}:{id}"
                    other_ids['rxnorm'] = fmted
                if 'chembl' in record.keys():
                    id = record['chembl']
                    fmted = f"{IDENTIFIER_PREFIXES['chembl']}:{id}"
                    other_ids['chembl'] = fmted
                if 'drugbank' in record.keys():
                    id = record['drugbank']
                    fmted = f"{IDENTIFIER_PREFIXES['drugbank']}:{id}"
                    other_ids['drugbank'] = fmted
                self._load_other_ids(concept_id, other_ids)
            if 'alias' in record.keys():
                alias = record['alias']
                alias = alias.replace('"', '""')
                self._load_alias(concept_id, alias)

    def _sqlite_str(self, string):
        """Sanitizes string to use as value in SQL statement

        Some wikidata entries include items with single or double quotes,
        like wikidata:Q80863 (alias: 5'-(Tetrahydrogen triphosphate) Adenosine)
        and wikidata:Q55871701 (label: Wedding "sugar")
        """
        if string == "NULL":
            return "NULL"
        else:
            sanitized = string.replace('"', '""').replace("'", "''")
            return f"'{sanitized}'"

    def _load_alias(self, concept_id: str, alias: str):
        """Load alias"""
        database.engine.execute(f"""INSERT INTO aliases(alias, concept_id)
                VALUES({self._sqlite_str(alias)}, '{concept_id}');""")

    def _load_other_ids(self, concept_id: str, other_ids: defaultdict):
        """Load individual other_ids row
        Args:
            concept_id: a str corresponding to full namespaced Wikidata
                concept ID
            other_ids: defaultdict that returns an empty string if key
                is not contained
        """
        statement = f"""INSERT INTO other_identifiers(concept_id, chembl_id,
                    drugbank_id, rxnorm_id, pubchemcompound_id,
                    pubchemsubstance_id, casregistry_id)
                    VALUES(
                        {self._sqlite_str(concept_id)},
                        {self._sqlite_str(other_ids['chembl'])},
                        {self._sqlite_str(other_ids['drugbank'])},
                        {self._sqlite_str(other_ids['rxnorm'])},
                        {self._sqlite_str(other_ids['pubchemCompound'])},
                        {self._sqlite_str(other_ids['pubchemSubstance'])},
                        {self._sqlite_str(other_ids['casRegistry'])}
                    )"""
        database.engine.execute(statement)

    def _load_therapy(self, concept_id: str, drug: Drug):
        """Load individual therapy row"""
        statement = f"""INSERT INTO therapies(concept_id, label, src_name)
                    VALUES(
                        {self._sqlite_str(concept_id)},
                        {self._sqlite_str(drug.label)},
                        'Wikidata'
                    )"""
        database.engine.execute(statement)

    def _load_data(self, *args, **kwargs):
        """Load data - called from base clase init"""
        B.metadata.create_all(bind=database.engine)
        self._get_db()

        self._extract_data(*args, **kwargs)
        self._add_meta()
        self._transform_data()

    def _get_db(self, *args, **kwargs):
        """Get db - unclear if needed"""
        db = database.SessionLocal()
        try:
            yield db
        finally:
            db.close()

    def _add_meta(self, *args, **kwargs):
        insert_meta = f"""
            INSERT INTO meta_data(src_name, data_license, data_license_url,
                version, data_url)
            SELECT
                'Wikidata',
                'CC0 1.0',
                'https://creativecommons.org/publicdomain/zero/1.0/',
                '{self._version}',
                NULL
            WHERE NOT EXISTS (
                SELECT * FROM meta_data
                WHERE src_name = 'Wikidata'
            );
        """
        database.engine.execute(insert_meta)
