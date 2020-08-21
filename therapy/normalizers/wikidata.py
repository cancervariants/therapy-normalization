from .base import Base, IDENTIFIER_PREFIXES, MatchType
from therapy import PROJECT_ROOT
import json
from therapy.models import Drug


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

    def normalize(self, term):
        """Normalize term using Wikidata"""
        if term in self._exact_index:
            match_keys = self._exact_index[term]
            match_type = 'match'
        elif term.lower() in self._lower_index:
            match_keys = self._lower_index[term.lower()]
            match_type = 'case-insensitive-match'
        else:
            return self.NormalizerResponse(term, None, tuple())
        if len(match_keys) > 1:
            match_type = 'ambiguous'
        therapy_records = list()
        for match_key in match_keys:
            match = self._records[match_key]
            response_record = match['therapy']
            therapy_records.append(response_record)
        return self.NormalizerResponse(
            term, match_type, tuple(therapy_records)
        )

    def _load_data(self, *args, **kwargs):
        with open(
            PROJECT_ROOT / 'data' / 'wikidata_medications.json', 'r'
        ) as f:
            self._data = json.load(f)
        self._exact_index = dict()
        self._lower_index = dict()
        self._records = dict()
        i = 0
        for record in self._data:
            i += 1
            record_id = record['item'].split('/')[-1]
            for k, v in record.items():
                if k == 'item':
                    k = 'wikidata'
                    v = record_id
                elif k == 'itemLabel':
                    k = 'label'
                elif k == 'therapy':
                    raise ValueError
                s = self._exact_index.setdefault(v, set())
                s.add(record_id)
                s = self._lower_index.setdefault(v.lower(), set())
                s.add(record_id)
                d = self._records.setdefault(record_id, dict())
                if k != 'wikidata':
                    s = d.setdefault(k, set())
                    s.add(v)
                else:
                    d[k] = v
                if k not in IDENTIFIER_PREFIXES:
                    continue
                v = f'{IDENTIFIER_PREFIXES[k]}:{v}'
                s = self._exact_index.setdefault(v, set())
                s.add(record_id)
                s = self._lower_index.setdefault(v.lower(), set())
                s.add(record_id)
                d = self._records[record_id]
                s = d.setdefault('other_identifiers', set())
                s.add(v)

        for k, record in self._records.items():
            assert len(record['label']) == 1
            params = {
                'label': list(record['label'])[0],
                'concept_identifier': f"wikidata:{record['wikidata']}",
                'aliases': list(record.get('alias', set())),
                'other_identifiers': list(record.get(
                    'other_identifiers', set()))
            }
            self._records[k]['therapy'] = Drug(**params)
