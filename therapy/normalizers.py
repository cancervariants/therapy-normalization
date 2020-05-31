"""Methods to normalize therapy terms."""
from abc import ABC, abstractmethod
from therapy import PROJECT_ROOT
import json
from collections import namedtuple


class Base(ABC):
    """The normalizer base class."""

    def __init__(self, *args, **kwargs):
        """Initialize the normalizer."""
        self._data = None
        self._load_data(*args, **kwargs)

    @abstractmethod
    def _load_data(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def normalize(self, term):
        """Normalize term to wikidata concept"""
        raise NotImplementedError

    NormalizerResponse = namedtuple(
        'NormalizerResponse',
        ['input_term', 'normalized_label', 'aliases', 'match_type']
    )


class Wikidata(Base):
    """A normalizer using the Wikidata resource."""

    SPARQL_QUERY = """
SELECT ?item ?itemLabel ?casRegistry ?pubchemCompound ?pubchemSubstance ?chembl
  ?rxnorm ?drugbank ?altLabel WHERE {
  ?item (wdt:P31/(wdt:P279*)) wd:Q12140.
  OPTIONAL {
    ?item skos:altLabel ?altLabel.
    FILTER((LANG(?altLabel)) = "en")
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
            match_key = self._exact_index[term]
            match_type = 'match'
        elif term.lower() in self._lower_index:
            match_key = self._lower_index[term.lower()]
            match_type = 'case-insensitive-match'
        else:
            return self.NormalizerResponse(term, None, dict(), None)
        if len(match_key) > 1:
            return self.NormalizerResponse(term, None, dict(), 'ambiguous')
        match = self._records[list(match_key)[0]]
        return self.NormalizerResponse(
            term, match['itemLabel'], match, match_type
        )

    def _load_data(self, *args, **kwargs):
        with open(
            PROJECT_ROOT / 'data' / 'wikidata_medications.json', 'r'
        ) as f:
            self._data = json.load(f)
        self._exact_index = dict()
        self._lower_index = dict()
        self._records = dict()
        for record in self._data:
            record_id = record['item'].split('/')[-1]
            for k, v in record.items():
                s = self._exact_index.setdefault(v, set())
                s.add(record_id)
                s = self._lower_index.setdefault(v.lower(), set())
                s.add(record_id)
                d = self._records.setdefault(record_id, dict())
                s = d.setdefault(k, set())
                s.add(v)
