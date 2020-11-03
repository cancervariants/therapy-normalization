"""Analysis for match results from each source."""
import csv
from therapy import query


def __main__():
    with open('../data/civic_drugs.tsv', 'r') as f:
        with open('../analysis/match_results.tsv', 'w') as w:
            f.readline()
            writer = csv.writer(w, delimiter='\t')
            writer.writerow(
                ['Label', 'Wikidata Match', 'ChEMBL Match', 'DrugBank Match'])
            for line in f:
                name = line.strip().split('\t')[0]
                resp = query.normalize(name, keyed=True)

                wikidata_match = \
                    resp['source_matches']['Wikidata']['match_type']
                chembl_match = \
                    resp['source_matches']['ChEMBL']['match_type']
                drugbank_match = \
                    resp['source_matches']['DrugBank']['match_type']

                writer.writerow([name, wikidata_match, chembl_match,
                                 drugbank_match])


if __name__ == '__main__':
    __main__()
