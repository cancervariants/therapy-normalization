"""Analysis for match results from each source."""
import csv
from therapy import query


def __main__():
    with open('../data/civic_drugs.tsv', 'r') as f:
        with open('../analysis/match_results.tsv', 'w') as w:
            f.readline()
            writer = csv.writer(w, delimiter='\t')
            writer.writerow(
                ['Label', 'Wikidata Match', 'ChEMBL Match', 'DrugBank Match',
                    'NCIt Match'])
            for line in f:
                line_spl = line.strip().split('\t')
                name = line_spl[0]
                if len(line_spl) > 1:
                    concept_id = line_spl[1]
                    resp = query.normalize(f"ncit:{concept_id}", keyed=True)
                    ncit_match = \
                        resp['source_matches']['NCIt']['match_type']
                resp = query.normalize(name, keyed=True)
                # resp = query.normalize(name, keyed=True)

                wikidata_match = \
                    resp['source_matches']['Wikidata']['match_type']
                chembl_match = \
                    resp['source_matches']['ChEMBL']['match_type']
                drugbank_match = \
                    resp['source_matches']['DrugBank']['match_type']
                if not ncit_match:
                    ncit_match = \
                        resp['source_matches']['NCIt']['match_type']

                writer.writerow([name, wikidata_match, chembl_match,
                                 drugbank_match, ncit_match])


if __name__ == '__main__':
    __main__()
