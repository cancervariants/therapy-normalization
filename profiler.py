"""This tool is used for profiling query return speed.
Call from command line with optional # of trials argument; otherwise,
will run 8 trials by default.
"""
from therapy.query import normalize
import random
from timeit import default_timer as timer
from sys import argv


QUERY_TERM_LIST = [
    # labels
    'ACETAMINOPHEN',
    '2,4-DICHLOROPHENOXYBUTYRIC ACID',
    # labels case insensitive
    'DaPANSUTRILE',
    'TmC-647055',
    # concept ID
    'chembl:CHEMBL1200807',
    'chembl:CHEMBL55895',
    # concept ID namespace case insensitive
    'wikidata:Q28852422',
    'chembl:CHEMBL1201274',
    # aliases
    'Adagen',
    '4-Hydroxy3-Methoxy Cinnamaldehyde',
    # aliases case insensitive
    'tcMDC-133999',
    '3-BROMO-Benzoic Acid',
    # trade name
    'BETAMETHASONE VALERATE',
    'M.V.I.-12 ADULT',
    # trade name case insensitive
    'THIOtHIXENE',
    'COLY-mYCIN S',
    # nonexistant
    'alksdfkjlwer',
    'roulkwernduel',
    # blank query
    '',
]


def run_trials(n=12):
    """Run n trials on a miscellaneous slate of queries"""
    queries_per_trial = []
    print(f"Running {n} trials")
    for i in range(n):
        to_randomize = QUERY_TERM_LIST[:]
        random.shuffle(to_randomize)
        queries_per_trial.append(to_randomize)
    times = []
    for i, queries in enumerate(queries_per_trial):
        start = timer()
        for query in queries:
            normalize(query)
        end = timer()
        duration = end - start
        print(f"Trial {i + 1}: {duration:.5f} seconds")
        times.append(duration)
    avg = sum(times) / len(times)
    max_val = max(times)
    min_val = min(times)
    print(f"Average: {avg:.5f} seconds")
    print(f"maximum: {max_val:.5f} seconds")
    print(f"minimum: {min_val:.5f} seconds")


if __name__ == '__main__':
    if len(argv) > 1:
        try:
            run_trials(int(argv[1]))
        except TypeError:
            raise Exception("Cannot parse {argv[1]} as an integer")
    else:
        run_trials()
