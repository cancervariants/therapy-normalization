This directory contains a number of utility scripts for generating test data and executing tests, primarily in CI.

### Source data generation

The source data generation scripts don't require any extra arguments; they can simply be executed a la `python3 tests/scripts/build_chembl_data.py`.
Most of them use a `TEST_IDS` constant to define data points to include (NCIt is the exception, it's a little weird). For the most part, adding new IDs is fairly straightforward, although RxNorm requires a little bit of sleuthing to track down additional trade name/brand IDs.

### Fixture data generation

ChEMBL and HemOnc require a disease normalization database connection to process drug indication data. CI tests employ a static lookup dictionary loaded from a JSON file. `python3 tests/scripts/build_disease_normalizer_data.py` will update all mappings employed by the test HemOnc and ChEMBL data. This means that those data files should be updated first before running this script.

### DynamoDB fixture runner

`dynamodb_run.sh` acquires and initiates a SQLite server with a DynamoDB-compliant frontend for testing all database-related functions locally. This script is used in GitHub Actions as a temporary instance for running tests.
