# Therapy Normalization
Services and guidelines for normalizing drug (and non-drug therapy) terms

## Developer instructions
The following sections include instructions specifically for developers.

### Installation
For a development install, we recommend using Pipenv. See the
[pipenv docs](https://pipenv-fork.readthedocs.io/en/latest/#install-pipenv-today)
for direction on installing pipenv in your compute environment.

Once installed, from the project root dir, just run:

```commandline
pipenv sync
```

### Deploying DynamoDB Locally

We use Amazon DynamoDB for data storage. To deploy locally, follow [these instructions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html).

### Initialize development environment

Code style is managed by [flake8](https://github.com/PyCQA/flake8) and checked prior to commit.

We use [pre-commit](https://pre-commit.com/#usage) to run conformance tests.

This ensures:

* Style correctness
* No large files
* AWS credentials are present
* Private key is present

Pre-commit *must* be installed before your first commit. Use the following command:

```commandline
pre-commit install
```

### Running tests

Unit tests are run with pytest.

```commandline
pipenv run pytest
```

We also provide [Tox](https://tox.wiki/en/latest/index.html) settings to test in multiple environments and check for proper type annotations and code style. If interpreters for Python 3.8 and Python 3.9 are present, the following will run all tests for all environments:

```commandline
tox
```

### Updating the database

Before you use the CLI to update the database, run the following in a separate terminal to start DynamoDB on `port 8000`:

```
java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb
```

To change the port, simply add `-port value`.

### Setting Environment Variables
RxNorm requires a UMLS license, which you can register for one [here](https://www.nlm.nih.gov/research/umls/index.html).
You must set the `RxNORM_API_KEY` environment variable to your API key. This can be found in the [UTS 'My Profile' area](https://uts.nlm.nih.gov/uts/profile) after singing in.
```shell script
export RXNORM_API_KEY={rxnorm_api_key}
```

HemOnc.org data requires a Harvard Dataverse API key. After creating a user account on the Harvard Dataverse website, you can follow [these instructions](https://guides.dataverse.org/en/latest/user/account.html) to generate a key. Once you have a key, set the following environment variable:

```shell script
export DATAVERSE_API_KEY={your api key}
```

#### Update source(s)
The Therapy Normalizer currently aggregates therapy data from:
* [ChEMBL](https://www.ebi.ac.uk/chembl/)
* [ChemIDPlus](https://chem.nlm.nih.gov/chemidplus/)
* [DrugBank](https://go.drugbank.com/) (using CC0 data only)
* [Drugs@FDA](https://www.accessdata.fda.gov/scripts/cder/daf/)
* [The IUPHAR/BPS Guide to PHARMACOLOGY](https://www.guidetopharmacology.org/)
* [HemOnc.org](https://hemonc.org/wiki/Main_Page) (using CC-BY data only).
* [The National Cancer Institute Thesaurus](https://ncithesaurus.nci.nih.gov/ncitbrowser/)
* [RxNorm](https://www.nlm.nih.gov/research/umls/rxnorm/index.html)
* [Wikidata](https://www.wikidata.org/wiki/Wikidata:Main_Page)

To update source(s), simply set `--normalizer` to the source(s) you wish to update separated by spaces. For example, the following command updates ChEMBL and Wikidata:

```commandline
python3 -m therapy.cli --normalizer="chembl wikidata"
```

You can update all sources at once with the `--update_all` flag:

```commandline
python3 -m therapy.cli --update_all
```

The `data/` subdirectory within the package source should house all desired input data. Files for all sources should follow the naming convention demonstrated below (with version numbers/dates changed where applicable).

```
therapy/data
├── chembl
│   └── chembl_27.db
├── chemidplus
│   └── chemidplus_20200327.xml
├── drugbank
│   └── drugbank_5.1.8.csv
├── guidetopharmacology
│   ├── guidetopharmacology_ligand_id_mapping_2021.3.tsv
│   └── guidetopharmacology_ligands_2021.3.tsv
├── hemonc
│   ├── hemonc_concepts_20210225.csv
│   ├── hemonc_rels_20210225.csv
│   └── hemonc_synonyms_20210225.csv
├── ncit
│   └── ncit_20.09d.owl
├── rxnorm
│   ├── drug_forms.yaml
│   └── rxnorm_20210104.RRF
└── wikidata
    └── wikidata_20210425.json
```

Updates to the HemOnc source depend on the [Disease Normalizer](https://github.com/cancervariants/disease-normalization) service. If the Disease Normalizer database appears to be empty or incomplete, updates to HemOnc will also trigger a refresh of the Disease Normalizer database. See its README for additional data requirements.

### Create Merged Concept Groups
The `/normalize` endpoint relies on merged concept groups.  The `--update_merged` flag generates these groups:

```commandline
python3 -m therapy.cli --update_merged
```

#### Specifying the database URL endpoint

The default URL endpoint is `http://localhost:8000`.
There are two different ways to specify the database URL endpoint.

The first way is to set the `--db_url` flag to the URL endpoint.
```commandline
python3 -m therapy.cli --update_all --db_url="http://localhost:8001"
```

The second way is to set the environment variable `THERAPY_NORM_DB_URL` to the URL endpoint.
```commandline
export THERAPY_NORM_DB_URL="http://localhost:8001"
python3 -m therapy.cli --update_all
```

### Starting the therapy normalization service

From the project root, run the following:

```commandline
uvicorn therapy.main:app --reload
```

Next, view the OpenAPI docs on your local machine:

http://127.0.0.1:8000/therapy
