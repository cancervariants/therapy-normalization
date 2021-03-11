# Therapy Normalization
Services and guidelines for normalizing drug (and non-drug therapy) terms

## Developer instructions
Following are sections include instructions specifically for developers.

### Installation
For a development install, we recommend using Pipenv. See the 
[pipenv docs](https://pipenv-fork.readthedocs.io/en/latest/#install-pipenv-today) 
for direction on installing pipenv in your compute environment.
 
Once installed, from the project root dir, just run:

```shell script
pipenv sync
```

### Deploying DynamoDB Locally

We use Amazon DynamoDB for our database. To deploy locally, follow [these instructions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html).

### Init coding style tests

Code style is managed by [flake8](https://github.com/PyCQA/flake8) and checked prior to commit.

We use [pre-commit](https://pre-commit.com/#usage) to run conformance tests.

This ensures:

* Check code style
* Check for added large files
* Detect AWS Credentials
* Detect Private Key

Before first commit run:

```
pre-commit install
```


### Running unit tests

Running unit tests is as easy as pytest.

```
pipenv run pytest
```

### Updating the gene normalization database

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

#### Update source(s)
The sources we currently use are: ChEMBL, NCIt, DrugBank, RxNorm, ChemIDplus, and Wikidata.

To update one source, simply set `--normalizer` to the source you wish to update. 

From the project root, run the following to update the ChEMBL source:

```
python3 -m therapy.cli --normalizer="chembl"
```

To update multiple sources, you can use the `normalizer` flag with the source names separated by spaces.

#### Update all sources

To update all sources, use the `--update_all` flag. 

From the project root, run the following to update all sources:

```
python3 -m therapy.cli --update_all
```

#### Specifying the database URL endpoint

To specify the database URL endpoint, simply set `--db_url` to the endpoint you want to use. You must also include `--normalizer` or `--update_all`.

From the project root, run the following to update all sources from the database with URL endpoint `http://localhost:8001`:

```
python3 -m therapy.cli --update_all --db_url="http://localhost:8001"
```

From the project root, run the following to update all sources from the database with URL endpoint `http://localhost:8000`:
```
python3 -m therapy.cli --update_all --dev
```


### Starting the therapy normalization service

From the project root, run the following:

```shell script
 uvicorn therapy.main:app --reload
```

Next, view the OpenAPI docs on your local machine: 

http://127.0.0.1:8000/therapy
