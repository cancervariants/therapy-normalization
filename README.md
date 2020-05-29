# Therapy Normalization
Services and guidelines for normalizing drug (and non-drug therapy) terms

## Running the tests

### Unit tests

Explain how to run the automated tests for this system

```
python3 -m pytest
```

### And coding style tests

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
