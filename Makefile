.PHONY: clean

requirements: Pipfile
	pipenv lock --requirements > requirements.txt
	pipenv lock --dev --requirements > requirements-dev.txt

clean:
	@find . -type f -name '*.pyc' -delete
	@find . -type d -name '__pycache__' | xargs rm -rf
	@rm -rf build/
	@rm -rf dist/
	@rm -f src/*.egg*
