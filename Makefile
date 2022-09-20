.PHONY: clean, run

run:
	uvicorn therapy.main:app --reload --port=$(or $(port), 8000)

reqs: Pipfile
	pipenv lock --requirements > requirements.txt
	pipenv lock --dev --requirements > requirements-dev.txt

clean:
	@find . -type f -name '*.pyc' -delete
	@find . -type d -name '__pycache__' | xargs rm -rf
	@rm -rf build/
	@rm -rf dist/
	@rm -f src/*.egg*
