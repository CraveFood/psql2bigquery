setup:
	@pip install -U pip poetry

dependencies:
	@make setup
	@poetry install --no-root --extras sentry

update:
	@poetry update

test:
	@make check
	@make lint
	@make unit

check:
	@poetry check

lint:
	@echo "Checking code style ..."
	@poetry run pylint psql2bigquery
	@poetry run black --check .

clean:
	@rm -rf dist/ build/ *.egg-info/

publish:
	@make clean
	@printf "\nPublishing lib"
	@make setup
	@poetry config pypi-token.pypi $(PYPI_API_TOKEN)
	@poetry publish --build
	@make clean


.PHONY: setup dependencies update test check lint clean publish
