install-dev: pyproject.toml
	pip install --upgrade pip &&\
	pip install --editable .[all-dev]

lint:
	pylint --disable=R,C src/config/*.py &&\
	pylint --disable=R,C tests/*.py

test:
	python -m pytest -vv --cov=src/config tests

format:
	black src/config/*.py &&\
	black tests/*.py

local-all: install-dev lint format test
