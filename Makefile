# Local

APP := config

install-dev: pyproject.toml
	pip install --upgrade pip &&\
	pip install --editable .[all-dev]

lint:
	pylint --disable=R,C src/${APP}/*.py &&\
	pylint --disable=R,C tests/*.py

test:
	python -m pytest -vv --cov=src/${APP} tests

format:
	black src/${APP}/*.py &&\
	black tests/*.py

	isort src/${APP}/*.py &&\
	isort tests/*.py

local-all: install-dev lint format test
