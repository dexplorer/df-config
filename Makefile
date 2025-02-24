install: pyproject.toml
	pip install --upgrade pip &&\
	pip install --editable . 
	pip install --editable .[test]

lint:
	pylint --disable=R,C src/config/*.py &&\
	pylint --disable=R,C tests/*.py

test:
	python -m pytest -vv --cov=src/config tests

format:
	black src/config/*.py &&\
	black tests/*.py

all:
	install lint format test
