VENV = .venv
PYTHON = $(VENV)/bin/python3
PIP = $(VENV)/bin/pip


$(VENV)/bin/activate: requirements/dev.txt
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements/dev.txt


install: $(VENV)/bin/activate

clean:
	rm -rf __pycache__

fmt:
	black ./

lint:
	pylint dune_client/

types:
	mypy dune_client/ --strict

check: fmt lint types

test-unit:
	python -m pytest tests/unit

test-e2e:
	python -m pytest tests/e2e

test-all: test-unit test-e2e
