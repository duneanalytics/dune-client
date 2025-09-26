VENV = .venv
PYTHON = $(VENV)/bin/python3

# Install dependencies using uv
install:
	uv sync --no-dev

# Install with dev dependencies
install-dev:
	uv sync --dev

clean:
	rm -rf __pycache__ .tox dist

fmt:
	uv run ruff format

fmt-check:
	uv run ruff format --check

lint:
	uv run ruff check

lint-fix:
	uv run ruff check --fix

types:
	uv run mypy dune_client/ --strict

check: fmt lint types

test-unit:
	uv run python -m pytest tests/unit

test-e2e:
	uv run python -m pytest tests/e2e

test-all: test-unit test-e2e

test-tox:
	uv run tox
