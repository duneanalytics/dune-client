# Development Configuration for Cursor

## Package Manager

Always use `uv` for Python package management in this project.

### Basic uv commands:
- **Install dependencies**: `uv sync` or `uv install`
- **Add new packages**: `uv add <package-name>`
- **Add dev dependencies**: `uv add --dev <package-name>`
- **Remove packages**: `uv remove <package-name>`
- **Run scripts**: `uv run <command>`

## Development Workflow

**Use the Makefile for all development tasks.** The project has a comprehensive Makefile that handles testing, linting, formatting, and other development tasks.

### Installation & Setup
- **Install production dependencies**: `make install`
- **Install with dev dependencies**: `make install-dev`

### Code Quality & Formatting
- **Format code**: `make fmt`
- **Check formatting**: `make fmt-check`
- **Lint code**: `make lint`
- **Fix linting issues**: `make lint-fix`
- **Type checking**: `make types`
- **Run all checks**: `make check` (formats, lints, and type checks)

### Testing
- **Run unit tests**: `make test-unit`
- **Run e2e tests**: `make test-e2e`
- **Run all tests**: `make test-all`
- **Run tox tests**: `make test-tox`

### Cleanup
- **Clean build artifacts**: `make clean`

## Do NOT use directly:
- `pip install` (use `uv` or Makefile targets)
- `pytest` directly (use `make test-unit` or `make test-e2e`)
- `ruff` directly (use `make lint`, `make fmt`, etc.)
- `mypy` directly (use `make types`)

## Project Structure
This project uses `uv` for dependency management (see `uv.lock` and `pyproject.toml`) and provides a Makefile for standardized development workflows. Always prefer Makefile targets over running tools directly.