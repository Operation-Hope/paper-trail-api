# Contributing to Paper Trail API

Thank you for your interest in contributing to Paper Trail API! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) (recommended package manager)
- Git

### Getting Started

1. **Clone the repository**
   ```bash
   git clone https://github.com/Operation-Hope/paper-trail-api.git
   cd paper-trail-api
   ```

2. **Create a virtual environment and install dependencies**
   ```bash
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   uv sync --all-extras
   ```

3. **Install the duckdb-loader package in development mode**
   ```bash
   uv pip install -e duckdb_loader/
   ```

4. **Set up pre-commit hooks**
   ```bash
   uv run pre-commit install
   ```

5. **Copy environment template**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

## Development Workflow

### Code Quality

We use the following tools to maintain code quality:

- **[Ruff](https://docs.astral.sh/ruff/)** - Linting and formatting
- **[ty](https://github.com/astral-sh/ty)** - Type checking
- **[pytest](https://pytest.org/)** - Testing

### Running Checks Locally

```bash
# Linting
uv run ruff check .

# Auto-fix linting issues
uv run ruff check --fix .

# Formatting
uv run ruff format .

# Type checking
uv run ty check duckdb_loader/

# Run tests
uv run pytest tests/ -v

# Run tests with coverage
uv run pytest tests/ --cov=duckdb_loader --cov-report=term-missing
```

### Pre-commit Hooks

Pre-commit hooks run automatically on `git commit`. They check:
- Ruff linting and formatting
- Trailing whitespace
- YAML validity
- Large files
- Merge conflicts
- Private keys

To run hooks manually:
```bash
uv run pre-commit run --all-files
```

## Making Changes

### Branch Naming

Use descriptive branch names:
- `feature/description` - New features
- `fix/description` - Bug fixes
- `docs/description` - Documentation updates
- `refactor/description` - Code refactoring

### Commit Messages

Write clear, concise commit messages:
- Use present tense ("Add feature" not "Added feature")
- First line should be 50 characters or less
- Include context in the body if needed

Example:
```
Add cycle filter preset for recent elections

Adds `recent_cycles(n)` function that returns a CycleFilter
for the last N election cycles, defaulting to 4 (2018-2024).
```

### Pull Requests

1. Create a feature branch from `main`
2. Make your changes
3. Ensure all checks pass locally
4. Push your branch and create a PR
5. Fill out the PR template
6. Request review

PR titles should be descriptive and follow the same guidelines as commit messages.

## Testing

### Running Tests

```bash
# Run all tests
uv run pytest tests/ -v

# Run specific test file
uv run pytest tests/test_filters.py -v

# Run specific test class or function
uv run pytest tests/test_filters.py::TestCycleFilter -v
uv run pytest tests/test_filters.py::TestCycleFilter::test_single_cycle_match -v

# Run with coverage
uv run pytest tests/ --cov=duckdb_loader --cov-report=html
```

### Writing Tests

- Place tests in the `tests/` directory
- Name test files `test_*.py`
- Name test functions `test_*`
- Use fixtures from `conftest.py` for shared data
- Add markers for slow or integration tests:
  ```python
  @pytest.mark.slow
  def test_large_dataset():
      ...

  @pytest.mark.integration
  def test_postgres_connection():
      ...
  ```

### Test Coverage

We aim for high test coverage on core functionality:
- Filters and filter presets
- Schema creation and validation
- Data loading logic (unit tests with mocks)

## Project Structure

```
paper-trail-api/
├── duckdb_loader/           # Main Python package
│   └── duckdb_loader/       # Package source
│       ├── cli.py           # CLI commands
│       ├── filters.py       # Data filters
│       ├── loader.py        # DuckDB loading
│       ├── postgres_loader.py # PostgreSQL loading
│       └── schema.py        # Schema definitions
├── scripts/                 # Utility scripts
├── tests/                   # Test suite
├── docs/                    # Documentation
├── database/                # SQL schemas
├── examples/                # Usage examples
└── data/                    # Data storage (gitignored)
```

## Code Style

### Python

- Follow PEP 8 (enforced by Ruff)
- Use type hints for function signatures
- Write docstrings for public functions and classes
- Keep functions focused and small
- Prefer composition over inheritance

### Documentation

- Update README.md for user-facing changes
- Add docstrings for new public APIs
- Update examples if behavior changes

## Getting Help

- Open an issue for bugs or feature requests
- Check existing issues before creating new ones
- Join discussions in pull requests

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
