# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2025-12-27

### Added
- Initial release of Paper Trail API
- DuckDB loader for loading DIME campaign finance data from HuggingFace
- PostgreSQL loader with streaming support for large datasets
- Flexible filtering system (cycle, state, amount, date, composite filters)
- CLI tool (`duckdb-loader`) for command-line data loading and querying
- DIME CSV to Parquet converter utility
- Comprehensive documentation including:
  - Raw data catalog with 20+ data sources
  - Contributing guidelines
  - Usage examples
- Development infrastructure:
  - CI/CD pipeline with lint, type check, and test jobs
  - Pre-commit hooks for code quality
  - Claude Code integration for automated reviews

### Data Sources
- HuggingFace dataset: `andyoneal/dime` (915M+ political contribution records)
- Election cycles: 1979-2024
- Coverage: Federal and state races

[Unreleased]: https://github.com/Operation-Hope/paper-trail-api/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Operation-Hope/paper-trail-api/releases/tag/v0.1.0
