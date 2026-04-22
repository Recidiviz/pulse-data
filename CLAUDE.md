# CLAUDE.md

## Rules

These apply to **every task** without exception:

- **Python types**: Add type information to every function definition. Use modern types (`str | None` not `Optional[str]`). Avoid `Any` where reasonable.
- **License headers**: Use the current year (e.g., `Copyright (C) 2026 Recidiviz, Inc.`) in new Python file headers. Look at recently created files for the correct format.
- **No test imports in production**: Never import from `recidiviz/tests/` outside of `recidiviz/tests/`. Test utilities and constants must stay in test code.
- **No obvious comments**: Don't add inline comments that explain things obvious from reading the code, or that are only meaningful in the current conversation context.
- **TODO format**: Reference GitHub issues as `TODO(#12345)`. Use `TODO(XXXX)` as a placeholder before filing — this fails lint, forcing the task to be filed before merging. A closed issue doesn't necessarily mean the TODO was addressed.
- **Data privacy**: Never access data from Maine (`US_ME`) or California (`US_CA`). If a query might touch this data, flag it to the user and confirm before running.
- **US_ID vs US_IX**: Idaho uses two state codes (`US_ID` and `US_IX`) that historically shared the same codebase and data infrastructure. If working in Idaho-related code, confirm with the user which state code applies before proceeding.
- **GitHub CLI**: Use `gh` for all GitHub operations (PRs, issues, etc.) — it has authenticated access to the private repo.

## Development Commands

### Environment Setup

- Use Python 3.11 with uv for dependency management
- First time setup: `./initial_setup.sh`
- Manual sync: `uv sync --all-extras` or `make uv-sync`
- Run commands with: `uv run <cmd>`
- Activate environment: `source .venv/bin/activate` or `make uv-shell`
- Common tasks available via Makefile: `make help`

### Python Testing

Tests live in `recidiviz/tests/` and mirror the source path. For example,
`recidiviz/big_query/big_query_view.py` is tested in
`recidiviz/tests/big_query/big_query_view.py`.

- Run a specific test file: `uv run pytest recidiviz/tests/path/to/test_file.py`
- Run all tests: `uv run pytest recidiviz` (slow — normally handled by CI)
- Test configuration in `setup.cfg` with coverage settings

### Code Quality

- Lint: `make pylint` (runs differential pylint on changed files; requires code to be committed, i.e. HEAD != main)
- Type checking: `uv run mypy recidiviz/path/to/changed/file.py` (run on specific files; full-repo mypy is slow)
- Security checking: `uv run bandit` (configured in `.bandit`)
- Auto-formatting: `black` and `isort` run automatically via pre-commit hooks

### Docker Development

- Build dev image: `make docker-build-dev`
- Admin panel: `make docker-admin`

### Querying Actual Data

The BigQuery (BQ) MCP server allows querying actual data. If no BQ MCP server
is configured locally, use the `bq` CLI.

See the **Data privacy** rule above — never query ME or CA data.

## Codebase Architecture

### Core Components

- **`recidiviz/`** - Main package with domain-specific modules:
  - **`admin_panel/`** - Administrative web interface
  - **`aggregated_metrics/`** - BQ view generation framework for aggregated metrics views
  - **`airflow/`** - Apache Airflow logic for orchestration deployed in Google Cloud Composer
  - **`calculator/`** - Stores query logic for many (but not all) BQ views
  - **`case_triage/`** - Case triage and pathways functionality
  - **`documents/`** - Document storage and LLM-based extraction
  - **`ingest/`** - Data ingestion configuration for different states/sources
  - **`persistence/`** - Database schemas, entities, and data access
  - **`pipelines/`** - Apache Beam data processing pipelines (see [Pipelines Documentation](./recidiviz/pipelines/CLAUDE.md))
    - **`ingest/`** - Ingest pipeline
    - **`metrics/`** - Metric computation pipelines
    - **`supplemental/`** - State-specific supplemental dataset pipelines
    - **`batch_identity_clustering/`** - Identity resolution clustering pipeline
  - **`tools/`** - Scripts for local, CI, or Cloud Build use
  - **`validation/`** - Framework for validation and quality checks
  - **`workflows/`** - Workflow orchestration and ETL

### Key Patterns

- SQLAlchemy for database ORM (version pinned <2.0.0)
- Apache Beam for data processing pipelines
- Flask for web applications with Flask-SQLAlchemy-Session
- Google Cloud Platform integration throughout
- State-specific ingest configurations in `recidiviz/ingest/direct/regions/`

### Configuration

- Environment-specific settings managed via `recidiviz/utils/environment.py`
- Database migrations handled by Alembic
- Pre-commit hooks configured for code quality enforcement

## Additional Context

States are abbreviated as `US_XX`. `US_OZ` is a fake state used for testing.
Additional fake codes (`US_XX`, `US_YY`, `US_WW`) are available in unit tests
and should be used when testing generic functionality.

## Sub-module Documentation

- [Ingest Process](./recidiviz/ingest/CLAUDE.md)
- [Pipelines](./recidiviz/pipelines/CLAUDE.md)
- [BigQuery Tooling](./recidiviz/big_query/CLAUDE.md)
- [Document Extraction](./recidiviz/NOT_FOR_PRODUCTION_USE/documents/CLAUDE.md)
- [Task Eligibility Spans](./recidiviz/task_eligibility/CLAUDE.md)
- [Deploy Tooling and Versioning](./recidiviz/tools/deploy/CLAUDE.md)

## Skills

Skills are invocable workflows documented in `.claude/skills/[skill_name]/SKILL.md`.
Available skills are listed in the system prompt at session start.

# Security Rules

Security patterns and anti-patterns that apply to all code in this repository:

- @.claude/rules/security.md

# Personal preferences

Each developer can maintain a personal local settings file at
`.claude/pulse-data-local-settings.md`. This file is gitignored and never
checked in. Use it for personal preferences, local paths, or any
machine-specific context you want Claude to have.

- @.claude/pulse-data-local-settings.md
