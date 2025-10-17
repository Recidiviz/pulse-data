# CLAUDE.md
Modify Claude.md and related files to add to claudes memory! If the developer
needs to explain things multiple times, consider adding the information we're
re-learning again and again to this file. Claude supports hierarchical
documentation as well as imports. You can add to this file directly from a
session using `/memory`. More documentation here:
https://docs.claude.com/en/docs/claude-code/memory

## Development Commands

### Environment Setup
- Use Python 3.11 with pipenv for dependency management
- Install dependencies: `pipenv sync --dev` (or `./initial_pipenv_setup.sh` on first run)
- Activate environment: `pipenv shell`
- Use `gh` to examine pull requests (PRs), create PRs, etc -- this is because the `gh` cli has been authenticated to access our private repo.

### Testing
- Run all tests: `pytest recidiviz`
- Run specific test: `pytest path/to/test_file.py`
- Test configuration in `setup.cfg` with coverage settings

### Code Quality
- Lint: `pipenv run pylint` or `./recidiviz/tools/lint/run_pylint.sh`
- Type checking: `mypy recidiviz`
- Security checking: `bandit` (configured in `.bandit` file)
- Auto-formatting: `black` and `isort` (configured in pre-commit hooks)
- Pre-commit hooks handle formatting automatically

### Docker Development
- Build dev image: `pipenv run docker-build-dev`
- Admin panel: `pipenv run docker-admin`
- Justice Counts: `pipenv run docker-jc`

### Querying actual data 
The BigQuery MCP server allows you to query for actual data. However, never
attempt to access any data from Maine or California. If you are worried a query
you're running may access this data, please flag this to user and make sure
they confirm before running the query.

## Codebase Architecture

### Core Components
- **`recidiviz/`** - Main package with domain-specific modules:
  - **`ingest/`** - Data ingestion pipelines for different states/sources
  - **`calculator/`** - Metrics calculation and analytics logic
  - **`persistence/`** - Database schemas, entities, and data access
  - **`pipelines/`** - Apache Beam data processing pipelines
  - **`validation/`** - Data validation and quality checks
  - **`admin_panel/`** - Administrative web interface
  - **`justice_counts/`** - Justice Counts application components
  - **`case_triage/`** - Case triage and pathways functionality
  - **`aggregated_metrics/`** - Aggregated metrics processing
  - **`workflows/`** - Workflow orchestration and ETL
  - **`tools/`** - Development and operational utilities

### Key Patterns
- Uses SQLAlchemy for database ORM (version pinned <2.0.0)
- Apache Beam for data processing pipelines
- Flask for web applications with Flask-SQLAlchemy-Session
- Google Cloud Platform integration throughout
- State-specific ingest configurations in `recidiviz/ingest/direct/regions/`

### Configuration
- Environment-specific settings managed via utils/environment.py
- Database migrations handled by Alembic
- Pre-commit hooks configured for code quality enforcement
- Docker Compose files for different service combinations

### Testing Philosophy
- Unit tests in parallel test directories (`recidiviz/tests/`)
- Pytest with xdist for parallel execution
- Code coverage tracking with pytest-cov
- Fixture directories excluded from test discovery

## Additional Context
- States are abbreviated as US_XX, where XX is the state code. US_ME = Maine, for example. US_OZ is fake state used for testing only.

### Important Views
- `workflows_views.client_record` and `workflows_views.resident_record` are two of the most important views leveraged by our front-end. If we need our front end to have access to data, it's quite possible the data should be added here as well.

## Data Ingestion Process
For detailed information about how raw data is imported, transformed through ingest views, and normalized into Recidiviz entities, see:

- [Ingest Process Documentation](./recidiviz/ingest/CLAUDE.md)
