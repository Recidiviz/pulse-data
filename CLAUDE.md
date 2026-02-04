# CLAUDE.md

Modify Claude.md and related files to add to claudes memory! If the developer
needs to explain things multiple times, consider adding the information we're
re-learning again and again to this file. Claude supports hierarchical
documentation as well as imports. You can add to this file directly from a
session using `/memory`. More documentation here:
https://docs.claude.com/en/docs/claude-code/memory

## Development Commands

### Environment Setup

- Use Python 3.11 with uv for dependency management
- First time setup: `./initial_setup.sh`
- Manual sync: `uv sync --all-extras` or `make uv-sync`
- Run commands with: `uv run <cmd>`
- Activate environment: `source .venv/bin/activate` or `make uv-shell`
- Common tasks available via Makefile: `make help`
- Whenever possible, use `gh` to examine pull requests (PRs), create PRs, etc --
  this is because the `gh` cli has been authenticated to access our private
  repo.

### Python testing

Tests for Python code live in `recidiviz/tests` directory. Generally, code is
tested in a file whose name / path mirrors its name. For example,
`recidiviz/big_query/big_query_view.py` has tests in
`recidiviz/tests/big_query/big_query_view.py`.

Running / configuring tests:

- Run tests in a specific file: `pytest recidiviz/tests/path/to/test_file.py`
- Run all tests: `pytest recidiviz`
  - This is not a common development flow as it is slow and will be handled by
    CI.
- Test configuration in `setup.cfg` with coverage settings
- To make test discovery faster, exclude fixture directories

### Python Code Style

- When creating new Python files, use the current year in the license header
  (e.g., `Copyright (C) 2026 Recidiviz, Inc.`). Look at recently created files
  for the correct format.
- Always add type information to every python function definition. Avoid using
  Any where reasonable. Use modern python types, i.e. `str | None` instead of
  `Optional[str]`.
- Don't add inline comments to explain things that are blatantly obvious just by
  reading the code or to explain something that is only meaningful in the
  context of the current conversation.
- Never import from `recidiviz/tests/` in production code (code outside of
  `recidiviz/tests/`). Test utilities and constants should stay in test code.

### Code Quality

- Lint:
  - `./recidiviz/tools/lint/run_pylint.sh` - Runs differential `pylint` on
    changed files and also some additional checks. Only works if on a branch
    where code has been committed (i.e. HEAD != main).
  - `make pylint` - Shortcut for the above
- Type checking: `uv run mypy recidiviz`
- Security checking: `uv run bandit` (configured in `.bandit` file)
- Auto-formatting: `black` and `isort` (configured in pre-commit hooks)
- Pre-commit hooks handle formatting automatically

### Docker Development

- Build dev image: `make docker-build-dev`
- Admin panel: `make docker-admin`

### Github

- TODOs in code reference tasks in Github using a specific format:
  - `TODO(#12345)` refers to a TODO that should be addressed by
    https://github.com/Recidiviz/pulse-data/issues/12345
  - `TODO(Recidiviz/looker#123)` refers to a TODO that should be addressed by
    https://github.com/Recidiviz/looker/issues/123
  - `TODO(XXXX)` can be used as a placeholder before filing a task - this will
    cause lint to fail, ensuring the task gets filed before merging
  - If you find a TODO in code that refers to a closed issue, it does not
    necessarily mean it has been addressed, as tasks are sometimes erroneously
    closed while TODOs still exist in code.

### Querying actual data

The BigQuery (BQ) MCP server allows you to query for actual data. If there is
not a BQ MCP server configured locally, you should use the `bq` command line
util to query data.

However, never attempt to access any data from Maine or California. If you are
worried a query you're running may access this data, please flag this to user
and make sure they confirm before running the query.

## Codebase Architecture

### Core Components

- **`recidiviz/`** - Main package with domain-specific modules:
  - **`admin_panel/`** - Administrative web interface
  - **`aggregated_metrics/`** - BQ view generation framework for aggregated
    metrics views
  - **`airflow/`** - Apache Airflow logic for orchestration deployed in Google
    Cloud Composer
  - **`calculator/`** - Stores query logic for many (but not all) of our BQ
    views
  - **`case_triage/`** - Case triage and pathways functionality
  - **`ingest/`** - Data ingestion configuration for different states/sources
  - **`persistence/`** - Database schemas, entities, and data access
  - **`pipelines/`** - Apache Beam data processing pipelines
    - **`ingest/state`** - Pipeline for ingest (reads configurations from
      `recidiviz/ingest/direct/regions`)
  - **`tools/`** - Scripts that can be run locally, in CI, or in Cloud Build
    jobs for development / operational uses.
  - **`validation/`** - Framework for running validation and quality checks
  - **`workflows/`** - Workflow orchestration and ETL

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

## Additional Context

- States are abbreviated as US_XX, where XX is the state code. US_ME = Maine,
  for example. US_OZ is fake state used for testing only. Some additional "fake"
  state codes (e.g. US_XX, US_YY, US_WW) are available in the context of
  unittests. These should be used when testing generic functionality.

## Data Ingestion Process

For detailed information about how raw data is imported, transformed through
ingest views, and normalized into Recidiviz entities, see:

- [Ingest Process Documentation](./recidiviz/ingest/CLAUDE.md)

## BigQuery Tooling and Libraries

For detailed information about how to work with BigQuery infrastructure,
tooling, and libraries:

- [BigQuery Documentation](./recidiviz/big_query/CLAUDE.md)

## Skills

Skills are specific workflows or procedures that Claude can follow. Each skill
is documented in its own directory under `.claude/skills/`. To create a new
skill, add a directory with a `SKILL.md` file:

- **`.claude/skills/[skill_name]/SKILL.md`** - Documentation for a specific
  skill

Available skills:

- [Commit](./.claude/skills/commit/SKILL.md) - Commit uncommitted changes with
  clear commit messages
- [Create PR](./.claude/skills/create-pr/SKILL.md) - Create a GitHub PR for the
  current branch
- [Create Recidiviz Data GitHub Tasks](./.claude/skills/create_recidiviz_data_github_tasks/SKILL.md)
- [Maintain Skill Files](./.claude/skills/maintain_skill_files/SKILL.md)
- [Maintain CLAUDE.md Documentation](./.claude/skills/maintain_claude_md/SKILL.md)
- [Upgrade Cloud Composer](./.claude/skills/upgrade_cloud_composer/SKILL.md)

# Personal preferences

- @.claude/pulse-data-local-settings.md
