# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Standalone script for diagnosing PG bug tickets using an Anthropic agentic loop.

Intended to run as a Cloud Build step. Reads issue metadata and secrets
from environment variables, runs the agent, and posts the diagnosis to GitHub.

Features:
- Conditionally loads diagnosis prompts based on product area labels
  (workflows, tasks, insights). Falls back to all three if none specified.
- Deduplication: skips issues that already have a diagnosis comment
  (override with FORCE_RERUN=1).
- Token usage logging per iteration and in the GitHub comment footer.
- Docker image caching via Artifact Registry.

To test, submit a Cloud Build job directly:

    gcloud builds submit recidiviz/tools/claude_workflows/ \\
      --config=recidiviz/tools/claude_workflows/pg_ticket_diagnosis/cloudbuild.yaml \\
      --substitutions="_ISSUE_NUMBER=<NUMBER>,\\
        _ISSUE_TITLE=$(gh issue view <NUMBER> --repo Recidiviz/recidiviz-dashboards --json title --jq '.title' | base64),\\
        _ISSUE_BODY=$(gh issue view <NUMBER> --repo Recidiviz/recidiviz-dashboards --json body --jq '.body' | base64),\\
        _ISSUE_REPO=Recidiviz/recidiviz-dashboards,\\
        _REPO_BRANCH=<BRANCH>,\\
        _PRODUCT_AREAS=workflows,\\
        _FORCE_RERUN=1" \\
      --project=recidiviz-staging

Replace <NUMBER> with the issue number and <BRANCH> with the branch to clone
(defaults to main). _PRODUCT_AREAS is a comma-separated list of product areas
(workflows, tasks, insights); leave empty for all. Requires GCP setup via
setup_gcp.sh.
"""
import base64
import logging
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from itertools import islice
from typing import Any, Callable
from zoneinfo import ZoneInfo

import anthropic
import google.auth
import google.auth.transport.requests
import requests
from google.auth import impersonated_credentials
from google.cloud import bigquery, secretmanager

# The Cloud Build container only ships run_pg_ticket_diagnosis.py; the rest
# of the recidiviz package lives in the cloned repo at REPO_PATH (set by
# cloudbuild.yaml). Prepend it so we can import recidiviz.utils.github.
sys.path.insert(0, os.environ.get("REPO_PATH", "."))

# pylint: disable=wrong-import-position
from claude_agent import (  # type: ignore[import-not-found]  # noqa: E402
    AgentConfig,
    AgentFailure,
    run_agent_loop,
)
from pii_doc_parser_utils import (  # type: ignore[import-not-found]  # noqa: E402
    find_issue_section,
    parse_doc,
)

from recidiviz.utils.github import (  # noqa: E402
    GITHUB_ISSUE_OR_COMMENT_BODY_MAX_LENGTH,
    RECIDIVIZ_DASHBOARDS_REPO,
    github_helperbot_client,
    upsert_issue_comment,
)
from recidiviz.utils.string_formatting import truncate_string_if_necessary  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_AGENT_ITERATIONS = 40
MODEL = "claude-opus-4-20250514"
SCRUB_MODEL = "claude-haiku-4-5-20251001"
AGENT_TIMEZONE = ZoneInfo("US/Eastern")

# Marker used to replace redacted PII in posted comments. Both the deterministic
# external-ID scrub and the Haiku scrub system prompt reference this so the
# output is consistent.
PII_REDACTION_MARKER = "[REDACTED PII]"


@dataclass(frozen=True)
class RuntimeConfig:
    """Bundles every environment variable read by this script.

    Loaded once at the top of main(); all downstream functions take the
    relevant fields as parameters rather than reading os.environ directly.
    """

    gcp_project: str
    bq_project: str
    repo_path: str
    build_id: str
    sa_email: str
    prompts_dir: str


def _load_runtime_config() -> RuntimeConfig:
    """Read every env var the script depends on, in one place."""
    gcp_project = get_env("GCP_PROJECT_ID")
    bq_project = os.environ.get("BQ_PROJECT", "recidiviz-staging")
    repo_path = os.environ.get("REPO_PATH", ".")
    build_id = get_env("BUILD_ID")
    sa_email = os.environ.get(
        "SA_EMAIL",
        f"diagnosis-for-pg-ticket@{gcp_project}.iam.gserviceaccount.com",
    )
    prompts_dir = os.environ.get(
        "PROMPTS_DIR",
        os.path.join(repo_path, ".claude/skills/investigate-pg-ticket"),
    )
    return RuntimeConfig(
        gcp_project=gcp_project,
        bq_project=bq_project,
        repo_path=repo_path,
        build_id=build_id,
        sa_email=sa_email,
        prompts_dir=prompts_dir,
    )


# ── helpers ──────────────────────────────────────────────────────────────────


class DiagnosisContext:
    """Per-run mutable state: BQ client and external ID tracker.

    Created once in main() and threaded through tool handlers so that
    separate runs (e.g. in tests) don't share state.
    """

    def __init__(self) -> None:
        self._bq_client: bigquery.Client | None = None
        self.known_external_ids: set[str] = set()

    def get_bq_client(self) -> bigquery.Client:
        if self._bq_client is None:
            self._bq_client = bigquery.Client()
        return self._bq_client

    def register_external_ids(self, ids: list[str]) -> None:
        self.known_external_ids.update(s for s in ids if s and s.strip())

    def scrub_known_external_ids(self, text: str) -> str:
        """Hard-replace any external ID seen during this run with the redaction marker."""
        if not self.known_external_ids:
            return text
        for eid in sorted(self.known_external_ids, key=len, reverse=True):
            text = re.sub(rf"\b{re.escape(eid)}\b", PII_REDACTION_MARKER, text)
        return text


def get_env(name: str) -> str:
    """Read a required environment variable."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _decode_base64_env(name: str) -> str:
    """Read a required env var, base64-decoding it if possible."""
    raw = get_env(name)
    try:
        return base64.b64decode(raw).decode()
    except Exception:
        return raw


def get_secret(name: str, gcp_project: str) -> str:
    """Read a secret from env vars (Cloud Build) or fall back to Secret Manager (local)."""
    env_map = {
        "pg_diagnosis_claude_api_key": "ANTHROPIC_API_KEY",
        "pg_diagnosis_github_token": "GITHUB_TOKEN",  # nosec B105 - env var name, not a credential
    }
    env_name = env_map.get(name, name)
    value = os.environ.get(env_name)
    if value:
        return value

    # Secret Manager fallback for local invocations where env vars aren't set.
    sm_map = {
        "pg_diagnosis_github_token": "github_deploy_script_pat",  # nosec B105 - Secret Manager resource name, not a credential
    }
    client = secretmanager.SecretManagerServiceClient()
    sm_name = sm_map.get(name, name)
    path = f"projects/{gcp_project}/secrets/{sm_name}/versions/latest"
    return client.access_secret_version(name=path).payload.data.decode()


DIAGNOSIS_MARKER = "<!-- pg-diagnosis-agent -->"
FOLLOW_UP_MARKER = "<!-- pg-diagnosis-followup -->"


def issue_has_marker(repo: str, issue_number: int, marker: str) -> bool:
    """Return True iff any existing comment on the issue starts with the marker."""
    issue = github_helperbot_client().get_repo(repo).get_issue(issue_number)
    return any(c.body.startswith(marker) for c in issue.get_comments())


def _post_marked_comment(
    repo: str,
    issue_number: int,
    body: str,
    marker: str = DIAGNOSIS_MARKER,
) -> None:
    """Upsert a comment whose body is prefixed with the given marker."""
    upsert_issue_comment(
        github_client=github_helperbot_client(),
        repo=repo,
        issue_number=issue_number,
        body=f"{marker}\n{body}",
        prefix=marker,
    )


# ── PII doc lookup ───────────────────────────────────────────────────────────

GITHUB_PII_DOC_ID = "1hYq--Xw6D5Lu96pSFVGeNu9AuxMNtB5F4ltI2VE9FZs"


class DiagnosisFailure(AgentFailure):
    """Signals a hard failure that should abort the agent and post a distinct message.

    Subclasses provide the user-facing headline and guidance; the exception's
    own str() (i.e. the message passed to the constructor) is the technical
    detail shown under **Detail:** in the posted comment.
    """

    HEADLINE = ""
    GUIDANCE = ""

    def user_message(self) -> str:
        return (
            f"⚠️ Automated diagnosis could not complete — {self.HEADLINE}\n\n"
            f"**Detail:** {self}\n\n"
            f"{self.GUIDANCE}"
        )


class PIIFetchError(DiagnosisFailure):
    """Raised when PII doc lookup fails due to auth or API errors."""

    HEADLINE = (
        "**failed to fetch the PII document** from go/github-pii. The agent "
        "never got as far as reading any PII for this issue."
    )
    GUIDANCE = (
        "Likely causes: the Cloud Build service account is missing Google Docs "
        "API access, the document was moved/renamed, or the Docs API returned a "
        "transient error. Investigate the service account permissions and re-run."
    )


class PIINotFoundError(DiagnosisFailure):
    """Raised when the PII doc has no entry for the given issue."""

    HEADLINE = (
        "the PII document was **fetched successfully**, but it has **no entry "
        "for this issue**."
    )
    GUIDANCE = (
        "Please add an entry for this issue to go/github-pii (including "
        "external IDs) and then re-run the diagnosis."
    )


class PersonIDLookupError(DiagnosisFailure):
    """Raised when external IDs from PII don't match any person in BQ."""

    HEADLINE = (
        "the PII was **fetched successfully**, but the external IDs it contained "
        "**could not be resolved to person IDs in BigQuery**."
    )
    GUIDANCE = (
        "Possible causes:\n"
        "- The external IDs in go/github-pii are incorrect or malformed.\n"
        "- The diagnosis service account is missing row-access-policy group "
        "memberships (see `recidiviz/tools/claude_workflows/pg_ticket_diagnosis/setup_gcp.sh`)."
    )


def fetch_pii_for_issue(issue_number: str, sa_email: str) -> str:
    """Fetch the PII section for the given issue from the go/github-pii doc."""
    try:
        scopes = ["https://www.googleapis.com/auth/documents.readonly"]
        credentials, _ = google.auth.default()
        # Compute engine credentials (Cloud Build) need impersonation to get scoped tokens.
        credentials = impersonated_credentials.Credentials(
            source_credentials=credentials,
            target_principal=sa_email,
            target_scopes=scopes,
        )
        credentials.refresh(google.auth.transport.requests.Request())
        url = f"https://docs.googleapis.com/v1/documents/{GITHUB_PII_DOC_ID}"
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {credentials.token}"},
            timeout=30,
        )
        doc = resp.json()
        if "error" in doc:
            raise PIIFetchError(
                f"Google Docs API error: {doc['error'].get('message', 'unknown')}"
            )
        lines = parse_doc(doc)
        section = find_issue_section(lines, issue_number)
        if section:
            return "\n".join(section)
        raise PIINotFoundError(
            f"Could not find issue {issue_number} in the PII document."
        )
    except (PIIFetchError, PIINotFoundError):  # pylint: disable=try-except-raise
        # Re-raise so the specific failure type isn't swallowed and re-wrapped
        # as PIIFetchError by the broader except below.
        raise
    except Exception as e:
        logger.exception("Failed to fetch PII doc")
        raise PIIFetchError(f"Error fetching PII doc: {e}") from e


# ── tool implementations ────────────────────────────────────────────────────


def _query_rows(
    sql: str, ctx: DiagnosisContext, limit: int = 51
) -> list[bigquery.table.Row]:
    """Run a BQ query and return up to `limit` rows. Raises on any BQ error."""
    return list(islice(ctx.get_bq_client().query(sql).result(), limit))


def _format_rows_as_table(rows: list[bigquery.table.Row]) -> str:
    """Format BQ rows as a pipe-delimited table, truncating at 50 rows."""
    if not rows:
        return "Query returned no rows."
    headers = list(rows[0].keys())
    lines = [" | ".join(headers)]
    lines += [" | ".join(str(r[h]) for h in headers) for r in rows[:50]]
    if len(rows) > 50:
        lines.append(
            "\n... truncated: showing first 50 rows. Add a LIMIT or WHERE clause to narrow results."
        )
    return "\n".join(lines)


def get_table_schema(dataset: str, table: str, ctx: DiagnosisContext) -> str:
    try:
        sql = f"""SELECT column_name, data_type
FROM `{dataset}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{table}'
ORDER BY ordinal_position"""
        rows = _query_rows(sql, ctx, limit=10_000)
        if not rows:
            return f"Table '{table}' not found in dataset '{dataset}'."
        return "\n".join(f"{r['column_name']} ({r['data_type']})" for r in rows)
    except Exception as e:
        logger.exception("Schema lookup failed")
        return f"Schema error: {e}"


def run_bq_query(sql: str, ctx: DiagnosisContext) -> str:
    try:
        return _format_rows_as_table(_query_rows(sql, ctx))
    except Exception as e:
        logger.exception("BigQuery query failed")
        return f"BigQuery error: {e}"


def _resolve_within_repo(
    user_path: str | None, repo_path: str
) -> tuple[str, str] | None:
    """Resolve a tool-supplied path against the repo root, keeping it inside.

    The tool `path` argument is attacker-controllable via prompt injection in
    the GitHub issue body, so we canonicalize both the repo root and the
    candidate path and reject anything that escapes the root (e.g. "../..").
    Returns (resolved_absolute_path, resolved_repo_root) on success, or None
    if the candidate escapes the repo. Passing None resolves to the repo root.
    """
    repo = os.path.realpath(repo_path)
    if user_path is None:
        return repo, repo
    candidate = os.path.realpath(os.path.join(repo, user_path))
    if candidate != repo and not candidate.startswith(repo + os.sep):
        return None
    return candidate, repo


def search_codebase(query: str, repo_path: str, path: str | None = None) -> str:
    """Search the local repo checkout using grep."""
    try:
        resolved = _resolve_within_repo(path, repo_path)
        if resolved is None:
            return f"Invalid path: {path} (must stay within the repo)"
        search_dir, repo = resolved
        result = subprocess.run(  # nosec B603, B607 - invoking system grep with controlled args
            [
                "grep",
                "-r",
                "-n",
                "-I",
                "-H",
                "--include=*.py",
                "--include=*.sql",
                "--include=*.yaml",
                "--include=*.md",
                query,
                search_dir,
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if not result.stdout.strip():
            return "No results found."
        by_file: dict[str, list[str]] = {}
        for line in result.stdout.splitlines():
            filepath, _, rest = line.partition(":")
            if not rest:
                continue
            if filepath not in by_file and len(by_file) >= 20:
                break
            by_file.setdefault(filepath, []).append(rest)
        sections = []
        for filepath, matches in by_file.items():
            rel = os.path.relpath(filepath, repo)
            sections.append(f"\n### {rel}")
            sections.extend(f"  {m}" for m in matches[:5])
        return "\n".join(sections)
    except Exception as e:
        logger.exception("Codebase search failed")
        return f"Search error: {e}"


_MAX_REPO_FILE_BYTES = 50_000


def read_repo_file(path: str, repo_path: str) -> str:
    """Read a file from the local repo checkout, truncating to 50KB."""
    try:
        resolved = _resolve_within_repo(path, repo_path)
        if resolved is None:
            return f"Invalid path: {path} (must stay within the repo)"
        filepath, _ = resolved
        with open(filepath, encoding="utf-8") as f:
            content = f.read(_MAX_REPO_FILE_BYTES + 1)
        if len(content) > _MAX_REPO_FILE_BYTES:
            return (
                content[:_MAX_REPO_FILE_BYTES] + "\n\n... truncated (file exceeds 50KB)"
            )
        return content
    except FileNotFoundError:
        return f"File not found: {path}"
    except IsADirectoryError:
        return f"Path is a directory, not a file: {path}"
    except Exception as e:
        logger.exception("File read failed")
        return f"Read error: {e}"


def look_up_person_ids(
    external_ids: list[str],
    state_code: str,
    bq_project: str,
    ctx: DiagnosisContext,
) -> str:
    """Look up person_ids from external IDs. Raises PersonIDLookupError if none found."""
    ctx.register_external_ids(external_ids)
    quoted = ", ".join(f"'{eid}'" for eid in external_ids)
    sql = (
        f"SELECT person_id, external_id, id_type, state_code "
        f"FROM `{bq_project}.normalized_state.state_person_external_id` "
        f"WHERE external_id IN ({quoted}) AND state_code = '{state_code}'"
    )
    try:
        rows = _query_rows(sql, ctx)
    except Exception as e:
        logger.exception("BigQuery query failed")
        return f"BigQuery error: {e}"
    if not rows:
        raise PersonIDLookupError(
            f"No person IDs found for external_ids={external_ids}, "
            f"state_code={state_code}"
        )
    ctx.register_external_ids([str(r["external_id"]) for r in rows])
    return _format_rows_as_table(rows)


def _build_tool_handlers(
    config: RuntimeConfig,
    ctx: DiagnosisContext,
) -> dict[str, Callable[[dict[str, Any]], str]]:
    """Build the tool-name → handler dict, closing over runtime config and context."""
    return {
        "run_bq_query": lambda args: run_bq_query(args["sql"], ctx),
        "get_table_schema": lambda args: get_table_schema(
            args["dataset"], args["table"], ctx
        ),
        "fetch_pii": lambda args: fetch_pii_for_issue(
            args["issue_number"], config.sa_email
        ),
        "look_up_person_ids": lambda args: look_up_person_ids(
            args["external_ids"], args["state_code"], config.bq_project, ctx
        ),
        "search_codebase": lambda args: search_codebase(
            args["query"], config.repo_path, args.get("path")
        ),
        "read_repo_file": lambda args: read_repo_file(args["path"], config.repo_path),
    }


TOOLS = [
    {
        "name": "run_bq_query",
        "description": "Run a SQL query against BigQuery and return results as a table.",
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "Standard SQL query to execute.",
                }
            },
            "required": ["sql"],
        },
    },
    {
        "name": "get_table_schema",
        "description": "Get column names and types for a BigQuery table. Use this before querying an unfamiliar table to avoid column name errors.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset": {
                    "type": "string",
                    "description": "Full dataset path (e.g. 'recidiviz-staging.task_eligibility_spans_us_tx').",
                },
                "table": {
                    "type": "string",
                    "description": "Table name without dataset prefix (e.g. 'us_tx_early_release_to_supervision_request_materialized').",
                },
            },
            "required": ["dataset", "table"],
        },
    },
    {
        "name": "fetch_pii",
        "description": "Fetch PII details (names, external IDs) for a GitHub issue from the go/github-pii Google Doc. Returns the relevant section of the doc for the given issue number.",
        "input_schema": {
            "type": "object",
            "properties": {
                "issue_number": {
                    "type": "string",
                    "description": "The GitHub issue number (e.g. '12097').",
                }
            },
            "required": ["issue_number"],
        },
    },
    {
        "name": "look_up_person_ids",
        "description": "Look up internal person_ids from external IDs (e.g. SID numbers, TDCJ numbers). Use this after fetch_pii to convert external IDs to person_ids for subsequent queries.",
        "input_schema": {
            "type": "object",
            "properties": {
                "external_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of external IDs from the PII doc (e.g. ['02636448', '02297793']).",
                },
                "state_code": {
                    "type": "string",
                    "description": "State code (e.g. 'US_TX'). Use 'US_IX' for Idaho.",
                },
            },
            "required": ["external_ids", "state_code"],
        },
    },
    {
        "name": "search_codebase",
        "description": "Search the local pulse-data repo checkout using grep. Returns matching file paths with line numbers and context. Use this to find task/opportunity names, view definitions, and code references.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search keywords (e.g. 'face_to_face_contact', 'complete_discharge').",
                },
                "path": {
                    "type": "string",
                    "description": "Optional directory to scope the search (e.g. 'recidiviz/task_eligibility/eligibility_spans/us_ix').",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "read_repo_file",
        "description": "Read a file from the local pulse-data repo checkout. Use this to read record view definitions, criteria views, or other source code to understand filtering logic.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "File path relative to repo root (e.g. 'recidiviz/calculator/query/state/views/workflows/firestore/us_ix_complete_discharge_early_from_supervision_request_record.py').",
                }
            },
            "required": ["path"],
        },
    },
]


# ── agentic loop ─────────────────────────────────────────────────────────────


def _build_system_prompt(config: RuntimeConfig, product_areas: list[str]) -> str:
    """Assemble the agent's system prompt from skill markdown + product-area sections."""

    def _load_prompt(name: str) -> str:
        path = os.path.join(config.prompts_dir, name)
        with open(path, encoding="utf-8") as f:
            return f.read()

    # TODO(#70351): Switch BQ_PROJECT to recidiviz-123 after getting prod SA permissions
    bq_project = config.bq_project

    def _replace_project(text: str) -> str:
        return text.replace("recidiviz-123", bq_project)

    extract_key_details = _replace_project(_load_prompt("extract-key-details.md"))
    present_results = _replace_project(_load_prompt("present-diagnosis-results.md"))

    all_areas = {
        "workflows": ("Workflows", "diagnose-workflows.md"),
        "tasks": ("Tasks", "diagnose-tasks.md"),
        "insights": (
            "Insights / Supervision Homepage (SHP)",
            "diagnose-insights.md",
        ),
    }
    areas_to_load = product_areas if product_areas else list(all_areas.keys())
    diagnosis_sections = []
    for area in areas_to_load:
        if area in all_areas:
            label, filename = all_areas[area]
            diagnosis_sections.append(
                f"**For {label}:**\n\n{_replace_project(_load_prompt(filename))}"
            )
    diagnosis_step = "\n\n".join(diagnosis_sections)
    logger.info("Product areas for diagnosis: %s", areas_to_load)

    today = datetime.now(AGENT_TIMEZONE).date().isoformat()

    return f"""You are an AI agent that performs initial diagnosis of incoming Product Growth bug tickets.
You are running unattended in a Cloud Build job. Never ask the user to do anything.

Today's date is {today} (US/Eastern). Use this when interpreting recency or
describing when events occurred. Never call any date "today" unless it equals
this date — the most recent row in a table is not necessarily today.

## Tools

1. run_bq_query — Run SQL queries against BigQuery.
2. get_table_schema — Get column names and types for a BQ table.
3. fetch_pii — Fetch PII (names, external IDs) for a GitHub issue from go/github-pii.
4. look_up_person_ids — Convert external IDs from PII to internal person_ids via BigQuery.
5. search_codebase — Grep the local pulse-data repo checkout. Returns file paths and matching lines.
6. read_repo_file — Read a file from the local repo checkout by path.

## Codebase conventions

- States are abbreviated as US_XX (e.g. US_TX = Texas, US_ME = Maine).
- US_ID and US_IX share the same codebase. Always use US_IX when searching code or querying BQ datasets.
- For deeper context, use read_repo_file to read the relevant CLAUDE.md files:
  - `recidiviz/big_query/CLAUDE.md` — BQ view architecture, dataset naming, materialization
  - `recidiviz/task_eligibility/CLAUDE.md` — task eligibility spans, criteria, helper functions
  - `recidiviz/ingest/CLAUDE.md` — data ingestion pipeline

## Investigation Steps

### Step 1: Fetch PII

Use fetch_pii to get external IDs for the affected people. You need these to look up person_ids.

### Step 2: Extract key details from the ticket

{extract_key_details}

If you're not confident which task/opportunity is the right match, pick the best candidate and proceed.

### Step 3: Look up person IDs

Use the look_up_person_ids tool with the external IDs from the PII doc and the state code.
For US_ID tickets, use state_code 'US_IX' (they share the same data).
The tool returns person_id, external_id, id_type, and state_code for each match.

In some cases, two `person_id` values may be associated with the same
`(state_code, external_id)` pair (different `id_type`s). If this happens,
disambiguate by querying `{bq_project}.normalized_state.state_person` for the
candidate `person_id`s and matching `full_name` against the name in the PII
doc entry — then use the matched `person_id` for subsequent queries.

### Step 4: Investigate and diagnose

Follow the applicable diagnosis path depending on the product area.

{diagnosis_step}

If the issue is not relevant to any of these product areas, do not try to
fit it into one of the above buckets. Instead, write a diagnosis that
explains we don't have specific instructions for this product area and
recommends that a human investigate manually.

### Step 5: Present diagnosis results

{present_results}

## Important Notes

- **NEVER include PII in your final response.** No names, no external IDs (SIDs, TDCJ numbers, SSNs),
  no email addresses. PII from fetch_pii is for your investigation only — strip it before responding.
  Use ONLY internal person_ids (large numeric IDs from our database). Person IDs are NOT PII.
- **Do NOT SELECT external_id (or aliased names like SID_Number, TDCJ_Number) in any query whose
  output you will echo back in the diagnosis.** Project only person_id when identifying individuals.
  If you need the external_id to join on, do the join in a subquery / CTE and project person_id out
  of the outer SELECT.
- Follow the output format in "Present diagnosis results" exactly.
- Only include queries you actually ran and got results for. Never guess SQL syntax.
- Always use `{bq_project}` as the GCP project in all SQL queries.
- Always run get_table_schema before querying an unfamiliar table.
- Never access data from Maine (US_ME) or California (US_CA)."""


def run_agent(
    issue_title: str,
    issue_body: str,
    issue_number: int,
    product_areas: list[str],
    config: RuntimeConfig,
    anthropic_api_key: str,
    ctx: DiagnosisContext,
) -> str:
    """Run the diagnosis agentic loop and return the final diagnosis text."""
    result = run_agent_loop(
        api_key=anthropic_api_key,
        system_prompt=_build_system_prompt(config, product_areas),
        user_message=(
            f"**Issue #{issue_number}**\n\n"
            f"**Issue title:** {issue_title}\n\n"
            f"**Issue body:**\n{issue_body}"
        ),
        tools=TOOLS,
        tool_handlers=_build_tool_handlers(config, ctx),
        config=AgentConfig(model=MODEL, max_iterations=MAX_AGENT_ITERATIONS),
        summary_instruction=(
            "You've reached the maximum number of investigation steps. "
            "Please write your diagnosis NOW using the 'Present diagnosis results' "
            "format, based on everything you've found so far. Include all SQL "
            "evidence you collected. Remember: NO PII in the output."
        ),
        failure_types=(DiagnosisFailure,),
    )
    return f"{result.text}{result.footer()}"


# ── main ─────────────────────────────────────────────────────────────────────


def scrub_pii_from_comment(text: str, anthropic_api_key: str) -> str:
    """Run a Haiku pass over the draft comment to redact names and external IDs.

    Runs as a separate Anthropic call (independent of the main agent loop) so
    this acts as a final guardrail before the comment is posted publicly.
    person_ids (internal Recidiviz numeric IDs, typically 18-19 digits) and
    officer/user email addresses are preserved; client names and state-issued
    external IDs are replaced with the redaction marker. Raises on API failure
    — the caller should let the exception propagate rather than post an
    un-scrubbed comment.
    """
    client = anthropic.Anthropic(api_key=anthropic_api_key)
    system = (
        "You are a PII scrubber. You will be given a draft comment about to be "
        "posted on a public GitHub issue. Redact personally identifying "
        "information before it is posted.\n\n"
        "Rules:\n"
        f"- Replace any person NAME (first, last, or combinations) with "
        f"{PII_REDACTION_MARKER}.\n"
        f"- Replace any EXTERNAL ID — state-issued client identifiers like TDCJ "
        f"numbers, SIDs, DOC numbers, CDCR numbers, typically short (6-10 digit) "
        f"numbers shown as a client identifier — with {PII_REDACTION_MARKER}.\n"
        "- DO NOT modify person_ids: internal Recidiviz numeric IDs, typically "
        "18-19 digits long. Preserve them exactly.\n"
        "- DO NOT modify officer / user email addresses (e.g. @tdcj.texas.gov, "
        "@michigan.gov). These are staff/agency contacts, not client PII; keep "
        "them intact.\n"
        "- DO NOT modify URLs, UUIDs, timestamps, SQL keywords, markdown, table "
        "structure, or any other non-PII content.\n"
        "- Output ONLY the scrubbed text — no commentary, no preamble, no "
        "trailing notes."
    )
    response = client.messages.create(
        model=SCRUB_MODEL,
        max_tokens=16_000,
        system=system,
        messages=[{"role": "user", "content": text}],
    )
    logger.info(
        "Scrub pass tokens: input=%d, output=%d",
        response.usage.input_tokens,
        response.usage.output_tokens,
    )
    for block in response.content:
        if block.type == "text":
            return block.text
    raise RuntimeError(
        "PII scrub pass returned no text blocks; refusing to post un-scrubbed comment."
    )


def _build_logs_footer(gcp_project: str, build_id: str) -> str:
    """Build a Markdown footer linking to the current Cloud Build run's logs."""
    logs_url = (
        f"https://console.cloud.google.com/cloud-build/builds/{build_id}"
        f"?project={gcp_project}"
    )
    return f"\n\nSee [Cloud Build logs]({logs_url}) for details."


def main() -> None:
    """Entry point: read config from env, run the agent, and post the diagnosis."""
    config = _load_runtime_config()

    issue_number = int(get_env("ISSUE_NUMBER"))
    issue_repo = os.environ.get("ISSUE_REPO", RECIDIVIZ_DASHBOARDS_REPO)

    # Title and body are base64-encoded in Cloud Build substitutions to avoid
    # breakage from commas, equals signs, or other special characters.
    issue_title = _decode_base64_env("ISSUE_TITLE")
    issue_body = _decode_base64_env("ISSUE_BODY")

    raw_areas = os.environ.get("PRODUCT_AREAS", "")
    product_areas = [a.strip().lower() for a in raw_areas.split(",") if a.strip()]

    force_rerun = os.environ.get("FORCE_RERUN", "").lower() in ("1", "true", "yes")
    logs_footer = _build_logs_footer(config.gcp_project, config.build_id)

    if not force_rerun and issue_has_marker(issue_repo, issue_number, DIAGNOSIS_MARKER):
        logger.info(
            "Diagnosis already exists for %s#%d, skipping (set FORCE_RERUN=1 to override)",
            issue_repo,
            issue_number,
        )
        # upsert is idempotent (edits in place; no duplicate notification),
        # so we don't need a separate "is the follow-up already there?" check.
        follow_up = (
            "A diagnosis comment already exists on this ticket, so the automated "
            "agent won't re-run.\n\n"
            "If you'd like to continue the investigation, run "
            f"`/investigate-pg-ticket {issue_number}` in your local Claude Code "
            "terminal to pick up from where the agent left off."
        )
        _post_marked_comment(
            issue_repo,
            issue_number,
            follow_up + logs_footer,
            marker=FOLLOW_UP_MARKER,
        )
        logger.info(
            "Posted/updated follow-up notice for %s#%d", issue_repo, issue_number
        )
        return

    anthropic_api_key = get_secret("pg_diagnosis_claude_api_key", config.gcp_project)
    ctx = DiagnosisContext()
    try:
        logger.info("Starting diagnosis for %s#%d", issue_repo, issue_number)
        comment = run_agent(
            issue_title,
            issue_body,
            issue_number,
            product_areas,
            config,
            anthropic_api_key,
            ctx,
        )
        logger.info("Scrubbing PII from comment before posting...")
        scrubbed = scrub_pii_from_comment(comment, anthropic_api_key)
        scrubbed = ctx.scrub_known_external_ids(scrubbed)
        # Truncate the diagnosis (not the footer) so the Cloud Build logs link
        # always survives the 65k-char comment cap, even on long diagnoses.
        diagnosis_budget = (
            GITHUB_ISSUE_OR_COMMENT_BODY_MAX_LENGTH
            - len(DIAGNOSIS_MARKER)
            - 1  # newline after the marker
            - len(logs_footer)
            - 200  # safety margin for any decoration we add later
        )
        scrubbed = truncate_string_if_necessary(scrubbed, max_length=diagnosis_budget)
        _post_marked_comment(issue_repo, issue_number, scrubbed + logs_footer)
        logger.info("Posted diagnosis for %s#%d", issue_repo, issue_number)
    except Exception:
        logger.exception("Failed to process %s#%d", issue_repo, issue_number)
        try:
            _post_marked_comment(
                issue_repo,
                issue_number,
                "⚠️ Automated diagnosis failed." + logs_footer,
            )
        except Exception:
            logger.exception(
                "Failed to post failure comment for %s#%d", issue_repo, issue_number
            )
        sys.exit(1)


if __name__ == "__main__":
    main()
