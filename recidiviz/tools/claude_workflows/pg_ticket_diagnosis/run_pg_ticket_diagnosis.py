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
- Reads the ticket's PII from the private Google Doc linked in its body, falling
  back to the shared go/github-pii doc for tickets filed before per-ticket docs
  existed (TODO(OBT-44025): drop that fallback). Refuses to post a diagnosis if
  it never resolved that PII to a person.
- Conditionally loads diagnosis prompts based on product area labels
  (workflows, tasks, insights). Falls back to all three if none specified.
- Deduplication: skips issues that already have a diagnosis comment
  (override with FORCE_RERUN=1).
- Token usage logging per iteration and in the GitHub comment footer.

To test end-to-end, fire the Cloud Build webhook trigger directly:

    SECRET=$(gcloud secrets versions access latest \\
      --secret=github_pg_diagnosis_webhook --project=recidiviz-staging)
    PROJECT_NUMBER=$(gcloud projects describe recidiviz-staging \\
      --format='value(projectNumber)')
    NUMBER=<issue number>
    ENCODED_TITLE=$(gh issue view $NUMBER --repo Recidiviz/pulse-data \\
      --json title --jq '.title' | base64 -w 0)
    ENCODED_BODY=$(gh issue view $NUMBER --repo Recidiviz/pulse-data \\
      --json body --jq '.body' | base64 -w 0)
    jq -n \\
      --arg issue_number "$NUMBER" \\
      --arg issue_title "$ENCODED_TITLE" \\
      --arg issue_body "$ENCODED_BODY" \\
      --arg issue_repo "Recidiviz/pulse-data" \\
      --arg repo_branch "main" \\
      --arg product_areas "workflows" \\
      --arg force_rerun "1" \\
      '{ISSUE_NUMBER: $issue_number, ISSUE_TITLE: $issue_title, ISSUE_BODY: $issue_body, ISSUE_REPO: $issue_repo, REPO_BRANCH: $repo_branch, PRODUCT_AREAS: $product_areas, FORCE_RERUN: $force_rerun}' \\
      | curl -X POST -H "Content-Type: application/json" --data @- \\
        "https://cloudbuild.googleapis.com/v1/projects/${PROJECT_NUMBER}/locations/us-west1/triggers/pg-diagnosis:webhook?key=<API_KEY>&secret=${SECRET}"

The API key portion of the URL is shown on the trigger's detail page in the
Cloud Build console. _PRODUCT_AREAS is a comma-separated list of product
areas (workflows, tasks, insights); leave empty for all. Requires GCP setup
via setup_gcp.sh and Terraform-applied pg-diagnosis-trigger.tf.

Alternatively, fire the `workflow_dispatch` event on
.github/workflows/pg-diagnosis.yml from the GitHub Actions UI — that wraps
the same webhook call.
"""
import base64
import logging
import os
import re
import subprocess
import sys
import time
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

from recidiviz.github.github_client import (
    GITHUB_ISSUE_OR_COMMENT_BODY_MAX_LENGTH,
    RECIDIVIZ_DATA_REPO,
    github_helperbot_client,
    helperbot_issue_has_comment_with_prefix,
    upsert_helperbot_comment,
)
from recidiviz.github.github_issue import GithubIssue
from recidiviz.issue_tracking.linear.linear_client import linear_client_from_secret
from recidiviz.tools.claude_workflows.claude_agent import (
    AgentConfig,
    AgentFailure,
    AgentResult,
    run_agent_loop,
)
from recidiviz.tools.claude_workflows.pg_ticket_diagnosis.pii_doc_parser_utils import (
    extract_pii_doc_id,
    find_issue_section,
    parse_doc,
    section_has_content,
)
from recidiviz.utils.string_formatting import truncate_string_if_necessary

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_AGENT_ITERATIONS = 40
MODEL = "claude-opus-4-8"
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
    """Per-run mutable state: BQ client, external ID tracker, and PII progress.

    Created once in main() and threaded through tool handlers so that
    separate runs (e.g. in tests) don't share state.
    """

    def __init__(self) -> None:
        self._bq_client: bigquery.Client | None = None
        self.known_external_ids: set[str] = set()
        # Whether fetch_pii ever returned usable PII text.
        self.pii_fetched: bool = False
        # Whether look_up_person_ids ever resolved a person. main() refuses to
        # post a diagnosis without this — see the guard in main().
        self.person_ids_resolved: bool = False

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


def issue_has_marker(issue: GithubIssue, marker: str) -> bool:
    """Return True iff a Helperbot comment on the issue starts with the marker."""
    return helperbot_issue_has_comment_with_prefix(
        issue_number=issue.number, prefix=marker, repo=issue.repo
    )


def resolve_linear_id_for_issue(issue: GithubIssue) -> str | None:
    """Return the Linear identifier synced to the GitHub issue, or None.

    Tickets now originate in Linear and sync to GitHub; PII in go/github-pii is
    keyed by the Linear identifier for those tickets, so we resolve it via
    Linear's native sync-attachment API to look up the right section. The
    LinearClient is built from the Linear API key in Secret Manager (shared
    linear_client_from_secret factory).

    Best-effort: any failure — missing/invalid Linear credentials or a Linear
    API error — degrades to None (logged) rather than aborting the diagnosis.
    The go/github-pii lookup still succeeds by GitHub number for pre-Linear
    tickets, so a Linear outage must not block those.

    TODO(OBT-44025): remove this function and the linear_id threading through
    run_agent/_build_tool_handlers with the shared-doc fallback — keying that doc
    is the only thing the identifier is used for.
    """
    try:
        issue_group = (
            linear_client_from_secret().get_equivalent_issue_group_for_github_issue(
                issue
            )
        )
        return issue_group.linear_issue.issue_identifier if issue_group else None
    except Exception:
        logger.warning(
            "Linear lookup failed for %s; proceeding with GitHub number only",
            issue,
            exc_info=True,
        )
        return None


def _post_marked_comment(
    issue: GithubIssue,
    body: str,
    marker: str = DIAGNOSIS_MARKER,
) -> None:
    """Upsert a comment whose body is prefixed with the given marker."""
    upsert_helperbot_comment(
        issue_number=issue.number,
        body=f"{marker}\n{body}",
        prefix=marker,
        repo=issue.repo,
    )


# ── PII doc lookup ───────────────────────────────────────────────────────────

# The legacy shared doc, used for tickets filed before every ticket got its own
# private PII doc. New tickets link theirs from a banner in the issue body.
# TODO(OBT-44025): drop this and the whole shared-doc fallback once every ticket
# we might still diagnose has its own PII doc.
GITHUB_PII_DOC_ID = "1hYq--Xw6D5Lu96pSFVGeNu9AuxMNtB5F4ltI2VE9FZs"

# The per-ticket doc is created a few seconds after the issue itself and the
# banner linking it is added afterwards, so the `issues.opened` webhook payload
# never carries the link. Re-read the live body a few times before concluding a
# ticket has no per-ticket doc.
PII_DOC_LOOKUP_ATTEMPTS = 3
PII_DOC_LOOKUP_DELAY_SECONDS = 10

# Headers labelling each source in the text handed to the agent, so it can tell
# where a given ID came from and report on a partial fetch sensibly.
TICKET_PII_DOC_LABEL = "=== From this ticket's own private PII doc ==="
# TODO(OBT-44025): remove with the shared doc.
SHARED_PII_DOC_LABEL = "=== From the shared go/github-pii doc (legacy) ==="


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
        "**failed to fetch the PII document**. The agent never got as far as "
        "reading any PII for this issue."
    )
    GUIDANCE = (
        "Likely causes: the diagnosis service account is not shared on the PII "
        "docs Drive folder (it needs at least Viewer — Drive ACLs are not "
        "managed by our Terraform, so a moved folder breaks this silently), the "
        "service account is missing Google Docs API access, the document was "
        "moved/deleted, or the Docs API returned a transient error. Investigate "
        "the service account permissions and re-run."
    )


class PIINotFoundError(DiagnosisFailure):
    """Raised when the legacy shared PII doc has no entry for the given issue.

    TODO(OBT-44025): remove with the shared-doc fallback — a ticket with its own
    PII doc can only fail with PIIFetchError.
    """

    HEADLINE = (
        "the PII document was **fetched successfully**, but it has **no entry "
        "for this issue**."
    )
    GUIDANCE = (
        "This ticket has no private PII doc linked in its body, so the agent "
        "fell back to the shared go/github-pii doc and found nothing there "
        "either. Newly filed tickets should get their own PII doc "
        "automatically — if the banner is missing from the top of this ticket, "
        "that automation didn't run. Otherwise, add an entry for this issue to "
        "go/github-pii (including external IDs) and re-run the diagnosis."
    )


class PIIUnusableError(DiagnosisFailure):
    """Raised after the loop when the agent never resolved PII to a person.

    This is the single enforcement point for "did we actually get PII?". The
    per-tool failures above only fire when a tool is actually called, so they
    can't catch an agent that reads a doc, finds nothing it can use, and presses
    on to write a pipeline-level diagnosis anyway. A speculative diagnosis reads
    as an answer, which is worse than saying we're blocked.
    """

    HEADLINE = (
        "the agent **never resolved this ticket's PII to a person**, so any "
        "diagnosis it produced would not be about the reported client."
    )
    GUIDANCE = (
        "Please open the PII doc linked at the top of this ticket and make sure "
        "the affected client's state-issued ID(s) are written there as text — "
        "screenshots can't be read — then re-run the diagnosis."
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


def _fetch_doc(*, doc_id: str, sa_email: str) -> dict:
    """Returns the Docs API JSON for `doc_id`. Raises PIIFetchError on failure."""
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
        url = f"https://docs.googleapis.com/v1/documents/{doc_id}"
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {credentials.token}"},
            timeout=30,
        )
        doc = resp.json()
        if "error" in doc:
            raise PIIFetchError(
                f"Google Docs API error for doc [{doc_id}]: "
                f"{doc['error'].get('message', 'unknown')}"
            )
        return doc
    except PIIFetchError:  # pylint: disable=try-except-raise
        # Re-raise so it isn't swallowed and re-wrapped by the broader except.
        raise
    except Exception as e:
        logger.exception("Failed to fetch PII doc [%s]", doc_id)
        raise PIIFetchError(f"Error fetching PII doc [{doc_id}]: {e}") from e


def resolve_pii_doc_id(
    issue: GithubIssue, fallback_body: str
) -> tuple[str | None, str]:
    """Returns the ticket's private PII doc ID (if any) and the issue body to use.

    Re-reads the body from GitHub rather than trusting `fallback_body` (which
    comes from the webhook payload): the doc is created — and the banner linking
    it prepended — a few seconds after the issue itself, so the payload body
    predates the link. Retries a few times before giving up, since the agent can
    start before the automation finishes.

    A None doc ID means this is a pre-cutover ticket whose PII lives in the
    shared go/github-pii doc.
    """
    body = fallback_body
    for attempt in range(1, PII_DOC_LOOKUP_ATTEMPTS + 1):
        try:
            live_issue = (
                github_helperbot_client().get_repo(issue.repo).get_issue(issue.number)
            )
            body = live_issue.body or ""
        except Exception:
            logger.warning(
                "Could not read live body for %s; using the webhook payload body",
                issue,
                exc_info=True,
            )
            break
        if doc_id := extract_pii_doc_id(body):
            logger.info("Resolved per-ticket PII doc %s for %s", doc_id, issue)
            return doc_id, body
        if attempt < PII_DOC_LOOKUP_ATTEMPTS:
            logger.info(
                "No PII doc link in %s yet (attempt %d/%d); waiting %ds",
                issue,
                attempt,
                PII_DOC_LOOKUP_ATTEMPTS,
                PII_DOC_LOOKUP_DELAY_SECONDS,
            )
            time.sleep(PII_DOC_LOOKUP_DELAY_SECONDS)

    # Reached either by exhausting the retries or by breaking out on a GitHub
    # read failure, in which case `body` is the webhook payload body — which for
    # a manual workflow_dispatch run may well carry the banner.
    doc_id = extract_pii_doc_id(body)
    if doc_id:
        logger.info("Resolved per-ticket PII doc %s for %s", doc_id, issue)
    else:
        logger.info(
            "No per-ticket PII doc for %s; falling back to go/github-pii", issue
        )
    return doc_id, body


def fetch_pii_for_issue(
    *,
    issue_number: str,
    linear_id: str | None,
    pii_doc_id: str | None,
    sa_email: str,
    ctx: DiagnosisContext,
) -> str:
    """Fetch every scrap of PII we hold for the given issue.

    Reads BOTH the ticket's own private PII doc (when one is linked from its
    body) and the legacy shared go/github-pii section, and returns whatever it
    found, labelled. Reading both matters during the transition: reporters still
    write into the shared doc out of habit, so a ticket can have an untouched
    per-ticket doc and a perfectly good entry in the shared one. Picking one
    source would silently drop the other.

    Raises only when neither source produced anything.

    TODO(OBT-44025): once the shared doc goes, this collapses into
    _fetch_ticket_pii_doc and pii_doc_id stops being optional. That cleanup is
    blocked on reporters no longer filing PII in the shared doc — deleting this
    while they still do would drop PII on the floor.
    """
    parts: list[str] = []
    fetch_errors: list[str] = []

    # Both reads are best-effort: one source failing must not hide the other.
    if pii_doc_id:
        try:
            if text := _fetch_ticket_pii_doc(doc_id=pii_doc_id, sa_email=sa_email):
                parts.append(f"{TICKET_PII_DOC_LABEL}\n{text}")
        except PIIFetchError as e:
            logger.warning("Could not read the per-ticket PII doc: %s", e)
            fetch_errors.append(str(e))

    try:
        if section := _fetch_shared_pii_doc_section(
            issue_number=issue_number, linear_id=linear_id, sa_email=sa_email
        ):
            parts.append(f"{SHARED_PII_DOC_LABEL}\n{section}")
    except PIIFetchError as e:
        logger.warning("Could not read the shared PII doc: %s", e)
        fetch_errors.append(str(e))

    if not parts:
        if fetch_errors:
            raise PIIFetchError("; ".join(fetch_errors))
        linear_detail = f" (Linear {linear_id})" if linear_id else ""
        raise PIINotFoundError(
            f"No PII found for issue {issue_number}{linear_detail} in either the "
            f"ticket's own PII doc or the shared go/github-pii doc."
        )

    logger.info("Fetched PII from %d source(s) for issue %s", len(parts), issue_number)
    ctx.pii_fetched = True
    return "\n\n".join(parts)


def _fetch_ticket_pii_doc(*, doc_id: str, sa_email: str) -> str:
    """Returns the full text of a ticket's own private PII doc.

    Returns the doc verbatim rather than trying to locate the client IDs within
    it: the agent is better at reading a free-form doc than any parser we'd write,
    and matching on the doc's template would break silently whenever that
    template — which lives outside this repo — was reworded. Whether the run
    actually got usable PII is enforced in main(), which refuses to post a
    diagnosis unless a person_id was resolved.
    """
    doc = _fetch_doc(doc_id=doc_id, sa_email=sa_email)
    return "\n".join(parse_doc(doc))


def _fetch_shared_pii_doc_section(
    *, issue_number: str, linear_id: str | None, sa_email: str
) -> str:
    """Returns this issue's section of the legacy shared go/github-pii doc.

    Returns "" when the doc has no section for this issue, or when the section it
    has is just a header with nothing under it — both are misses the caller
    treats as "this source had nothing", not as failures.

    TODO(OBT-44025): remove — the per-ticket doc replaces this.
    """
    doc = _fetch_doc(doc_id=GITHUB_PII_DOC_ID, sa_email=sa_email)
    identifiers = [issue_number] + ([linear_id] if linear_id else [])
    section = find_issue_section(parse_doc(doc), identifiers)
    if not section or not section_has_content(section):
        return ""
    return "\n".join(section)


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
    ctx.person_ids_resolved = True
    return _format_rows_as_table(rows)


def _build_tool_handlers(
    config: RuntimeConfig,
    ctx: DiagnosisContext,
    linear_id: str | None,
    pii_doc_id: str | None,
) -> dict[str, Callable[[dict[str, Any]], str]]:
    """Build the tool-name → handler dict, closing over runtime config and context."""
    return {
        "run_bq_query": lambda args: run_bq_query(args["sql"], ctx),
        "get_table_schema": lambda args: get_table_schema(
            args["dataset"], args["table"], ctx
        ),
        "fetch_pii": lambda args: fetch_pii_for_issue(
            issue_number=args["issue_number"],
            linear_id=linear_id,
            pii_doc_id=pii_doc_id,
            sa_email=config.sa_email,
            ctx=ctx,
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
        "description": "Fetch PII details (names, external IDs) for a GitHub issue. Reads both the ticket's own private PII Google Doc and the legacy shared go/github-pii doc, and returns whatever either holds, labelled by source — so check both sections for client IDs.",
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
3. fetch_pii — Fetch PII (names, external IDs) for a GitHub issue from every doc we hold for it.
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
It reads every doc we hold for the ticket — its own private PII doc, and the legacy shared
go/github-pii doc — and returns them labelled by source, so you never need to open any link
yourself. **Read every section it returns before concluding there are no IDs**: reporters are
mid-migration between the two docs, so the client's ID is often in only one of them.

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
- **If you cannot get usable client IDs, STOP.** If fetch_pii returns no external IDs (e.g.
  the doc holds only screenshots), say plainly that the ticket's PII doc has no usable client
  IDs and that a human needs to fill it in. Do NOT substitute a pipeline-level or
  state-wide diagnosis — an answer about the state's data in general reads as an answer about
  this client, which is worse than reporting that you were blocked.
- Follow the output format in "Present diagnosis results" exactly.
- Only include queries you actually ran and got results for. Never guess SQL syntax.
- Always use `{bq_project}` as the GCP project in all SQL queries.
- Always run get_table_schema before querying an unfamiliar table.
- Never access data from Maine (US_ME) or California (US_CA)."""


def run_agent(
    issue: GithubIssue,
    issue_title: str,
    issue_body: str,
    product_areas: list[str],
    config: RuntimeConfig,
    anthropic_api_key: str,
    ctx: DiagnosisContext,
    linear_id: str | None,
    pii_doc_id: str | None,
) -> AgentResult:
    """Run the diagnosis agentic loop and return its result.

    Returns the whole AgentResult, not just the text, so main() can tell an
    already-reported failure apart from a run that finished normally.
    """
    return run_agent_loop(
        api_key=anthropic_api_key,
        system_prompt=_build_system_prompt(config, product_areas),
        user_message=(
            f"**Issue #{issue.number}**\n\n"
            f"**Issue title:** {issue_title}\n\n"
            f"**Issue body:**\n{issue_body}"
        ),
        tools=TOOLS,
        tool_handlers=_build_tool_handlers(config, ctx, linear_id, pii_doc_id),
        config=AgentConfig(model=MODEL, max_iterations=MAX_AGENT_ITERATIONS),
        summary_instruction=(
            "You've reached the maximum number of investigation steps. "
            "Please write your diagnosis NOW using the 'Present diagnosis results' "
            "format, based on everything you've found so far. Include all SQL "
            "evidence you collected. Remember: NO PII in the output."
        ),
        failure_types=(DiagnosisFailure,),
    )


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

    issue = GithubIssue(
        repo=os.environ.get("ISSUE_REPO", RECIDIVIZ_DATA_REPO),
        number=int(get_env("ISSUE_NUMBER")),
    )

    # Title and body are base64-encoded in Cloud Build substitutions to avoid
    # breakage from commas, equals signs, or other special characters.
    issue_title = _decode_base64_env("ISSUE_TITLE")
    issue_body = _decode_base64_env("ISSUE_BODY")

    raw_areas = os.environ.get("PRODUCT_AREAS", "")
    product_areas = [a.strip().lower() for a in raw_areas.split(",") if a.strip()]

    force_rerun = os.environ.get("FORCE_RERUN", "").lower() in ("1", "true", "yes")
    logs_footer = _build_logs_footer(config.gcp_project, config.build_id)

    if not force_rerun and issue_has_marker(issue, DIAGNOSIS_MARKER):
        logger.info(
            "Diagnosis already exists for %s, skipping (set FORCE_RERUN=1 to override)",
            issue,
        )
        # upsert is idempotent (edits in place; no duplicate notification),
        # so we don't need a separate "is the follow-up already there?" check.
        follow_up = (
            "A diagnosis comment already exists on this ticket, so the automated "
            "agent won't re-run.\n\n"
            "If you'd like to continue the investigation, run "
            f"`/investigate-pg-ticket {issue.number}` in your local Claude Code "
            "terminal to pick up from where the agent left off."
        )
        _post_marked_comment(
            issue,
            follow_up + logs_footer,
            marker=FOLLOW_UP_MARKER,
        )
        logger.info("Posted/updated follow-up notice for %s", issue)
        return

    pii_doc_id, issue_body = resolve_pii_doc_id(issue, issue_body)

    # Needed to key the shared doc, which is read even when the ticket has its
    # own PII doc — see fetch_pii_for_issue.
    linear_id = resolve_linear_id_for_issue(issue)
    logger.info("Linear ID for %s: %s", issue, linear_id)

    anthropic_api_key = get_secret("pg_diagnosis_claude_api_key", config.gcp_project)
    ctx = DiagnosisContext()
    try:
        logger.info("Starting diagnosis for %s", issue)
        result = run_agent(
            issue,
            issue_title,
            issue_body,
            product_areas,
            config,
            anthropic_api_key,
            ctx,
            linear_id,
            pii_doc_id,
        )
        # A run that finished normally but never resolved the ticket's PII to a
        # person didn't diagnose this client's problem, whatever it wrote — post
        # the blocker instead of the speculation. `failed` runs already carry a
        # more specific message (e.g. which doc was empty), so leave those be.
        if not result.failed and not ctx.person_ids_resolved:
            logger.warning(
                "Discarding diagnosis for %s: person IDs were never resolved "
                "(pii_fetched=%s)",
                issue,
                ctx.pii_fetched,
            )
            detail = (
                "The agent read PII for this ticket but never resolved it to a "
                "person in BigQuery."
                if ctx.pii_fetched
                else "The agent never successfully read any PII for this ticket."
            )
            _post_marked_comment(
                issue, PIIUnusableError(detail).user_message() + logs_footer
            )
            return

        comment = f"{result.text}{result.footer()}"
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
        _post_marked_comment(issue, scrubbed + logs_footer)
        logger.info("Posted diagnosis for %s", issue)
    except Exception as e:
        logger.exception("Failed to process %s", issue)
        # Surface the error type and message so a failure isn't a contentless
        # "it failed." Scrub any external IDs seen this run; person_ids are not
        # PII. Most failures here are infra (model/API/auth errors) with no PII.
        detail = ctx.scrub_known_external_ids(f"{type(e).__name__}: {e}")
        try:
            _post_marked_comment(
                issue,
                f"⚠️ Automated diagnosis failed.\n\n**Detail:** {detail}" + logs_footer,
            )
        except Exception:
            logger.exception("Failed to post failure comment for %s", issue)
        sys.exit(1)


if __name__ == "__main__":
    main()
