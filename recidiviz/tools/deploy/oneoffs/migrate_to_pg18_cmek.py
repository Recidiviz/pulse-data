"""
Migrate Cloud SQL instances to Postgres 18 with CMEK using Cloud KMS + Database Migration Service.

This script is phase-based. Run each phase in order; manual steps occur between phases.

Usage:
    uv run python migrate_to_pg18_cmek.py <source_instance> --project <project> --db-secret-prefix <prefix> preflight
    uv run python migrate_to_pg18_cmek.py <source_instance> --project <project> --db-secret-prefix <prefix> setup-migration [--dry-run]
    uv run python migrate_to_pg18_cmek.py <source_instance> --project <project> --db-secret-prefix <prefix> cutover [--dry-run]

Phases:
    preflight         Check (not execute) that prerequisites are satisfied.
    setup-migration   Prepare source DB for DMS, create connection profiles, create and start
                      migration job.
    cutover           Promote DMS job, update instance ID secret, redeploy Cloud Run services.
"""

import argparse
import functools
import getpass
import json
import os
import shutil
import subprocess
import sys
import tempfile
import termios
import time
from collections.abc import Callable, Generator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, TypeVar
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from recidiviz.tools.utils.script_helpers import (
    prompt_for_confirmation,
    prompt_for_step,
)
from recidiviz.utils.future_executor import map_fn_with_progress_bar_results
from recidiviz.utils.string import StrictStringFormatter

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

POSTGRES_18_VERSION = "POSTGRES_18"
KMS_KEY_PROJECT = "cmek-82ade411-5705-4461-b2cb-9"
KMS_KEYRING = "cloudsql-cmek"
KEY_ROTATION_PERIOD = "90d"
DB_USER = "postgres"
DB_NAME = "postgres"
DEST_SUFFIX = "-cmek"
PROXY_PORT = 5432
REQUIRED_APIS = [
    "cloudkms.googleapis.com",
    "sqladmin.googleapis.com",
    "datamigration.googleapis.com",
]
REQUIRED_SERVICE_AGENTS: dict[str, str] = {
    "sqladmin.googleapis.com": "service-{project_number}@gcp-sa-cloud-sql.iam.gserviceaccount.com",
}
DMS_SOURCE_PROFILE_SUFFIX = "-source"
DMS_JOB_SUFFIX = "-migration"
CLOUD_RUN_ANNOTATION_KEY = "run.googleapis.com/cloudsql-instances"
SERVICE_DEPLOY_POLL_INTERVAL_SECONDS = 15
SERVICE_DEPLOY_TIMEOUT_MINUTES = 10
CloudRunResourceType = Literal["services", "jobs"]

_T = TypeVar("_T")


# ---------------------------------------------------------------------------
# Step decorators
# ---------------------------------------------------------------------------


def _flush_stdin() -> None:
    """Discard any buffered stdin input so stale keypresses don't auto-confirm."""
    try:
        termios.tcflush(sys.stdin.fileno(), termios.TCIFLUSH)
    except (termios.error, OSError):
        # Not a terminal (e.g. piped input), nothing to flush
        pass


def confirm_step(prompt: str) -> Callable[[Callable[..., _T]], Callable[..., _T]]:
    """Decorator that prompts the user for confirmation before running a step.

    Flushes stdin to discard stale keypresses, then delegates to
    ``prompt_for_confirmation`` from script_helpers.
    """

    def decorator(fn: Callable[..., _T]) -> Callable[..., _T]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> _T:
            _flush_stdin()
            prompt_for_confirmation(prompt)
            return fn(*args, **kwargs)

        return wrapper

    return decorator


def catch_errors(fn: Callable[..., _T]) -> Callable[..., _T | None]:
    """Decorator that catches all exceptions and asks the user whether to proceed.

    On error, prints the error and prompts the user. Defaults to exit.
    Returns None if the user chooses to continue past the error.
    Calls sys.exit(1) if the user declines.
    """

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> _T | None:
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            print(f"  ERROR: {e}")
            _flush_stdin()
            prompt_for_confirmation("  Continue anyway?", exit_on_cancel=True)
            return None

    return wrapper


# ---------------------------------------------------------------------------
# Core gcloud helper
# ---------------------------------------------------------------------------


def run_bq(args: list[str]) -> subprocess.CompletedProcess[str]:
    """Run a bq command, printing it and raising RuntimeError on non-zero exit."""
    cmd = ["bq"] + args
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(
        cmd, capture_output=True, text=True, check=False
    )  # nosec B603
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result


def run_gcloud(args: list[str]) -> subprocess.CompletedProcess[str]:
    """Run a gcloud command, printing it and raising RuntimeError on non-zero exit."""
    cmd = ["gcloud"] + args
    _REDACT_KEYWORDS = ("password", "certificate", "key")
    display_cmd = []
    redact_next = False
    for token in cmd:
        if redact_next:
            display_cmd.append("<REDACTED>")
            redact_next = False
            continue
        token_lower = token.lower()
        # --flag=value form
        if "=" in token and any(kw in token_lower for kw in _REDACT_KEYWORDS):
            display_cmd.append(token.split("=")[0] + "=<REDACTED>")
        # --flag value form: redact the next token
        elif token.startswith("--") and any(
            kw in token_lower for kw in _REDACT_KEYWORDS
        ):
            display_cmd.append(token)
            redact_next = True
        else:
            display_cmd.append(token)

    print(f"Running command: {' '.join(display_cmd)}")
    result = subprocess.run(
        cmd, capture_output=True, text=True, check=False
    )  # nosec B603
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result


def get_secret(project: str, secret_id: str) -> str:
    """Fetch a secret value from Google Cloud Secret Manager."""
    try:
        result = run_gcloud(
            [
                "secrets",
                "versions",
                "access",
                "latest",
                f"--secret={secret_id}",
                "--project",
                project,
            ]
        )
    except RuntimeError as e:
        raise RuntimeError(f"Failed to fetch secret '{secret_id}': {e}") from e
    return result.stdout.strip()


# ---------------------------------------------------------------------------
# Project / instance utilities
# ---------------------------------------------------------------------------


def get_instance_details(project: str, instance_name: str) -> dict[str, Any]:
    """Fetch Cloud SQL instance details."""
    try:
        result = run_gcloud(
            [
                "sql",
                "instances",
                "describe",
                instance_name,
                "--project",
                project,
                "--format",
                "json",
            ]
        )
    except RuntimeError as e:
        if "was not found" in str(e) or "NOT_FOUND" in str(e):
            raise RuntimeError(
                f"Instance '{instance_name}' not found in project '{project}'"
            ) from e
        raise RuntimeError(f"Failed to get instance details: {e}") from e

    return json.loads(result.stdout)


def get_instance_connection_name(project: str, instance_name: str) -> str:
    """Return the connection name (project:region:instance) for a Cloud SQL instance."""
    try:
        result = run_gcloud(
            [
                "sql",
                "instances",
                "describe",
                instance_name,
                "--project",
                project,
                "--format",
                "value(connectionName)",
            ]
        )
    except RuntimeError as e:
        raise RuntimeError(
            f"Failed to get connection name for '{instance_name}': {e}"
        ) from e
    connection_name = result.stdout.strip()
    if not connection_name:
        raise RuntimeError(f"Empty connection name for instance '{instance_name}'")
    return connection_name


def get_outgoing_ips(project: str, instance_name: str) -> list[str]:
    """Return the outgoing IP addresses for a Cloud SQL instance."""
    details = get_instance_details(project, instance_name)
    return [
        addr["ipAddress"]
        for addr in details.get("ipAddresses", [])
        if addr.get("type") == "OUTGOING"
    ]


def allowlist_dest_ips_on_source(
    project: str,
    source_instance: str,
    dest_instance: str,
    dry_run: bool = False,
) -> None:
    """Add the destination instance's outgoing IPs to the source's authorized networks.

    Preserves any existing authorized networks on the source instance.
    """
    outgoing_ips = get_outgoing_ips(project, dest_instance)
    if not outgoing_ips:
        print(
            f"  WARNING: No outgoing IPs found on '{dest_instance}'. "
            "Skipping authorized networks update."
        )
        return

    # Get existing authorized networks
    source_details = get_instance_details(project, source_instance)
    ip_config = source_details.get("settings", {}).get("ipConfiguration", {})
    existing_networks = [
        entry["value"] for entry in ip_config.get("authorizedNetworks", [])
    ]

    # Merge: existing + new outgoing IPs (as /32 CIDRs), deduplicated
    new_cidrs = [f"{ip}/32" for ip in outgoing_ips]
    all_networks = list(dict.fromkeys(existing_networks + new_cidrs))

    if dry_run:
        print(
            f"  [DRY RUN] Would allowlist destination outgoing IPs on "
            f"'{source_instance}': {new_cidrs}"
        )
        return

    print(f"  Allowlisting destination outgoing IPs: {new_cidrs}")
    run_gcloud(
        [
            "sql",
            "instances",
            "patch",
            source_instance,
            "--project",
            project,
            f"--authorized-networks={','.join(all_networks)}",
            "--quiet",
        ]
    )
    print(f"  Updated authorized networks on '{source_instance}'")


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------


def resolve_db_password(
    project: str, args: argparse.Namespace, prompt_if_missing: bool = False
) -> str | None:
    """Resolve the DB password from available sources.

    Checks --db-secret-prefix (derives {prefix}_db_password), falling back
    to interactive prompt.
    Returns None if no password is available and prompt_if_missing is False.
    """
    if args.db_secret_prefix:
        secret_id = f"{args.db_secret_prefix}_db_password"
        return get_secret(project, secret_id)
    if prompt_if_missing:
        return getpass.getpass(f"Enter password for {DB_USER}: ")
    return None


def get_project_number(project: str) -> str:
    """Return the numeric project number for the given project."""
    try:
        result = run_gcloud(
            [
                "projects",
                "describe",
                project,
                "--format",
                "value(projectNumber)",
            ]
        )
    except RuntimeError as e:
        raise RuntimeError(f"Failed to get project number for '{project}': {e}") from e
    number = result.stdout.strip()
    if not number:
        raise RuntimeError(f"Empty project number for '{project}'")
    return number


def derive_dest_instance_name(
    source_instance: str, suffix: str, new_instance_prefix: str | None = None
) -> str:
    """Return the destination instance name by appending suffix to the prefix.

    If *new_instance_prefix* is provided it is used instead of *source_instance*
    as the base name.
    """
    base = new_instance_prefix if new_instance_prefix else source_instance
    return f"{base}{suffix}"


def kms_key_path(region: str, key_name: str) -> str:
    """Return the full KMS key resource path."""
    return (
        f"projects/{KMS_KEY_PROJECT}/locations/{region}"
        f"/keyRings/{KMS_KEYRING}/cryptoKeys/{key_name}"
    )


@contextmanager
def cloud_sql_proxy(
    project: str, instance_name: str, port: int = 5432
) -> Generator[subprocess.Popen[bytes], None, None]:
    """
    Context manager that starts a Cloud SQL Auth Proxy and terminates it on exit.

    The proxy is automatically shut down when the ``with`` block exits,
    freeing the port for other migrations to use.
    """
    connection_name = get_instance_connection_name(project, instance_name)

    print(f"\nStarting Cloud SQL Auth Proxy for {connection_name} on port {port}...")
    proxy = subprocess.Popen(  # nosec B603 B607
        [
            "cloud-sql-proxy",
            f"--port={port}",
            connection_name,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        time.sleep(2)
        if proxy.poll() is not None:
            stderr = proxy.stderr.read().decode() if proxy.stderr else ""
            raise RuntimeError(f"Cloud SQL Auth Proxy failed to start: {stderr}")

        print(f"Cloud SQL Auth Proxy is running (listening on port {port}).")
        yield proxy
    finally:
        if proxy.poll() is None:
            proxy.terminate()
            try:
                proxy.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proxy.kill()
        print(f"Cloud SQL Auth Proxy for {connection_name} stopped.")


def run_psql(
    db_user: str,
    db_password: str,
    db_name: str,
    sql: str,
    port: int = 5432,
) -> subprocess.CompletedProcess[str]:
    """Run a SQL statement via psql against localhost (Cloud SQL Auth Proxy)."""
    return subprocess.run(  # nosec B603 B607
        [
            "psql",
            "-h",
            "127.0.0.1",
            "-p",
            str(port),
            "-U",
            db_user,
            "-d",
            db_name,
            "-t",
            "-A",
            "-c",
            sql,
        ],
        capture_output=True,
        text=True,
        timeout=60,
        env={**os.environ, "PGPASSWORD": db_password},
        check=False,
    )


_SEQUENCE_QUERY = """\
SELECT
    'SELECT SETVAL(' ||
       quote_literal(quote_ident(sn.nspname) || '.' || quote_ident(cs.relname)) ||
       ', COALESCE(MAX(' || quote_ident(a.attname) || '), 1)) FROM ' ||
       quote_ident(tn.nspname) || '.' || quote_ident(ct.relname) || ';'
FROM pg_depend d
    JOIN pg_class cs ON cs.oid = d.objid AND cs.relkind = 'S'
    JOIN pg_class ct ON ct.oid = d.refobjid
    JOIN pg_attribute a ON a.attrelid = ct.oid AND d.refobjsubid = a.attnum
    JOIN pg_namespace tn ON tn.oid = ct.relnamespace
    JOIN pg_namespace sn ON sn.oid = cs.relnamespace
ORDER BY sn.nspname, cs.relname;
"""


@catch_errors
def update_sequences(
    project: str,
    instance_name: str,
    db_user: str,
    db_password: str,
    port: int = 5432,
    dry_run: bool = False,
) -> None:
    """Update all sequences on the destination instance to match table data.

    DMS does not migrate sequence values. For each user database, this function
    discovers all sequences and runs SETVAL to set each one to the current MAX
    of its owning column.
    """
    databases = list_user_databases(project, instance_name)
    if not databases:
        print("  No user databases found, skipping sequence update.")
        return

    with cloud_sql_proxy(project, instance_name, port=port):
        for db_name in databases:
            print(f"  Updating sequences in database '{db_name}'...")

            # Get the list of SETVAL statements
            result = run_psql(
                db_user=db_user,
                db_password=db_password,
                db_name=db_name,
                sql=_SEQUENCE_QUERY,
                port=port,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"Failed to list sequences in '{db_name}': {result.stderr.strip()}"
                )

            setval_statements = [
                line.strip() for line in result.stdout.splitlines() if line.strip()
            ]

            if not setval_statements:
                print(f"    No sequences found in '{db_name}'.")
                continue

            print(f"    Found {len(setval_statements)} sequence(s).")

            if dry_run:
                for stmt in setval_statements:
                    print(f"    [DRY RUN] Would run: {stmt}")
                continue

            for stmt in setval_statements:
                print(f"    Running: {stmt}")
                r = run_psql(
                    db_user=db_user,
                    db_password=db_password,
                    db_name=db_name,
                    sql=stmt,
                    port=port,
                )
                if r.returncode != 0:
                    raise RuntimeError(
                        f"Failed to update sequence in '{db_name}': {r.stderr.strip()}"
                    )

    print("  Sequence update complete.")


# ---------------------------------------------------------------------------
# Preflight check functions
# Each returns bool and prints its own PASS/FAIL line. Never raises.
# ---------------------------------------------------------------------------


def check_apis_enabled(project: str) -> bool:
    """Check that all required APIs are enabled in the project."""
    print("\n[Check 1] Verifying required APIs are enabled...")
    try:
        result = run_gcloud(
            [
                "services",
                "list",
                "--enabled",
                "--project",
                project,
                "--format",
                "value(config.name)",
            ]
        )
    except RuntimeError as e:
        print(f"  FAIL: Could not list enabled services: {e}")
        return False

    enabled = set(result.stdout.splitlines())
    all_passed = True
    for api in REQUIRED_APIS:
        if api in enabled:
            print(f"  PASS: {api}")
        else:
            print(f"  FAIL: {api} is NOT enabled")
            print(f"         Fix: gcloud services enable {api} --project {project}")
            all_passed = False
    return all_passed


def check_kms_access(region: str) -> bool:
    """
    Check that we have cloudkms.admin access on the key project by describing the keyring.
    """
    print(
        f"\n[Check 2] Verifying KMS access on keyring '{KMS_KEYRING}' "
        f"in project '{KMS_KEY_PROJECT}'..."
    )
    try:
        run_gcloud(
            [
                "kms",
                "keyrings",
                "describe",
                KMS_KEYRING,
                "--project",
                KMS_KEY_PROJECT,
                "--location",
                region,
            ]
        )
    except RuntimeError as e:
        print(f"  FAIL: Cannot access keyring: {e}")
        print(
            f"  Fix: Ensure you have roles/cloudkms.admin on project '{KMS_KEY_PROJECT}'"
        )
        return False

    print(f"  PASS: Keyring '{KMS_KEYRING}' accessible in '{KMS_KEY_PROJECT}'")
    return True


def check_service_agents(project: str) -> bool:
    """Check that required service agents exist in the project."""
    print("\n[Check 3] Verifying required service agents are provisioned...")
    project_number = get_project_number(project)
    all_passed = True
    for service, email_template in REQUIRED_SERVICE_AGENTS.items():
        email = StrictStringFormatter().format(
            email_template, project_number=project_number
        )
        try:
            run_gcloud(
                [
                    "iam",
                    "service-accounts",
                    "describe",
                    email,
                    "--project",
                    project,
                ]
            )
            print(f"  PASS: {service} → {email}")
        except RuntimeError:
            print(f"  FAIL: Service agent for {service} not found: {email}")
            print(
                f"    Fix: gcloud beta services identity create "
                f"--service={service} --project {project}"
            )
            all_passed = False
    return all_passed


def check_logical_decoding_flags(project: str, instance_name: str) -> bool:
    """
    Check that the source instance has logical decoding and pglogical flags set.

    Verifies:
      - cloudsql.logical_decoding = on
      - cloudsql.enable_pglogical = on
    """
    print("\n[Check 4] Verifying logical decoding flags on source instance...")
    try:
        instance = get_instance_details(project, instance_name)
    except RuntimeError as e:
        print(f"  FAIL: {e}")
        return False

    flags: dict[str, str] = {
        f["name"]: f["value"]
        for f in instance.get("settings", {}).get("databaseFlags", [])
    }

    all_passed = True

    logical_decoding = flags.get("cloudsql.logical_decoding", "")
    if logical_decoding == "on":
        print("  PASS: cloudsql.logical_decoding = on")
    else:
        print(
            f"  FAIL: cloudsql.logical_decoding = {logical_decoding!r} (expected 'on')"
        )
        all_passed = False

    enable_pglogical = flags.get("cloudsql.enable_pglogical", "")
    if enable_pglogical == "on":
        print("  PASS: cloudsql.enable_pglogical = on")
    else:
        print(
            f"  FAIL: cloudsql.enable_pglogical = {enable_pglogical!r} (expected 'on')"
        )
        all_passed = False

    if not all_passed:
        print(
            f"  Fix: gcloud sql instances patch {instance_name} --project {project} "
            "--database-flags=cloudsql.logical_decoding=on,cloudsql.enable_pglogical=on"
        )

    return all_passed


# ---------------------------------------------------------------------------
# KMS action functions
# ---------------------------------------------------------------------------


def create_kms_key(
    region: str,
    key_name: str,
    dry_run: bool = False,
) -> str:
    """
    Create a Cloud KMS key for a Cloud SQL instance.

    Returns the full KMS key resource path.
    """
    full_path = kms_key_path(region, key_name)
    if dry_run:
        print(f"  [DRY RUN] Would create KMS key: {full_path}")
        return full_path

    try:
        run_gcloud(
            [
                "kms",
                "keys",
                "create",
                key_name,
                "--keyring",
                KMS_KEYRING,
                "--location",
                region,
                "--project",
                KMS_KEY_PROJECT,
                "--purpose",
                "encryption",
            ]
        )
    except RuntimeError as e:
        if "ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
            print(f"  Key '{key_name}' already exists, reusing.")
            return full_path
        raise RuntimeError(f"Failed to create KMS key '{key_name}': {e}") from e

    print(f"  Created KMS key: {full_path}")
    return full_path


def set_key_rotation_schedule(
    region: str,
    key_name: str,
    dry_run: bool = False,
) -> None:
    """Set a 90-day rotation schedule on a KMS key."""
    next_rotation = (datetime.now(timezone.utc) + timedelta(days=90)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    if dry_run:
        print(
            f"  [DRY RUN] Would set rotation schedule on key '{key_name}' "
            f"(period={KEY_ROTATION_PERIOD}, next={next_rotation})"
        )
        return

    try:
        run_gcloud(
            [
                "kms",
                "keys",
                "set-rotation-schedule",
                key_name,
                "--keyring",
                KMS_KEYRING,
                "--location",
                region,
                "--project",
                KMS_KEY_PROJECT,
                "--rotation-period",
                KEY_ROTATION_PERIOD,
                "--next-rotation-time",
                next_rotation,
            ]
        )
    except RuntimeError as e:
        raise RuntimeError(
            f"Failed to set rotation schedule on key '{key_name}': {e}"
        ) from e
    print(
        f"  Rotation schedule set: period={KEY_ROTATION_PERIOD}, next={next_rotation}"
    )


def grant_cloudsql_sa_key_access(
    region: str,
    key_name: str,
    project_number: str,
    dry_run: bool = False,
) -> None:
    """Grant Cloud SQL service agent encrypter/decrypter access on a KMS key."""
    member = f"serviceAccount:service-{project_number}@gcp-sa-cloud-sql.iam.gserviceaccount.com"
    role = "roles/cloudkms.cryptoKeyEncrypterDecrypter"

    if dry_run:
        print(f"  [DRY RUN] Would grant {role} to {member} on key '{key_name}'")
        return

    try:
        run_gcloud(
            [
                "kms",
                "keys",
                "add-iam-policy-binding",
                key_name,
                "--keyring",
                KMS_KEYRING,
                "--location",
                region,
                "--project",
                KMS_KEY_PROJECT,
                "--member",
                member,
                "--role",
                role,
            ]
        )
    except RuntimeError as e:
        raise RuntimeError(
            f"Failed to grant {role} to {member} on key '{key_name}': {e}"
        ) from e
    print(f"  Granted {role} to {member}")


# ---------------------------------------------------------------------------
# Setup-migration action functions
# Each raises RuntimeError on failure.
# ---------------------------------------------------------------------------


_SYSTEM_DATABASES = {"template0", "template1", "cloudsqladmin"}


def list_user_databases(project: str, instance_name: str) -> list[str]:
    """Return the names of non-system databases on a Cloud SQL instance."""
    result = run_gcloud(
        [
            "sql",
            "databases",
            "list",
            f"--instance={instance_name}",
            "--project",
            project,
            "--format=value(name)",
        ]
    )
    return [
        db
        for db in result.stdout.strip().splitlines()
        if db and db not in _SYSTEM_DATABASES
    ]


def setup_source_for_dms(
    project: str,
    source_instance: str,
    db_user: str,
    db_password: str,
    dms_user_password: str,
    db_name: str,
    proxy_port: int,
    dry_run: bool = False,
) -> None:
    """
    Prepare the source database for DMS by creating the pglogical extension and dms_user.

    DMS requires pglogical to be installed on *every* user database in the
    instance, not just the primary one.  This function installs the extension
    and grants dms_user access to the pglogical schema on each user database,
    then runs the remaining setup (dms_user creation, public-schema grants)
    on the primary database.

    Connects via Cloud SQL Auth Proxy using psql.
    """
    # Statements that only need to run once (roles are instance-wide)
    instance_statements = [
        "DO $$ BEGIN "
        f"CREATE USER dms_user WITH REPLICATION LOGIN PASSWORD '{dms_user_password}'; "
        "EXCEPTION WHEN duplicate_object THEN "
        f"ALTER USER dms_user WITH REPLICATION LOGIN PASSWORD '{dms_user_password}'; "
        "END $$;",
        "GRANT cloudsqlsuperuser TO dms_user;",
    ]

    # Statements that must run on every user database
    per_db_statements = [
        "CREATE EXTENSION IF NOT EXISTS pglogical;",
        "GRANT USAGE ON SCHEMA public TO dms_user;",
        "GRANT SELECT ON ALL TABLES IN SCHEMA public TO dms_user;",
        "GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO dms_user;",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO dms_user;",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO dms_user;",
        "GRANT USAGE ON SCHEMA pglogical TO dms_user;",
        "GRANT SELECT ON ALL TABLES IN SCHEMA pglogical TO dms_user;",
    ]

    user_databases = list_user_databases(project, source_instance)
    print(f"  User databases: {', '.join(user_databases)}")

    if dry_run:
        print(f"  [DRY RUN] Would run once on {db_name} (instance-wide):")
        for stmt in instance_statements:
            display = (
                stmt
                if "PASSWORD" not in stmt
                else "<CREATE USER ... PASSWORD redacted>"
            )
            print(f"    {display}")
        print(
            f"  [DRY RUN] Would run on each user database ({', '.join(user_databases)}):"
        )
        for stmt in per_db_statements:
            print(f"    {stmt}")
        return

    with cloud_sql_proxy(project, source_instance, port=proxy_port):
        # Create dms_user (roles are instance-wide, so any DB works)
        for stmt in instance_statements:
            result = run_psql(
                db_user=db_user,
                db_password=db_password,
                db_name=db_name,
                sql=stmt,
                port=proxy_port,
            )
            if result.returncode != 0:
                display = (
                    stmt
                    if "PASSWORD" not in stmt
                    else "<CREATE USER ... PASSWORD redacted>"
                )
                raise RuntimeError(
                    f"SQL statement failed on {db_name}: {display}\n"
                    f"Error: {result.stderr.strip()}"
                )

        # Install pglogical and grant dms_user access on every user database
        for current_db in user_databases:
            print(f"  Setting up database '{current_db}' for DMS...")
            for stmt in per_db_statements:
                result = run_psql(
                    db_user=db_user,
                    db_password=db_password,
                    db_name=current_db,
                    sql=stmt,
                    port=proxy_port,
                )
                if result.returncode != 0:
                    raise RuntimeError(
                        f"SQL statement failed on {current_db}: {stmt}\n"
                        f"Error: {result.stderr.strip()}"
                    )

    print("  Source database prepared for DMS.")


@confirm_step("Proceed?")
def create_source_connection_profile(
    project: str,
    region: str,
    source_instance: str,
    db_name: str,
    dms_user_password: str,
    ssl_config: dict[str, str] | None = None,
    dry_run: bool = False,
) -> str:
    """
    Create a DMS connection profile for the source Cloud SQL instance.

    Fetches the instance's public IP to use as --host.

    If ssl_config is provided, it should contain keys: ca_certificate,
    client_certificate, private_key. These will be passed as
    --ssl-type=SERVER_CLIENT with the corresponding cert flags.

    Returns the profile name.
    """
    profile_name = f"{source_instance}{DMS_SOURCE_PROFILE_SUFFIX}"

    # DMS requires --host; use the instance's public IP.
    source_details = get_instance_details(project, source_instance)
    ip_addresses = source_details.get("ipAddresses", [])
    # Prefer PRIMARY (public) IP; fall back to any available IP.
    host_ip = next(
        (addr["ipAddress"] for addr in ip_addresses if addr.get("type") == "PRIMARY"),
        None,
    ) or next(
        (addr["ipAddress"] for addr in ip_addresses),
        None,
    )
    if not host_ip:
        raise RuntimeError(
            f"Source instance '{source_instance}' has no IP address. "
            "DMS requires --host for connection profiles."
        )

    ssl_type = "SERVER_CLIENT" if ssl_config else "NONE"

    if dry_run:
        print(
            f"  [DRY RUN] Would create source connection profile '{profile_name}' "
            f"(host={host_ip}, user=dms_user, db={db_name}, ssl={ssl_type})"
        )
        return profile_name

    cmd = [
        "database-migration",
        "connection-profiles",
        "create",
        "postgresql",
        profile_name,
        "--project",
        project,
        "--region",
        region,
        "--cloudsql-instance",
        source_instance,
        f"--host={host_ip}",
        "--port=5432",
        "--username=dms_user",
        f"--password={dms_user_password}",
        f"--database={db_name}",
        "--no-async",
    ]

    if ssl_config:
        cmd += [
            "--ssl-type=SERVER_CLIENT",
            f"--ca-certificate={ssl_config['ca_certificate']}",
            f"--client-certificate={ssl_config['client_certificate']}",
            f"--private-key={ssl_config['private_key']}",
        ]

    try:
        run_gcloud(cmd)
    except RuntimeError as e:
        if "ALREADY_EXISTS" in str(e):
            print(
                f"  Source connection profile '{profile_name}' already exists, skipping."
            )
            return profile_name
        raise RuntimeError(
            f"Failed to create source connection profile '{profile_name}': {e}"
        ) from e

    print(f"  Created source connection profile: {profile_name}")
    return profile_name


@catch_errors
def patch_dest_additional_settings(
    project: str,
    dest_instance: str,
    params: dict[str, Any],
    dry_run: bool = False,
) -> None:
    """Patch the destination instance to match source SSL and backup settings."""
    patch_args = [
        "sql",
        "instances",
        "patch",
        dest_instance,
        "--project",
        project,
    ]

    if params.get("ssl_mode"):
        patch_args.append(f"--ssl-mode={params['ssl_mode']}")

    if params.get("backup_start_time"):
        patch_args.append(f"--backup-start-time={params['backup_start_time']}")

    if params.get("point_in_time_recovery"):
        patch_args.append("--enable-point-in-time-recovery")

    if params.get("retained_backups_count") is not None:
        patch_args.append(
            f"--retained-backups-count={params['retained_backups_count']}"
        )

    if params.get("retained_transaction_log_days") is not None:
        patch_args.append(
            f"--retained-transaction-log-days={params['retained_transaction_log_days']}"
        )

    if params.get("backup_location"):
        patch_args.append(f"--backup-location={params['backup_location']}")

    patch_args.append("--quiet")

    if dry_run:
        print(
            f"  [DRY RUN] Would patch additional settings on '{dest_instance}':\n"
            f"    {' '.join(patch_args)}"
        )
        return

    print(f"  Patching additional settings on '{dest_instance}'...")
    run_gcloud(patch_args)
    print(f"  Additional settings patched on '{dest_instance}'")


def get_instance_creation_params(
    source_details: dict[str, Any],
) -> dict[str, Any]:
    """Extract instance creation parameters from source instance details.

    Returns a dict with keys: tier, edition, availability_type, zone,
    secondary_zone, db_flags, data_disk_type, data_disk_size_gb, user_labels,
    ssl_mode, server_ca_cert.
    """
    settings = source_details.get("settings", {})
    # Zone comes from the top-level gceZone, secondary from settings
    zone = source_details.get("gceZone")
    secondary_zone = source_details.get("secondaryGceZone")

    ip_config = settings.get("ipConfiguration", {})
    ssl_mode = ip_config.get("sslMode")

    # Server CA cert is embedded in the instance details
    server_ca_cert = source_details.get("serverCaCert", {}).get("cert")

    backup_config = settings.get("backupConfiguration", {})

    return {
        "tier": settings["tier"],
        "edition": (settings.get("edition") or "ENTERPRISE").lower(),
        "availability_type": settings.get("availabilityType", "REGIONAL"),
        "zone": zone,
        "secondary_zone": secondary_zone,
        "db_flags": settings.get("databaseFlags", []),
        "data_disk_type": settings.get("dataDiskType", "PD_SSD"),
        "data_disk_size_gb": settings.get("dataDiskSizeGb"),
        "user_labels": settings.get("userLabels", {}),
        "ssl_mode": ssl_mode,
        "server_ca_cert": server_ca_cert,
        "backup_start_time": backup_config.get("startTime"),
        "point_in_time_recovery": backup_config.get(
            "pointInTimeRecoveryEnabled", False
        ),
        "retained_backups_count": backup_config.get("backupRetentionSettings", {}).get(
            "retainedBackups"
        ),
        "retained_transaction_log_days": backup_config.get(
            "transactionLogRetentionDays"
        ),
        "backup_location": backup_config.get("location"),
    }


def _client_cert_name(instance_name: str) -> str:
    """Return the DMS client certificate name for a given instance."""
    return f"dms-client_{instance_name}"


def create_client_cert(
    project: str,
    instance_name: str,
) -> tuple[str, str]:
    """Create a client certificate on a Cloud SQL instance for DMS.

    The cert is named ``dms-client_<instance_name>``. If a cert with that name
    already exists the function raises RuntimeError with instructions to delete
    it, since the private key is only available at creation time and we cannot
    silently re-use an existing cert.

    Returns (client_cert_pem, private_key_pem).
    """
    cert_name = _client_cert_name(instance_name)

    # Check whether the cert already exists
    try:
        result = run_gcloud(
            [
                "sql",
                "ssl",
                "client-certs",
                "describe",
                cert_name,
                f"--instance={instance_name}",
                "--project",
                project,
                "--format=value(commonName)",
            ]
        )
        # Command succeeded — cert exists only if output is non-empty
        if result.stdout.strip():
            raise RuntimeError(
                f"Client cert '{cert_name}' already exists on instance '{instance_name}'. "
                f"Delete it first with:\n"
                f"  gcloud sql ssl client-certs delete {cert_name} "
                f"--instance={instance_name} --project {project} --quiet"
            )
    except RuntimeError as e:
        # Re-raise our own "already exists" error
        if "already exists" in str(e):
            raise
        # Otherwise the describe failed (cert doesn't exist) — continue

    with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as key_file:
        key_path = key_file.name
    # gcloud expects the file to not exist — it writes the key there itself
    os.unlink(key_path)

    run_gcloud(
        [
            "sql",
            "ssl",
            "client-certs",
            "create",
            cert_name,
            key_path,
            f"--instance={instance_name}",
            "--project",
            project,
        ]
    )

    # Read the private key
    with open(key_path, encoding="utf-8") as f:
        private_key_pem = f.read()
    os.unlink(key_path)

    # Fetch the client certificate
    result = run_gcloud(
        [
            "sql",
            "ssl",
            "client-certs",
            "describe",
            cert_name,
            f"--instance={instance_name}",
            "--project",
            project,
            "--format=value(cert)",
        ]
    )
    client_cert_pem = result.stdout.strip()

    print(f"  Created client cert '{cert_name}' on instance '{instance_name}'")
    return client_cert_pem, private_key_pem


def delete_client_cert(
    project: str,
    instance_name: str,
    dry_run: bool = False,
) -> None:
    """Delete the DMS client certificate from a Cloud SQL instance."""
    cert_name = _client_cert_name(instance_name)
    if dry_run:
        print(
            f"  [DRY RUN] Would delete client cert '{cert_name}' "
            f"from instance '{instance_name}'"
        )
        return

    try:
        run_gcloud(
            [
                "sql",
                "ssl",
                "client-certs",
                "delete",
                cert_name,
                f"--instance={instance_name}",
                "--project",
                project,
                "--quiet",
            ]
        )
        print(f"  Deleted client cert '{cert_name}' from instance '{instance_name}'")
    except RuntimeError as e:
        if "NOT_FOUND" in str(e) or "was not found" in str(e).lower():
            print(
                f"  Client cert '{cert_name}' not found on '{instance_name}', skipping."
            )
        else:
            raise


def create_dest_connection_profile(
    project: str,
    region: str,
    dest_instance: str,
    source_profile: str,
    source_details: dict[str, Any],
    key_path: str,
    db_password: str,
    dry_run: bool = False,
) -> str:
    """
    Create a DMS cloudsql connection profile for the destination.

    Uses the cloudsql connection profile type so that DMS creates and manages
    the destination Cloud SQL instance itself. This avoids the need to
    pre-create the instance and the demote-destination step.

    Returns the profile name.
    """
    profile_name = dest_instance
    params = get_instance_creation_params(source_details)

    if dry_run:
        print(
            f"  [DRY RUN] Would create cloudsql destination connection profile "
            f"'{profile_name}':"
        )
        print(f"    version:           {POSTGRES_18_VERSION}")
        print(f"    source_profile:    {source_profile}")
        print(f"    tier:              {params['tier']}")
        print(f"    edition:           {params['edition']}")
        print(f"    availability_type: {params['availability_type']}")
        print(f"    zone:              {params['zone']}")
        print(f"    secondary_zone:    {params['secondary_zone']}")
        print(f"    data_disk_type:    {params['data_disk_type']}")
        print(f"    data_disk_size_gb: {params['data_disk_size_gb']}")
        print(f"    db_flags:          {params['db_flags']}")
        print(f"    user_labels:       {params['user_labels']}")
        print(f"    ssl_mode:          {params['ssl_mode']}")
        print(
            f"    server_ca_cert:    {'<present>' if params['server_ca_cert'] else None}"
        )
        print(f"    cmek:              {key_path}")
        return profile_name

    cmd = [
        "database-migration",
        "connection-profiles",
        "create",
        "cloudsql",
        profile_name,
        "--project",
        project,
        "--region",
        region,
        "--role=DESTINATION",
        f"--database-version-name={POSTGRES_18_VERSION}",
        f"--source-id={source_profile}",
        f"--tier={params['tier']}",
        f"--edition={params['edition']}",
        f"--availability-type={params['availability_type']}",
        f"--data-disk-type={params['data_disk_type']}",
        "--auto-storage-increase",
        f"--root-password={db_password}",
        f"--cmek-key={key_path}",
        "--enable-ip-v4",
        "--no-async",
    ]

    if params["zone"]:
        cmd += [f"--zone={params['zone']}"]
    if params["secondary_zone"]:
        cmd += [f"--secondary-zone={params['secondary_zone']}"]
    if params["data_disk_size_gb"]:
        cmd += [f"--data-disk-size={params['data_disk_size_gb']}"]

    # Database flags
    if params["db_flags"]:
        cmd += [
            "--database-flags",
            ",".join(f"{f['name']}={f['value']}" for f in params["db_flags"]),
        ]

    # User labels
    if params["user_labels"]:
        cmd += [
            "--user-labels",
            ",".join(f"{k}={v}" for k, v in params["user_labels"].items()),
        ]

    # SSL
    if params["ssl_mode"] in (
        "TRUSTED_CLIENT_CERTIFICATE_REQUIRED",
        "ENCRYPTED_ONLY",
    ):
        cmd += ["--require-ssl"]

    try:
        run_gcloud(cmd)
    except RuntimeError as e:
        if "ALREADY_EXISTS" in str(e):
            print(
                f"  Destination connection profile '{profile_name}' already exists, skipping."
            )
            return profile_name
        raise RuntimeError(
            f"Failed to create destination connection profile '{profile_name}': {e}"
        ) from e

    print(f"  Created destination connection profile: {profile_name}")
    return profile_name


def create_migration_job(
    project: str,
    region: str,
    source_instance: str,
    source_profile: str,
    dest_profile: str,
    dry_run: bool = False,
) -> str:
    """
    Create a continuous (CDC) DMS migration job.

    Returns the job name.
    """
    job_name = f"{source_instance}{DMS_JOB_SUFFIX}"
    if dry_run:
        print(
            f"  [DRY RUN] Would create migration job '{job_name}' "
            f"(source={source_profile}, dest={dest_profile}, type=CONTINUOUS)"
        )
        return job_name

    try:
        run_gcloud(
            [
                "database-migration",
                "migration-jobs",
                "create",
                job_name,
                "--project",
                project,
                "--region",
                region,
                "--type=CONTINUOUS",
                f"--source={source_profile}",
                f"--destination={dest_profile}",
                "--no-async",
                "--static-ip",
            ]
        )
    except RuntimeError as e:
        if "ALREADY_EXISTS" in str(e):
            print(f"  Migration job '{job_name}' already exists, skipping creation.")
            return job_name
        raise RuntimeError(f"Failed to create migration job '{job_name}': {e}") from e

    print(f"  Created migration job: {job_name}")
    return job_name


@confirm_step(
    "Start the migration job? This will begin the replication process, during which "
    "tables cannot be dropped on the source instance. If now is not a good time, you can "
    "start the job from the DMS console later."
)
def start_migration_job(
    project: str, region: str, job_name: str, dry_run: bool = False
) -> None:
    """Start a DMS migration job."""
    if dry_run:
        print(f"  [DRY RUN] Would start migration job '{job_name}'")
        return

    try:
        run_gcloud(
            [
                "database-migration",
                "migration-jobs",
                "start",
                job_name,
                "--project",
                project,
                "--region",
                region,
            ]
        )
    except RuntimeError as e:
        raise RuntimeError(f"Failed to start migration job '{job_name}': {e}") from e

    print(f"  Migration job '{job_name}' started.")


# ---------------------------------------------------------------------------
# DMS monitoring utilities
# ---------------------------------------------------------------------------


def get_migration_job_details(
    project: str, region: str, job_name: str
) -> dict[str, Any]:
    """Fetch DMS migration job details as parsed JSON."""
    try:
        result = run_gcloud(
            [
                "database-migration",
                "migration-jobs",
                "describe",
                job_name,
                "--project",
                project,
                "--region",
                region,
                "--format",
                "json",
            ]
        )
    except RuntimeError as e:
        raise RuntimeError(f"Failed to describe migration job '{job_name}': {e}") from e
    return json.loads(result.stdout)


@confirm_step("Proceed with cutover?")
def promote_migration_job(
    project: str, region: str, job_name: str, dry_run: bool = False
) -> None:
    """Promote a DMS migration job, making the destination read-write."""
    if dry_run:
        print(f"  [DRY RUN] Would promote migration job '{job_name}'")
        return

    try:
        run_gcloud(
            [
                "database-migration",
                "migration-jobs",
                "promote",
                job_name,
                "--project",
                project,
                "--region",
                region,
            ]
        )
    except RuntimeError as e:
        raise RuntimeError(f"Failed to promote migration job '{job_name}': {e}") from e

    print(f"  Migration job '{job_name}' promoted. Destination is now read-write.")


def wait_for_instance_operations(
    project: str,
    instance_name: str,
    timeout: int = 600,
    poll_interval: int = 15,
) -> None:
    """Wait until all pending Cloud SQL operations on the instance complete."""
    print(f"  Waiting for pending operations on '{instance_name}' to complete...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = run_gcloud(
            [
                "sql",
                "operations",
                "list",
                f"--instance={instance_name}",
                "--project",
                project,
                "--filter=status!=DONE",
                "--format=value(name)",
            ]
        )
        pending = [op for op in result.stdout.strip().splitlines() if op]
        if not pending:
            print(f"  All operations on '{instance_name}' are complete.")
            return
        print(
            f"  {len(pending)} operation(s) still running, "
            f"waiting {poll_interval}s..."
        )
        time.sleep(poll_interval)
    raise RuntimeError(
        f"Timed out after {timeout}s waiting for operations on '{instance_name}'"
    )


def set_secret_value(project: str, secret_id: str, value: str) -> None:
    """Push a new version to a Secret Manager secret.

    Uses subprocess.run directly because we need to pipe stdin (--data-file=-),
    which run_gcloud doesn't support.
    """
    full_cmd = [
        "gcloud",
        "secrets",
        "versions",
        "add",
        secret_id,
        "--project",
        project,
        "--data-file=-",
    ]
    print(f"Running command: {' '.join(full_cmd)}")
    result = subprocess.run(  # nosec B603
        full_cmd,
        input=value,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to update secret '{secret_id}': {result.stderr.strip()}"
        )
    print(f"  Secret '{secret_id}' updated.")


@catch_errors
def update_prefixed_secret(
    project: str,
    db_secret_prefix: str,
    secret_suffix: str,
    new_value: str,
    dry_run: bool = False,
) -> None:
    """Update the {prefix}_{suffix} secret with a new value."""
    secret_id = f"{db_secret_prefix}_{secret_suffix}"

    # Log the current value for operator awareness
    try:
        current = get_secret(project, secret_id)
        print(f"  Current value: {current}")
    except RuntimeError:
        print(f"  Could not read current value of '{secret_id}' (may not exist yet)")

    print(f"  New value:     {new_value}")

    if dry_run:
        print(f"  [DRY RUN] Would update secret '{secret_id}'")
        return

    set_secret_value(project, secret_id, new_value)


AIRFLOW_CONNECTION_SECRET = (
    "airflow-connections-operations_postgres_conn_id"  # nosec B105
)
AIRFLOW_CONNECTION_DB_SECRET_PREFIX = "operations_v2"  # nosec B105


@catch_errors
def update_airflow_connection_secret(
    project: str,
    db_secret_prefix: str,
    dest_instance: str,
    dest_ip: str,
    dry_run: bool = False,
) -> None:
    """Update the Airflow connection secret's host and instance if applicable.

    Only the operations DB has an Airflow connection secret. If the
    db_secret_prefix doesn't match, this is a no-op.
    """
    if db_secret_prefix != AIRFLOW_CONNECTION_DB_SECRET_PREFIX:
        print(
            f"  Skipping Airflow connection update "
            f"(prefix '{db_secret_prefix}' != '{AIRFLOW_CONNECTION_DB_SECRET_PREFIX}')"
        )
        return

    secret_id = AIRFLOW_CONNECTION_SECRET

    try:
        current_value = get_secret(project, secret_id)
    except RuntimeError as e:
        raise RuntimeError(
            f"Failed to read Airflow connection secret '{secret_id}': {e}"
        ) from e

    parsed = urlparse(current_value)
    params = parse_qs(parsed.query, keep_blank_values=True)

    old_instance = params.get("instance", ["<not set>"])[0]
    old_host = parsed.hostname or "<not set>"
    print(f"  Current host:     {old_host}")
    print(f"  Current instance: {old_instance}")
    print(f"  New host:         {dest_ip}")
    print(f"  New instance:     {dest_instance}")

    if old_instance == dest_instance and old_host == dest_ip:
        print("  Already up to date, skipping.")
        return

    if dry_run:
        print(f"  [DRY RUN] Would update '{secret_id}'")
        return

    # Replace host (IP) in the netloc
    new_netloc = parsed.netloc.replace(old_host, dest_ip)
    # Replace instance in the query params
    params["instance"] = [dest_instance]
    new_query = urlencode(params, doseq=True)
    new_uri = urlunparse(parsed._replace(netloc=new_netloc, query=new_query))
    set_secret_value(project, secret_id, new_uri)


# ---------------------------------------------------------------------------
# BigQuery connection update
# ---------------------------------------------------------------------------


@catch_errors
def update_bq_connection(
    project: str,
    db_secret_prefix: str,
    dest_connection_name: str,
    region: str,
    dry_run: bool = False,
) -> None:
    """Update the BigQuery Cloud SQL connection to point to the new instance.

    The connection is named ``{db_secret_prefix}_cloudsql`` and lives in the
    same region as the Cloud SQL instance.
    """
    bq_connection = f"{db_secret_prefix}_cloudsql"
    connection_path = "--connection_type=CLOUD_SQL"
    properties = json.dumps({"instanceId": dest_connection_name})

    print(f"  BQ connection:   {bq_connection}")
    print(f"  New instance ID: {dest_connection_name}")

    if dry_run:
        print(f"  [DRY RUN] Would update BQ connection '{bq_connection}'")
        return

    run_bq(
        [
            "update",
            "--connection",
            connection_path,
            f"--properties={properties}",
            f"--location={region}",
            f"--project_id={project}",
            bq_connection,
        ]
    )
    print(f"  BQ connection '{bq_connection}' updated successfully.")


# ---------------------------------------------------------------------------
# Cloud Run resource discovery and update
# ---------------------------------------------------------------------------


def _get_cloudsql_instances_annotation(resource: dict[str, Any]) -> str:
    """Extract the cloudsql-instances value from a Cloud Run resource JSON.

    Works for both services and jobs across v1 (Knative) and v2 API formats.
    """
    # v1/Knative style: metadata.annotations
    annotation = (
        resource.get("metadata", {})
        .get("annotations", {})
        .get(CLOUD_RUN_ANNOTATION_KEY, "")
    )
    if annotation:
        return annotation

    # v1/Knative style: spec.template.metadata.annotations
    annotation = (
        resource.get("spec", {})
        .get("template", {})
        .get("metadata", {})
        .get("annotations", {})
        .get(CLOUD_RUN_ANNOTATION_KEY, "")
    )
    if annotation:
        return annotation

    # v2 style: template.template.volumes[].cloudSqlInstance.instances
    for volume in resource.get("template", {}).get("template", {}).get("volumes", []):
        instances = volume.get("cloudSqlInstance", {}).get("instances", [])
        if instances:
            return ",".join(instances)

    return ""


def _get_resource_name(resource: dict[str, Any]) -> str:
    """Extract the short name from a Cloud Run resource JSON."""
    # v1: metadata.name
    name = resource.get("metadata", {}).get("name", "")
    if name:
        return name
    # v2: fully-qualified "name" field (projects/P/locations/R/.../NAME)
    name = resource.get("name", "")
    if "/" in name:
        return name.rsplit("/", 1)[-1]
    return name


def _get_resource_region(resource: dict[str, Any]) -> str | None:
    """Extract the region from a Cloud Run resource JSON."""
    # v1: metadata.labels
    region = (
        resource.get("metadata", {})
        .get("labels", {})
        .get("cloud.googleapis.com/location", "")
    )
    if region:
        return region

    # v1: parse from selfLink (.../locations/{region}/...)
    self_link = resource.get("metadata", {}).get("selfLink", "")
    if self_link:
        parts = self_link.split("/")
        try:
            loc_idx = parts.index("locations")
            return parts[loc_idx + 1]
        except (ValueError, IndexError):
            pass

    # v2: parse from fully-qualified name
    name = resource.get("name", "")
    parts = name.split("/")
    try:
        loc_idx = parts.index("locations")
        return parts[loc_idx + 1]
    except (ValueError, IndexError):
        pass

    return None


def discover_cloud_run_resources(
    project: str,
    source_connection_name: str,
) -> list[tuple[CloudRunResourceType, str, str]]:
    """Discover Cloud Run services and jobs that reference the source instance.

    Returns a list of (resource_type, name, region) triples, where
    resource_type is ``"services"`` or ``"jobs"``.
    """
    found: list[tuple[CloudRunResourceType, str, str]] = []

    for resource_type in ("services", "jobs"):
        try:
            result = run_gcloud(
                [
                    "run",
                    resource_type,
                    "list",
                    "--project",
                    project,
                    "--format",
                    "json",
                ]
            )
        except RuntimeError as e:
            raise RuntimeError(f"Failed to list Cloud Run {resource_type}: {e}") from e

        resources = json.loads(result.stdout)

        for resource in resources:
            name = _get_resource_name(resource)
            cloudsql_value = _get_cloudsql_instances_annotation(resource)

            if source_connection_name not in cloudsql_value:
                continue

            region = _get_resource_region(resource)
            if not region:
                print(
                    f"  WARNING: Could not determine region for "
                    f"{resource_type.rstrip('s')} '{name}', skipping"
                )
                continue

            found.append((resource_type, name, region))
            print(f"  Found {resource_type.rstrip('s')}: {name} (region={region})")

    if not found:
        print("  No Cloud Run resources reference the source instance.")

    return found


@catch_errors
def update_cloud_run_resources(
    project: str,
    resources: list[tuple[CloudRunResourceType, str, str]],
    dest_connection_name: str,
    dry_run: bool = False,
) -> list[tuple[CloudRunResourceType, str, str]]:
    """Update Cloud Run services and jobs to add the destination instance.

    Uses ``--add-cloudsql-instances`` so only the new instance needs to be
    specified.

    Returns the list of (resource_type, name, region) triples that were
    successfully updated.
    """
    if not resources:
        return []

    print(f"\n  Updating {len(resources)} Cloud Run resource(s)...")

    if dry_run:
        for rtype, name, region in resources:
            print(f"  [DRY RUN] Would update {rtype.rstrip('s')} '{name}' in {region}")
        return list(resources)

    def _update_resource(
        item: tuple[CloudRunResourceType, str, str],
    ) -> str:
        rtype, rname, rregion = item
        run_gcloud(
            [
                "run",
                rtype,
                "update",
                rname,
                "--project",
                project,
                "--region",
                rregion,
                f"--add-cloudsql-instances={dest_connection_name}",
            ]
        )
        return rname

    executor_result = map_fn_with_progress_bar_results(
        work_items=resources,
        work_fn=_update_resource,
        progress_bar_message="Updating Cloud Run resources",
        overall_timeout_sec=600,
        single_work_item_timeout_sec=300,
        max_workers=len(resources),
    )

    updated = [item for item, _ in executor_result.successes]

    if executor_result.exceptions:
        for (rtype, name, region), exc in executor_result.exceptions:
            print(f"  FAILED: {rtype.rstrip('s')} {name} (region={region}): {exc}")
        failed_names = [
            f"{rtype.rstrip('s')} {name}"
            for (rtype, name, _), _ in executor_result.exceptions
        ]
        succeeded_names = [f"{rtype.rstrip('s')} {name}" for rtype, name, _ in updated]
        raise RuntimeError(
            f"Failed to update {len(executor_result.exceptions)} resource(s): "
            f"{', '.join(failed_names)}. "
            f"Successfully updated: {', '.join(succeeded_names) or 'none'}."
        )

    return updated


def wait_for_service_deployments(
    project: str,
    services: list[tuple[str, str]],
) -> None:
    """Wait for each Cloud Run service's latest revision to serve 100% traffic.

    Args:
        services: list of (service_name, region) tuples.
    """
    if not services:
        return

    print(f"\nWaiting for {len(services)} service deployment(s) to complete...")
    timeout_seconds = SERVICE_DEPLOY_TIMEOUT_MINUTES * 60

    for svc_name, svc_region in services:
        print(f"  Waiting for '{svc_name}' (region={svc_region})...")
        start_time = time.time()

        while True:
            try:
                result = run_gcloud(
                    [
                        "run",
                        "services",
                        "describe",
                        svc_name,
                        "--project",
                        project,
                        "--region",
                        svc_region,
                        "--format",
                        "json",
                    ]
                )
                svc_details = json.loads(result.stdout)
                traffic = svc_details.get("status", {}).get("traffic", [])
                if (
                    traffic
                    and traffic[0].get("latestRevision")
                    and traffic[0].get("percent") == 100
                ):
                    print(f"  '{svc_name}' is fully deployed.")
                    break
            except (RuntimeError, json.JSONDecodeError, KeyError):
                pass

            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                raise RuntimeError(
                    f"Timeout waiting for '{svc_name}' to deploy "
                    f"(waited {SERVICE_DEPLOY_TIMEOUT_MINUTES} minutes)"
                )

            print(f"    Still deploying... ({int(elapsed)}s elapsed)")
            time.sleep(SERVICE_DEPLOY_POLL_INTERVAL_SECONDS)


# ---------------------------------------------------------------------------
# Phase orchestrators
# ---------------------------------------------------------------------------


def check_upgrade_compatibility(project: str, instance_name: str) -> bool:
    """
    Optionally run the major-version-upgrade pre-check on the source instance.

    Prompts the user before running (the check doesn't affect uptime but may take
    a while). Returns True if the check passed or was skipped, False if it failed.
    """
    print("\n[Check 5] Major-version-upgrade compatibility pre-check")
    if not prompt_for_step(
        "  Run upgrade compatibility pre-check? "
        "(no impact on performance/uptime, but may take a while)"
    ):
        print("  SKIP: Upgrade compatibility check skipped.")
        return True

    try:
        result = run_gcloud(
            [
                "sql",
                "instances",
                "pre-check-major-version-upgrade",
                instance_name,
                "--project",
                project,
                "--target-database-version",
                POSTGRES_18_VERSION,
                "--format",
                "json",
            ]
        )
    except RuntimeError as e:
        print(f"  FAIL: Upgrade compatibility pre-check failed: {e}")
        return False

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        # Some gcloud versions print non-JSON for this command; treat non-empty stdout as success.
        if result.stdout.strip():
            print(
                f"  PASS: Upgrade compatibility pre-check completed.\n  Output: {result.stdout.strip()}"
            )
            return True
        print("  FAIL: Unexpected empty output from pre-check.")
        return False

    # The response is an Operation. If it completed without error, the pre-check passed.
    status = data.get("status", "")
    errors = data.get("error", {})
    if errors:
        print(f"  FAIL: Upgrade compatibility pre-check reported errors: {errors}")
        return False

    print(f"  PASS: Upgrade compatibility pre-check completed (status={status}).")
    return True


def run_preflight(args: argparse.Namespace, project: str) -> int:
    """Run all preflight checks and report results."""
    print("\n" + "=" * 60)
    print("PREFLIGHT CHECKS")
    print("=" * 60)

    source_details = get_instance_details(project, args.source_instance)
    region = source_details["region"]

    results: list[bool] = []

    # Check 1: APIs
    results.append(check_apis_enabled(project))

    # Check 2: KMS access
    results.append(check_kms_access(region))

    # Check 3: Service agents
    results.append(check_service_agents(project))

    # Check 4: Logical decoding flags on source instance
    results.append(check_logical_decoding_flags(project, args.source_instance))

    # Check 5: Upgrade compatibility pre-check (optional, user-prompted)
    results.append(check_upgrade_compatibility(project, args.source_instance))

    failed = sum(1 for r in results if not r)
    print("\n" + "=" * 60)
    if failed == 0:
        print("All preflight checks PASSED.")
        print("\nNext step: run the 'setup-migration' phase.")
    else:
        print(f"{failed} of {len(results)} preflight checks FAILED.")
        print("Resolve the issues above before proceeding.")
    print("=" * 60)

    return 0 if failed == 0 else 1


def run_setup_migration(args: argparse.Namespace, project: str) -> int:
    """Prepare source DB for DMS, create connection profiles, create and start migration job."""
    dest_instance = derive_dest_instance_name(
        args.source_instance, DEST_SUFFIX, args.new_instance_prefix
    )
    source_details = get_instance_details(project, args.source_instance)
    region = source_details["region"]

    print("\n" + "=" * 60)
    print("SETUP MIGRATION")
    print("=" * 60)
    print(f"  Source instance:      {args.source_instance}")
    print(f"  Destination instance: {dest_instance}")
    print(f"  Region:               {region}")
    print(f"  Database:             {DB_NAME}")
    if args.dry_run:
        print("  [DRY RUN MODE]")

    db_password = resolve_db_password(project, args, prompt_if_missing=True)
    assert db_password is not None
    # Step 1: Create KMS key
    project_number = get_project_number(project)

    print("\n[Step 1] Creating KMS key...")
    key_path = create_kms_key(
        region=region,
        key_name=dest_instance,
        dry_run=args.dry_run,
    )

    # Step 2: Set rotation schedule
    print("\n[Step 2] Setting 90-day key rotation schedule...")
    set_key_rotation_schedule(
        region=region,
        key_name=dest_instance,
        dry_run=args.dry_run,
    )

    # Step 3: Grant Cloud SQL service agent access
    print("\n[Step 3] Granting Cloud SQL service agent KMS access...")
    grant_cloudsql_sa_key_access(
        region=region,
        key_name=dest_instance,
        project_number=project_number,
        dry_run=args.dry_run,
    )
    # Step 4: Set up source database for DMS
    print("\n[Step 4] Preparing source database for DMS...")
    setup_source_for_dms(
        project=project,
        source_instance=args.source_instance,
        db_user=DB_USER,
        db_password=db_password,
        dms_user_password=db_password,
        db_name=DB_NAME,
        proxy_port=PROXY_PORT,
        dry_run=args.dry_run,
    )

    # Step 5: Prepare SSL client certs (if source requires SSL)
    params = get_instance_creation_params(source_details)
    ssl_config = None
    if params["ssl_mode"] in (
        "TRUSTED_CLIENT_CERTIFICATE_REQUIRED",
        "ENCRYPTED_ONLY",
    ):
        cert_name = _client_cert_name(args.source_instance)
        print("\n[Step 5] Creating SSL client certificate for DMS...")
        if args.dry_run:
            print(
                f"  [DRY RUN] Would create client cert '{cert_name}' "
                f"on '{args.source_instance}'"
            )
        else:
            try:
                client_cert, private_key = create_client_cert(
                    project=project,
                    instance_name=args.source_instance,
                )
            except RuntimeError as e:
                print(f"  ERROR: Failed to create client cert: {e}")
                print(
                    "  Run the 'cleanup' phase to remove stale certs, "
                    "then re-run 'setup-migration'."
                )
                return 1
            ssl_config = {
                "ca_certificate": params["server_ca_cert"],
                "client_certificate": client_cert,
                "private_key": private_key,
            }

    # Step 6: Create connection profiles
    print("\n[Step 6] Creating DMS connection profiles...")

    source_profile = create_source_connection_profile(
        project=project,
        region=region,
        source_instance=args.source_instance,
        db_name=DB_NAME,
        dms_user_password=db_password,
        ssl_config=ssl_config,
        dry_run=args.dry_run,
    )
    key_path = kms_key_path(region, dest_instance)
    dest_profile = create_dest_connection_profile(
        project=project,
        region=region,
        dest_instance=dest_instance,
        source_profile=source_profile,
        source_details=source_details,
        key_path=key_path,
        db_password=db_password,
        dry_run=args.dry_run,
    )

    # Step 7: Allowlist destination outgoing IPs on source instance
    print("\n[Step 7] Allowlisting destination IPs on source instance...")
    allowlist_dest_ips_on_source(
        project=project,
        source_instance=args.source_instance,
        dest_instance=dest_instance,
        dry_run=args.dry_run,
    )

    # Step 8: Create, verify, and start migration job
    print("\n[Step 8] Creating migration job...")
    job_name = create_migration_job(
        project=project,
        region=region,
        source_instance=args.source_instance,
        source_profile=source_profile,
        dest_profile=dest_profile,
        dry_run=args.dry_run,
    )

    print("\n[Step 9] Starting migration job...")
    start_migration_job(
        project=project, region=region, job_name=job_name, dry_run=args.dry_run
    )

    print("\n" + "=" * 60)
    if args.dry_run:
        print("DRY RUN COMPLETE — no changes were made.")
    else:
        print("SETUP MIGRATION COMPLETE")
        print(f"  Migration job:        {job_name}")
        print(f"  Source profile:       {source_profile}")
        print(f"  Destination profile:  {dest_profile}")
        print("\nNext steps:")
        print(
            f"  - Monitor replication lag:\n"
            f"    gcloud database-migration migration-jobs describe {job_name} "
            f"--region={region} --format='json(state,phase,duration,error)'"
        )
        print("  - When phase=CDC and lag is near zero, run the 'cutover' phase")
    print("=" * 60)

    return 0


def run_cutover(args: argparse.Namespace, project: str) -> int:
    """Promote DMS job, update instance ID secret, redeploy Cloud Run services."""
    dest_instance = derive_dest_instance_name(
        args.source_instance, DEST_SUFFIX, args.new_instance_prefix
    )
    job_name = f"{args.source_instance}{DMS_JOB_SUFFIX}"
    source_details = get_instance_details(project, args.source_instance)
    region = source_details["region"]

    db_password = resolve_db_password(project, args, prompt_if_missing=True)
    assert db_password is not None

    # Build list of secrets that will be updated
    secrets_to_update = [
        f"{args.db_secret_prefix}_cloudsql_instance_id",
        f"{args.db_secret_prefix}_db_host",
    ]
    if args.db_secret_prefix == AIRFLOW_CONNECTION_DB_SECRET_PREFIX:
        secrets_to_update.append(AIRFLOW_CONNECTION_SECRET)

    print("\n" + "=" * 60)
    print("CUTOVER")
    print("=" * 60)
    print(f"  Source instance:      {args.source_instance}")
    print(f"  Destination instance: {dest_instance}")
    print(f"  Region:               {region}")
    print(f"  DMS job:              {job_name}")
    if args.dry_run:
        print("  [DRY RUN MODE]")

    # Step 1: Validate DMS job status
    print("\n[Step 1] Validating migration job status...")
    details = get_migration_job_details(project, region, job_name)
    phase = details.get("phase", "UNKNOWN")
    state = details.get("state", "UNKNOWN")
    print(f"  Job state={state}, phase={phase}")
    already_promoted = state == "COMPLETED"
    if phase != "CDC" and not already_promoted:
        raise RuntimeError(
            f"Migration job '{job_name}' is in state='{state}', phase='{phase}', "
            f"expected phase='CDC' or state='COMPLETED'."
        )
    if already_promoted:
        print("  Migration job is already COMPLETED (destination already promoted).")
        _flush_stdin()
        prompt_for_confirmation("  Continue with remaining cutover steps?")

    # Step 2: Discover Cloud Run resources that will be updated
    print("\n[Step 2] Discovering Cloud Run resources to update...")
    source_connection_name = get_instance_connection_name(project, args.source_instance)
    dest_connection_name = get_instance_connection_name(project, dest_instance)
    cloud_run_resources = discover_cloud_run_resources(
        project=project,
        source_connection_name=source_connection_name,
    )

    # Step 3: Display cutover details
    # Best-effort desktop notification
    if shutil.which("alerter"):
        try:
            subprocess.run(  # nosec B603 B607
                ["alerter", "-message", "Migration ready for cutover", "-title", "DMS"],
                capture_output=True,
                timeout=5,
                check=False,
            )
        except (subprocess.TimeoutExpired, OSError):
            pass

    print("\n" + "-" * 60)
    print("READY FOR CUTOVER")
    print("-" * 60)
    print(f"  DMS job:          {job_name}")
    print(f"  Dest instance:    {dest_instance}")
    print(f"  Secrets to update ({len(secrets_to_update)}):")
    for s in secrets_to_update:
        print(f"    {s}")
    if cloud_run_resources:
        print(f"  Cloud Run resources to update ({len(cloud_run_resources)}):")
        for rtype, rname, rregion in cloud_run_resources:
            print(f"    {rtype.rstrip('s')}: {rname} ({rregion})")
    else:
        print("  Cloud Run resources to update: none")
    print()
    print("Before proceeding, verify replication lag is near zero in the Console:")
    print(
        f"  https://console.cloud.google.com/dbmigration/migrations/jobs?project={project}"
    )
    print(f"  Open migration job '{job_name}' and check the replication lag.")
    print()
    print("This will:")
    print(
        "  1. Promote the DMS job (source becomes READ-ONLY, dest becomes read-write)"
    )
    print("  2. Update the secrets listed above")
    print("  3. Update Cloud Run services and jobs referencing the source instance")
    print()
    print("Services will experience brief write errors between promote and redeploy.")

    # Step 4: Promote DMS migration job (@confirm_step prompts before executing)
    if already_promoted:
        print("\n[Step 4] Skipping promote — migration job already completed.")
    else:
        # NOTE: We do not stop Cloud Run services before promoting. The promote
        # makes the source instance read-only, so writes fail immediately. The
        # brief error window between promote and redeployment completing is
        # acceptable — stopping services would add complexity and its own failure
        # modes.
        print("\n[Step 4] Promoting DMS migration job...")
        promote_migration_job(
            project=project, region=region, job_name=job_name, dry_run=args.dry_run
        )

    if not args.dry_run:
        wait_for_instance_operations(project, dest_instance)

    # Step 5: Patch SSL and backup settings on promoted destination instance
    print("\n[Step 5] Patching additional settings on destination instance...")
    params = get_instance_creation_params(source_details)
    patch_dest_additional_settings(
        project=project,
        dest_instance=dest_instance,
        params=params,
        dry_run=args.dry_run,
    )

    # Step 6: Update sequences (DMS does not migrate sequence values)
    print("\n[Step 6] Updating sequences on destination instance...")
    update_sequences(
        project=project,
        instance_name=dest_instance,
        db_user=DB_USER,
        db_password=db_password,
        port=PROXY_PORT,
        dry_run=args.dry_run,
    )

    # Step 7: Update secrets
    print("\n[Step 7a] Updating cloudsql_instance_id secret...")
    update_prefixed_secret(
        project=project,
        db_secret_prefix=args.db_secret_prefix,
        secret_suffix="cloudsql_instance_id",  # nosec B106
        new_value=dest_connection_name,
        dry_run=args.dry_run,
    )

    print("\n[Step 7b] Updating db_host secret...")
    dest_details = get_instance_details(project, dest_instance)
    dest_ip = next(
        (
            addr["ipAddress"]
            for addr in dest_details.get("ipAddresses", [])
            if addr.get("type") == "PRIMARY"
        ),
        None,
    )
    if dest_ip:
        update_prefixed_secret(
            project=project,
            db_secret_prefix=args.db_secret_prefix,
            secret_suffix="db_host",  # nosec B106
            new_value=dest_ip,
            dry_run=args.dry_run,
        )
    else:
        print(
            f"  WARNING: No PRIMARY IP found on '{dest_instance}', skipping db_host update"
        )

    # Step 8: Update Airflow connection secret (if applicable)
    print("\n[Step 8] Updating Airflow connection secret...")
    update_airflow_connection_secret(
        project=project,
        db_secret_prefix=args.db_secret_prefix,
        dest_instance=dest_instance,
        dest_ip=dest_ip or "",
        dry_run=args.dry_run,
    )

    # Step 9: Update BigQuery connection
    print("\n[Step 9] Updating BigQuery connection...")
    update_bq_connection(
        project=project,
        db_secret_prefix=args.db_secret_prefix,
        dest_connection_name=dest_connection_name,
        region=region,
        dry_run=args.dry_run,
    )

    # Step 10: Update Cloud Run services and jobs
    print("\n[Step 10] Updating Cloud Run resources...")
    updated = (
        update_cloud_run_resources(
            project=project,
            resources=cloud_run_resources,
            dest_connection_name=dest_connection_name,
            dry_run=args.dry_run,
        )
        or []
    )

    # Step 11: Wait for service deployments (jobs don't serve traffic)
    updated_services = [
        (name, rgn) for rtype, name, rgn in updated if rtype == "services"
    ]
    if not args.dry_run:
        print("\n[Step 11] Waiting for service deployments...")
        wait_for_service_deployments(project, updated_services)
    else:
        print("\n[Step 11] [DRY RUN] Would wait for service deployments")

    print("\n" + "=" * 60)
    if args.dry_run:
        print("DRY RUN COMPLETE — no changes were made.")
    else:
        updated_names = [f"{name} ({rtype.rstrip('s')})" for rtype, name, _ in updated]
        print("CUTOVER COMPLETE")
        print(f"  DMS job promoted:       {job_name}")
        print(f"  Secrets updated:        {', '.join(secrets_to_update)}")
        print(f"  Resources updated:      {', '.join(updated_names) or 'none'}")
        print("\nNext steps:")
        print("  - Verify application health")
        print("  - Monitor for errors in Cloud Run logs")
        print(
            "  - When satisfied, run the cleanup phase to remove the old "
            "instance and migration resources"
        )
    print("=" * 60)

    return 0


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the migration script."""
    parser = argparse.ArgumentParser(
        prog="migrate_to_pg18_cmek.py",
        description="Migrate Cloud SQL instances to Postgres 18 with CMEK.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "source_instance",
        help="Name of the source Cloud SQL instance.",
    )
    parser.add_argument(
        "--project",
        required=True,
        help="GCP project ID.",
    )
    parser.add_argument(
        "--new-instance-prefix",
        help=(
            "Override the base name used for the destination instance. "
            "By default the source instance name is used, so "
            "'my-instance' becomes 'my-instance-cmek'. Pass e.g. "
            "'my-prefix' to get 'my-prefix-cmek' instead."
        ),
    )
    parser.add_argument(
        "--db-secret-prefix",
        help=(
            "Secret Manager prefix for this database (e.g. 'persistence', "
            "'public_pathways'). Used to derive secret names like "
            "{prefix}_cloudsql_instance_id and {prefix}_db_password. "
            "Required for cutover."
        ),
    )

    subparsers = parser.add_subparsers(dest="phase", required=True)

    # preflight
    subparsers.add_parser(
        "preflight",
        help="Check that prerequisites are satisfied.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # setup-migration
    setup_p = subparsers.add_parser(
        "setup-migration",
        help=(
            "Prepare source DB for DMS, create connection profiles, "
            "create and start migration job."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    setup_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without making changes.",
    )

    # cutover
    cutover_p = subparsers.add_parser(
        "cutover",
        help="Promote DMS job, update instance ID secret, redeploy Cloud Run services.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    cutover_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without making changes.",
    )

    # cleanup
    cleanup_p = subparsers.add_parser(
        "cleanup",
        help="Delete migration job, source and destination connection profiles.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    cleanup_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without making changes.",
    )

    return parser


@confirm_step("Delete migration job?")
@catch_errors
def _cleanup_delete_migration_job(
    project: str, region: str, job_name: str, dry_run: bool
) -> None:
    """Delete the DMS migration job, polling until it is fully removed."""
    if dry_run:
        print(f"  [DRY RUN] Would delete migration job '{job_name}'")
        return
    try:
        run_gcloud(
            [
                "database-migration",
                "migration-jobs",
                "delete",
                job_name,
                "--project",
                project,
                "--region",
                region,
                "--quiet",
            ]
        )
        print(f"  Delete initiated for migration job '{job_name}', waiting...")
        start_time = time.time()
        while time.time() - start_time < 300:
            try:
                run_gcloud(
                    [
                        "database-migration",
                        "migration-jobs",
                        "describe",
                        job_name,
                        "--project",
                        project,
                        "--region",
                        region,
                    ]
                )
                # Still exists, keep waiting
                time.sleep(10)
            except RuntimeError as describe_err:
                if "NOT_FOUND" in str(describe_err):
                    break
                raise
        print(f"  Deleted migration job '{job_name}'")
    except RuntimeError as e:
        if "NOT_FOUND" in str(e):
            print(f"  Migration job '{job_name}' not found, skipping.")
        else:
            raise


@confirm_step("Delete connection profile?")
@catch_errors
def _cleanup_delete_connection_profile(
    project: str, region: str, profile_name: str, dry_run: bool
) -> None:
    if dry_run:
        print(f"  [DRY RUN] Would delete connection profile '{profile_name}'")
        return
    try:
        run_gcloud(
            [
                "database-migration",
                "connection-profiles",
                "delete",
                profile_name,
                "--project",
                project,
                "--region",
                region,
                "--quiet",
            ]
        )
        print(f"  Deleted connection profile '{profile_name}'")
    except RuntimeError as e:
        if "NOT_FOUND" in str(e):
            print(f"  Connection profile '{profile_name}' not found, skipping.")
        else:
            raise


@confirm_step("Delete DMS client certificate?")
@catch_errors
def _cleanup_delete_client_cert(
    project: str, instance_name: str, dry_run: bool
) -> None:
    delete_client_cert(
        project=project,
        instance_name=instance_name,
        dry_run=dry_run,
    )


def run_cleanup(args: argparse.Namespace, project: str) -> int:
    """Delete migration job and connection profiles."""
    dest_instance = derive_dest_instance_name(
        args.source_instance, DEST_SUFFIX, args.new_instance_prefix
    )
    source_details = get_instance_details(project, args.source_instance)
    region = source_details["region"]

    job_name = f"{args.source_instance}{DMS_JOB_SUFFIX}"
    source_profile = f"{args.source_instance}{DMS_SOURCE_PROFILE_SUFFIX}"
    dest_profile = dest_instance

    print("\n" + "=" * 60)
    print("CLEANUP")
    print("=" * 60)
    print(f"  Migration job:        {job_name}")
    print(f"  Source profile:       {source_profile}")
    print(f"  Destination profile:  {dest_profile}")
    print(f"  Region:               {region}")
    if args.dry_run:
        print("  [DRY RUN MODE]")

    # Step 1: Delete migration job (async, then poll until gone)
    print("\n[Step 1] Deleting migration job...")
    _cleanup_delete_migration_job(
        project=project, region=region, job_name=job_name, dry_run=args.dry_run
    )

    # Step 2: Delete destination connection profile
    print("\n[Step 2] Deleting destination connection profile...")
    _cleanup_delete_connection_profile(
        project=project,
        region=region,
        profile_name=dest_profile,
        dry_run=args.dry_run,
    )

    # Step 3: Delete source connection profile
    print("\n[Step 3] Deleting source connection profile...")
    _cleanup_delete_connection_profile(
        project=project,
        region=region,
        profile_name=source_profile,
        dry_run=args.dry_run,
    )

    # Step 4: Delete DMS client certificate from source instance
    print("\n[Step 4] Deleting DMS client certificate from source instance...")
    _cleanup_delete_client_cert(
        project=project,
        instance_name=args.source_instance,
        dry_run=args.dry_run,
    )

    print("\n" + "=" * 60)
    if args.dry_run:
        print("DRY RUN COMPLETE — no changes were made.")
    else:
        print("CLEANUP COMPLETE")
    print("=" * 60)

    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    """Entry point for the migration script."""
    parser = build_parser()
    args = parser.parse_args()

    try:
        project = args.project

        if args.phase == "preflight":
            return run_preflight(args, project)
        if args.phase == "setup-migration":
            return run_setup_migration(args, project)
        if args.phase == "cutover":
            if not args.db_secret_prefix:
                parser.error("--db-secret-prefix is required for the cutover phase")
            return run_cutover(args, project)
        if args.phase == "cleanup":
            return run_cleanup(args, project)

        print(f"Unknown phase: {args.phase}", file=sys.stderr)
        return 1

    except RuntimeError as e:
        print(f"\nError: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nAborted by user.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
