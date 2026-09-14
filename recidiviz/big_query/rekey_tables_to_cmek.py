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
"""Re-encrypts existing BigQuery tables with a CMEK key, safely.

Tables created before a project got its default CMEK key keep Google-managed
encryption forever: appends never change a table's key. This module rewrites
such a table in place (a server-side copy job, ~6 seconds regardless of size,
free) so it carries the CMEK key, with guards that make the rewrite safe to
run against live projects:

  - refuses tables with an active streaming buffer, tables with streaming
    writes in the last 30 days (per the streaming ledgers), and tables
    modified more recently than min_quiet_hours. A copy job reads only
    flushed columnar storage, so rows still in write-optimized storage would
    be silently dropped by the overwrite.
  - copies the table to a *_rekey_bak backup before every overwrite. Time
    travel is forbidden on tables that have ever had row access policies, so
    the backup is the only reliable undo.
  - fingerprints the full table before and after (row count only above
    checksum_max_gb, since the fingerprint scans the table at query pricing).
    A mismatch keeps the backup and stops the run.
  - skips tables that already have the key, so re-running resumes an
    interrupted sweep.

This is temporary migration code for the CJIS Assured Workloads move
(Recidiviz/zenhub-tasks#2606); delete it when every table carries the key.
It uses google.cloud.bigquery directly because BigQueryClientImpl has no
copy-with-destination-encryption operation and this module does not outlive
the migration.

Callers: recidiviz.tools.rekey_bq_tables_to_cmek (CLI sweep) and
RekeyTablesToCmekEntrypoint (temporary Airflow first-task).
"""
import datetime
import enum
import json
import logging
import re
from collections import Counter
from typing import IO, Iterable

import attr
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

BACKUP_SUFFIX = "_rekey_bak"
STREAM_LOOKBACK_DAYS = 30

# Identifiers are interpolated into SQL (BigQuery cannot bind table names as
# query parameters), so every identifier must match these before any query is
# built. Targets arrive from operator CLI flags and manifest files.
_PROJECT_ID_RE = re.compile(r"^[a-z][a-z0-9-]{4,61}[a-z0-9]$")
_DATASET_ID_RE = re.compile(r"^[A-Za-z0-9_]{1,1024}$")
_TABLE_ID_RE = re.compile(r"^[A-Za-z0-9_$-]{1,1024}$")
_BQ_REGION_RE = re.compile(r"^[a-z0-9-]{1,32}$")


def _validated(value: str, pattern: re.Pattern, what: str) -> str:
    if not pattern.match(value):
        raise ValueError(f"Invalid {what}: [{value}]")
    return value


class RekeyAction(enum.Enum):
    """Outcome of one table's re-key attempt. Every SKIP_* value is a guard
    refusing an unsafe or unnecessary rewrite."""

    SKIP_MISSING = "SKIP_MISSING"
    SKIP_NOT_BASE = "SKIP_NOT_BASE"
    SKIP_ALREADY_CMEK = "SKIP_ALREADY_CMEK"
    SKIP_OTHER_KEY = "SKIP_OTHER_KEY"
    SKIP_BUFFER = "SKIP_BUFFER"
    SKIP_STREAMED = "SKIP_STREAMED"
    SKIP_NOT_QUIET = "SKIP_NOT_QUIET"
    WOULD_REKEY = "WOULD_REKEY"
    REKEYED_VERIFIED = "REKEYED_VERIFIED"
    CRITICAL_MISMATCH = "CRITICAL_MISMATCH"
    FAILED = "FAILED"


@attr.define(kw_only=True)
class RekeyResult:
    dataset_id: str
    table_id: str
    action: RekeyAction
    detail: str = ""

    def to_line(self) -> str:
        return (
            f"{self.action.value:<22} {self.dataset_id}.{self.table_id}  "
            f"{self.detail}"
        )


def recently_streamed_tables(
    *, client: bigquery.Client, project_id: str, bq_region: str
) -> set[tuple[str, str]]:
    """Returns (dataset_id, table_id) pairs with streaming writes in the last
    STREAM_LOOKBACK_DAYS days, from both the legacy streaming and the Storage
    Write API ledgers."""
    _validated(project_id, _PROJECT_ID_RE, "project_id")
    _validated(bq_region, _BQ_REGION_RE, "bq_region")
    sql = f"""
    SELECT dataset_id, table_id
    FROM `{project_id}`.`region-{bq_region}`.INFORMATION_SCHEMA.STREAMING_TIMELINE_BY_PROJECT
    WHERE start_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {STREAM_LOOKBACK_DAYS} DAY)
    UNION DISTINCT
    SELECT dataset_id, table_id
    FROM `{project_id}`.`region-{bq_region}`.INFORMATION_SCHEMA.WRITE_API_TIMELINE_BY_PROJECT
    WHERE start_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {STREAM_LOOKBACK_DAYS} DAY)
    """
    return {(row[0], row[1]) for row in client.query(sql).result()}


def _checksum(*, client: bigquery.Client, table_ref: str, checksum_mode: str) -> str:
    """Returns an order-independent content fingerprint of the table, or only
    the row count when checksum_mode is "count" (COUNT(*) on a native table
    reads no data, while the fingerprint scans the whole table)."""
    if checksum_mode == "full":
        sql = f"""
        SELECT CONCAT(CAST(COUNT(*) AS STRING), '|',
               IFNULL(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING), 'EMPTY'))
        FROM `{table_ref}` t
        """
    else:
        sql = f"SELECT CAST(COUNT(*) AS STRING) FROM `{table_ref}`"
    return next(iter(client.query(sql).result()))[0]


def _current_kms_key(table: bigquery.Table) -> str | None:
    config = table.encryption_configuration
    return config.kms_key_name if config else None


def rekey_table(
    *,
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
    kms_key: str | None,
    apply: bool,
    min_quiet_hours: float,
    streamed_tables: set[tuple[str, str]],
    checksum_max_gb: float,
    keep_backups: bool,
) -> RekeyResult:
    """Re-encrypts one table with kms_key via an in-place copy job, or reports
    what would happen when apply is False. Every SKIP_* outcome is a guard
    refusing an unsafe or unnecessary rewrite; see the module docstring for
    why each guard exists."""
    _validated(project_id, _PROJECT_ID_RE, "project_id")
    _validated(dataset_id, _DATASET_ID_RE, "dataset_id")
    _validated(table_id, _TABLE_ID_RE, "table_id")
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    def result(action: RekeyAction, detail: str = "") -> RekeyResult:
        return RekeyResult(
            dataset_id=dataset_id, table_id=table_id, action=action, detail=detail
        )

    try:
        table = client.get_table(table_ref)
    except NotFound:
        return result(RekeyAction.SKIP_MISSING)
    if table.table_type != "TABLE":
        return result(RekeyAction.SKIP_NOT_BASE, table.table_type)

    current_key = _current_kms_key(table)
    if current_key:
        if kms_key and current_key.startswith(kms_key):
            return result(RekeyAction.SKIP_ALREADY_CMEK)
        return result(RekeyAction.SKIP_OTHER_KEY, f"has [{current_key}]")

    if table.streaming_buffer is not None:
        return result(RekeyAction.SKIP_BUFFER, "active streaming buffer")
    if (dataset_id, table_id) in streamed_tables:
        return result(
            RekeyAction.SKIP_STREAMED,
            f"streamed within {STREAM_LOOKBACK_DAYS}d; pause its writer first",
        )
    quiet = datetime.datetime.now(datetime.timezone.utc) - table.modified
    if quiet < datetime.timedelta(hours=min_quiet_hours):
        return result(
            RekeyAction.SKIP_NOT_QUIET,
            f"modified {quiet.total_seconds() / 3600:.1f}h ago "
            f"(< {min_quiet_hours}h)",
        )

    checksum_mode = (
        "full" if (table.num_bytes or 0) <= checksum_max_gb * 1e9 else "count"
    )

    if not apply:
        return result(
            RekeyAction.WOULD_REKEY,
            f"{table.num_rows} rows, {(table.num_bytes or 0) / 1e9:.2f} GB, "
            f"quiet {quiet.days}d, checksum={checksum_mode}",
        )

    if not kms_key:
        return result(RekeyAction.FAILED, "kms_key is required to apply")

    before = _checksum(client=client, table_ref=table_ref, checksum_mode=checksum_mode)

    backup_ref = f"{table_ref}{BACKUP_SUFFIX}"
    backup_config = bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE")
    client.copy_table(table_ref, backup_ref, job_config=backup_config).result()

    rekey_config = bigquery.CopyJobConfig(
        write_disposition="WRITE_TRUNCATE",
        destination_encryption_configuration=bigquery.EncryptionConfiguration(
            kms_key_name=kms_key
        ),
    )
    client.copy_table(table_ref, table_ref, job_config=rekey_config).result()

    new_key = _current_kms_key(client.get_table(table_ref)) or ""
    after = _checksum(client=client, table_ref=table_ref, checksum_mode=checksum_mode)
    if after != before or not new_key.startswith(kms_key):
        return result(
            RekeyAction.CRITICAL_MISMATCH,
            f"before=[{before}] after=[{after}] key=[{new_key}] "
            f"BACKUP KEPT: [{backup_ref}]",
        )

    if not keep_backups:
        client.delete_table(backup_ref, not_found_ok=True)
    return result(RekeyAction.REKEYED_VERIFIED, f"checksum {before} ({checksum_mode})")


def rekey_tables(
    *,
    client: bigquery.Client,
    project_id: str,
    tables: Iterable[tuple[str, str]],
    kms_key: str | None,
    apply: bool = False,
    min_quiet_hours: float = 24,
    bq_region: str = "us",
    checksum_max_gb: float = 100,
    keep_backups: bool = False,
    results_log: IO[str] | None = None,
) -> list[RekeyResult]:
    """Runs rekey_table over each (dataset_id, table_id) pair, stopping only
    on a content mismatch. One table's failure (a policy-tag 403, for
    example) records a FAILED result and the sweep continues; any backup
    already made for that table is kept."""
    streamed_tables = recently_streamed_tables(
        client=client, project_id=project_id, bq_region=bq_region
    )
    results = []
    for dataset_id, table_id in tables:
        try:
            table_result = rekey_table(
                client=client,
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id,
                kms_key=kms_key,
                apply=apply,
                min_quiet_hours=min_quiet_hours,
                streamed_tables=streamed_tables,
                checksum_max_gb=checksum_max_gb,
                keep_backups=keep_backups,
            )
        except Exception as e:  # pylint: disable=broad-except
            table_result = RekeyResult(
                dataset_id=dataset_id,
                table_id=table_id,
                action=RekeyAction.FAILED,
                detail=f"{type(e).__name__}: {e}"[:300],
            )
        results.append(table_result)
        logging.info("%s", table_result.to_line())
        if results_log:
            log_entry = attr.asdict(table_result)
            log_entry["action"] = table_result.action.value
            results_log.write(json.dumps(log_entry) + "\n")
            results_log.flush()
        if table_result.action == RekeyAction.CRITICAL_MISMATCH:
            logging.error(
                "HALTING: content mismatch on [%s.%s] — investigate before "
                "continuing.",
                dataset_id,
                table_id,
            )
            break
    summary = Counter(r.action.value for r in results)
    logging.info("SUMMARY: %s", json.dumps(dict(summary)))
    return results
