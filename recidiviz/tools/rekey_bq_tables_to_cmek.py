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
"""CLI for re-encrypting existing BigQuery tables with CMEK.

Part of the CJIS Assured Workloads migration (Recidiviz/zenhub-tasks#2606).
Dry run is the default; nothing changes without --apply. Run it under a PAM
grant (pam-deploy-app: needs bigquery.admin plus
datacatalog.categoryFineGrainedReader for policy-tagged tables).

Usage:
    # Dry run one table:
    uv run python -m recidiviz.tools.rekey_bq_tables_to_cmek \\
        --project-id recidiviz-staging --table some_dataset.some_table

    # Sweep a manifest (CSV with ds,tb columns), for real:
    uv run python -m recidiviz.tools.rekey_bq_tables_to_cmek \\
        --project-id recidiviz-staging \\
        --kms-key projects/.../cryptoKeys/recidiviz-staging-bq-default \\
        --manifest rekey_manifest.csv --apply

The sweep is idempotent: already-keyed tables are skipped, so re-running
resumes after any interruption (including PAM grant expiry).
"""
import argparse
import csv
import logging
import sys

from google.cloud import bigquery

from recidiviz.big_query.rekey_tables_to_cmek import (
    BACKUP_SUFFIX,
    RekeyAction,
    rekey_tables,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--kms-key", help="full CMEK key path; required with --apply")
    parser.add_argument("--apply", action="store_true", help="default is a dry run")
    parser.add_argument(
        "--table",
        action="append",
        default=[],
        help="dataset_id.table_id, repeatable",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        default=[],
        help="every base table in this dataset, repeatable",
    )
    parser.add_argument(
        "--manifest", help="CSV file with ds and tb columns, one table per row"
    )
    parser.add_argument("--min-quiet-hours", type=float, default=24)
    parser.add_argument("--bq-region", default="us")
    parser.add_argument("--checksum-max-gb", type=float, default=100)
    parser.add_argument("--keep-backups", action="store_true")
    parser.add_argument("--results-log", default="rekey_results.jsonl")
    return parser.parse_args()


def main() -> None:
    """Resolves targets from the CLI flags and runs the sweep."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _parse_args()
    if args.apply and not args.kms_key:
        raise ValueError("--kms-key is required with --apply")

    client = bigquery.Client(project=args.project_id)

    targets: list[tuple[str, str]] = []
    for spec in args.table:
        dataset_id, table_id = spec.split(".", 1)
        targets.append((dataset_id, table_id))
    for dataset_id in args.dataset:
        targets.extend(
            (dataset_id, t.table_id)
            for t in client.list_tables(dataset_id)
            if t.table_type == "TABLE" and not t.table_id.endswith(BACKUP_SUFFIX)
        )
    if args.manifest:
        with open(args.manifest, encoding="utf-8") as manifest_file:
            targets.extend(
                (row["ds"], row["tb"]) for row in csv.DictReader(manifest_file)
            )
    if not targets:
        raise ValueError("no targets: pass --table, --dataset, or --manifest")

    mode = "APPLY" if args.apply else "DRY RUN (no changes will be made)"
    logging.info(
        "mode: %s | project: %s | targets: %d", mode, args.project_id, len(targets)
    )
    with open(args.results_log, "a", encoding="utf-8") as results_log:
        results = rekey_tables(
            client=client,
            project_id=args.project_id,
            tables=targets,
            kms_key=args.kms_key,
            apply=args.apply,
            min_quiet_hours=args.min_quiet_hours,
            bq_region=args.bq_region,
            checksum_max_gb=args.checksum_max_gb,
            keep_backups=args.keep_backups,
            results_log=results_log,
        )
    if any(
        r.action in (RekeyAction.CRITICAL_MISMATCH, RekeyAction.FAILED) for r in results
    ):
        sys.exit(1)


if __name__ == "__main__":
    main()
