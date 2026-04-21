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
"""Dry-run version of the `/auth/import_ingested_users` endpoint for the
ingested_incarceration_and_supervision_product_users CSV.

This tool reads an `ingested_incarceration_and_supervision_product_users.csv`
file from GCS and reports every change that the production import flow would
make to the `roster` and `user_override` tables, without committing
anything.

It is intended to be used to vet the new
`ingested_incarceration_and_supervision_product_users.csv` import flow before
enabling it in production (currently only the supervision-only file is
imported).

Usage (against staging):
    python -m recidiviz.tools.auth.dry_run_import_ingested_users \
        --project_id recidiviz-staging \
        --state_code US_XX \
        --gcs_path gs://recidiviz-staging-product-user-import/US_XX/ingested_incarceration_and_supervision_product_users.csv

Notes:
- This script never commits. It opens a session in non-autocommit mode,
  monkey-patches `session.commit` to `session.flush` so the in-flight
  upserts run for real *within the transaction* (which is necessary for the
  cleanup-overrides step at the end to see the post-CSV Roster state), then
  rolls back at exit. Nothing is persisted.
- The script does not require the new file to already be enabled in
  production.
"""
import argparse
import logging
import sys
from typing import Any

from sqlalchemy.orm import Session

from recidiviz.auth.helpers import convert_user_object_to_dict
from recidiviz.auth.import_ingested_users import (
    ImportIngestedUsersGcsfsCsvReaderDelegate,
    process_ingested_users_post_upsert,
)
from recidiviz.calculator.query.state.views.reference.ingested_incarceration_and_supervision_product_users import (
    INGESTED_INCARCERATION_AND_SUPERVISION_PRODUCT_USERS_VIEW_BUILDER,
)
from recidiviz.cloud_storage.gcsfs_csv_reader import GcsfsCsvReader
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.case_triage.schema import (
    Roster,
    UserOverride,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def _snapshot_table(
    session: Session,
    table: type[Roster] | type[UserOverride],
    state_code: str,
) -> dict[str, dict[str, Any]]:
    """Snapshots a Roster or UserOverride table for ``state_code``, keyed by
    email and with each value being the dict produced by
    ``convert_user_object_to_dict`` (i.e. all columns except the
    auto-managed created/updated timestamps)."""
    rows = session.query(table).filter(table.state_code == state_code).all()
    return {row.email_address: convert_user_object_to_dict(row) for row in rows}


def _print_diff(
    label: str,
    before: dict[str, dict[str, Any]],
    after: dict[str, dict[str, Any]],
) -> None:
    """Compare two {email -> column-dict} snapshots and log the differences."""
    before_emails = set(before.keys())
    after_emails = set(after.keys())

    created = sorted(after_emails - before_emails)
    deleted = sorted(before_emails - after_emails)
    common = before_emails & after_emails

    # email -> {column_name -> "old_repr -> new_repr"} for changed columns only.
    updated: dict[str, dict[str, str]] = {}
    for email in sorted(common):
        changes = {
            col: f"{before[email].get(col)!r} -> {after_val!r}"
            for col, after_val in after[email].items()
            if before[email].get(col) != after_val
        }
        if changes:
            updated[email] = changes

    logging.info(
        "=== %s diff: %d created, %d updated, %d deleted ===",
        label,
        len(created),
        len(updated),
        len(deleted),
    )
    for email in created:
        logging.info("  CREATE %s: %s", email, after[email])
    for email, changes in updated.items():
        logging.info("  UPDATE %s:", email)
        for col, change in changes.items():
            logging.info("      %s: %s", col, change)
    for email in deleted:
        logging.info("  DELETE %s: %s", email, before[email])


def _run_dry_run(session: Session, state_code: str, gcs_path: str) -> None:
    """Runs the import_ingested_users flow for the CSV at ``gcs_path`` and logs
    every change that would be made to ``Roster`` and ``UserOverride``, then
    rolls back the session so nothing is persisted."""
    state_code_upper = state_code.upper()
    columns = INGESTED_INCARCERATION_AND_SUPERVISION_PRODUCT_USERS_VIEW_BUILDER.columns
    csv_path = GcsfsFilePath.from_absolute_path(gcs_path)

    logging.info("Snapshotting Roster + UserOverride for %s...", state_code_upper)
    roster_before = _snapshot_table(session, Roster, state_code_upper)
    overrides_before = _snapshot_table(session, UserOverride, state_code_upper)
    logging.info(
        "Snapshot: %d Roster row(s), %d UserOverride row(s)",
        len(roster_before),
        len(overrides_before),
    )

    # Monkey-patch session.commit -> session.flush so the upsert phase mutates
    # the session (and is visible to subsequent in-transaction queries) without
    # actually persisting. Restored after the upsert phase.
    original_commit = session.commit
    session.commit = session.flush  # type: ignore[method-assign]
    try:
        logging.info("Reading CSV %s...", csv_path.uri())
        reader_delegate = ImportIngestedUsersGcsfsCsvReaderDelegate(
            session=session,
            state_code=state_code_upper,
            columns=[c.name for c in columns],
        )
        csv_reader = GcsfsCsvReader(fs=GcsfsFactory.build())
        csv_reader.streaming_read(
            path=csv_path,
            delegate=reader_delegate,
            chunk_size=1000,
            header=None,
        )
        # Flush so the snapshot reflects everything.
        session.flush()
        ingested_emails = reader_delegate.emails

        roster_after_upsert = _snapshot_table(session, Roster, state_code_upper)
        overrides_after_upsert = _snapshot_table(
            session, UserOverride, state_code_upper
        )

        logging.info("")
        logging.info("##### CSV upsert phase changes #####")
        _print_diff("Roster (CSV upsert)", roster_before, roster_after_upsert)
        _print_diff(
            "UserOverride (CSV upsert)", overrides_before, overrides_after_upsert
        )

        logging.info("")
        logging.info("##### Post-upsert reconciliation (dry_run=True) #####")
        process_ingested_users_post_upsert(
            session=session,
            state_code=state_code_upper,
            ingested_emails=ingested_emails,
            supervision_only=False,
            dry_run=True,
        )
    finally:
        session.commit = original_commit  # type: ignore[method-assign]
        logging.info("Rolling back all changes (dry run).")
        session.rollback()


def parse_arguments(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project_id",
        required=True,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="GCP project hosting the Cloud SQL instance to dry-run against.",
    )
    parser.add_argument(
        "--state_code",
        required=True,
        type=StateCode,
        choices=list(StateCode),
        help="State the CSV is for.",
    )
    parser.add_argument(
        "--gcs_path",
        required=True,
        type=str,
        help=(
            "GCS path (gs://bucket/path/to/...) to the "
            "ingested_incarceration_and_supervision_product_users.csv to dry-run."
        ),
    )
    return parser.parse_known_args(argv)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    known_args, _ = parse_arguments(sys.argv[1:])

    db_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
    with local_project_id_override(
        known_args.project_id
    ), cloudsql_proxy_control.connection(
        schema_type=SchemaType.CASE_TRIAGE,
    ), SessionFactory.for_proxy(
        db_key, autocommit=False
    ) as session:
        _run_dry_run(
            session=session,
            state_code=known_args.state_code.value,
            gcs_path=known_args.gcs_path,
        )


if __name__ == "__main__":
    main()
