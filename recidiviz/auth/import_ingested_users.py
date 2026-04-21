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
"""Core helpers for importing ingested product users into Roster / UserOverride.

This module holds the upsert and post-processing logic that is invoked by the
``import_ingested_users`` Flask endpoint. It is factored out so that the same
logic can be exercised from offline tools (e.g. dry-run scripts) without going
through the HTTP layer.
"""
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Optional, Type

import pandas as pd
from dateutil.tz import tzlocal
from sqlalchemy import delete, func, select, update
from sqlalchemy.orm import Session

from recidiviz.auth.cleanup_user_overrides import cleanup_user_overrides
from recidiviz.auth.helpers import (
    convert_user_object_to_dict,
    generate_pseudonymized_id,
    generate_user_hash,
    validate_roles,
)
from recidiviz.calculator.query.state.views.reference.ingested_supervision_product_users import (
    INGESTED_SUPERVISION_PRODUCT_USERS_VIEW_BUILDER,
)
from recidiviz.cloud_storage.gcsfs_csv_reader_delegates import (
    SimpleGcsfsCsvReaderDelegate,
)
from recidiviz.persistence.database.schema.case_triage.schema import (
    Roster,
    StateRolePermissions,
    UserOverride,
)
from recidiviz.reporting.email_reporting_utils import validate_email_address


class ImportIngestedUsersGcsfsCsvReaderDelegate(SimpleGcsfsCsvReaderDelegate):
    """Helper class to upsert chunks of data read from GCS to the Roster table."""

    def __init__(
        self, session: Session, state_code: str, columns: Optional[list[str]]
    ) -> None:
        self.emails: list[str] = []
        self.session = session
        self.state_code = state_code
        self.columns = columns if columns else []

    def on_dataframe(self, encoding: str, chunk_num: int, df: pd.DataFrame) -> bool:
        df.columns = self.columns
        # df.to_dict exports missing values as 'nan', so export to json instead.
        rows = json.loads(df.to_json(orient="records"))
        _upsert_user_rows(self.session, self.state_code, rows, Roster, self.columns)
        self.emails.extend(r["email_address"] for r in rows)
        return True


def _upsert(
    session: Session,
    row: dict[str, Any],
    table: Type[Roster] | Type[UserOverride],
    update_null_only: bool = False,
) -> None:
    existing = (
        session.query(table)
        .filter_by(
            state_code=f"{row['state_code']}", email_address=f"{row['email_address']}"
        )
        .first()
    )
    if existing:
        for key, value in row.items():
            if not update_null_only or getattr(existing, key) is None:
                setattr(existing, key, value)
    else:
        new_row = table(**row)
        session.add(new_row)


def _upsert_user_rows(
    session: Session,
    state_code: str,
    rows: list[dict[str, Any]],
    table: Type[Roster] | Type[UserOverride],
    columns: list[str],
) -> None:
    """Upserts rows into a table that stores user attributes (Roster or UserOverride),
    along with some validation."""
    for row in rows:
        # TODO(#53580): Remove this once supervision_product_users columns match incarceration_and_supervision_product_users columns
        supervision_col_names = [
            c.name for c in INGESTED_SUPERVISION_PRODUCT_USERS_VIEW_BUILDER.columns
        ]
        if columns != supervision_col_names:
            row["external_id"] = row.get("staff_external_id") or row.get("external_id")
            row["district"] = row.get("district_id") or row.get("district")
            columns_to_delete = set(columns) - set(supervision_col_names)
            for column in columns_to_delete:
                del row[column]

        # For UserOverride (admin panel CSV uploads): if external_id is
        # None/empty, remove it from the row so _upsert() doesn't overwrite
        # an existing value with NULL. This prevents orphaned pseudonymized_ids
        # (where external_id is cleared but pseudonymized_id remains, causing
        # lookup mismatches). For Roster (ingestion), we allow clearing values.
        if table == UserOverride and row.get("external_id") is None:
            row.pop("external_id", None)

        if not row["email_address"]:
            raise ValueError(
                "Roster contains a row that is missing an email address (required)"
            )
        validate_email_address(row["email_address"])

        email = row["email_address"].lower()

        roles = [value.strip().lower() for value in row["roles"].split(",")]
        row["roles"] = roles
        validate_roles(row)

        # Experiments create arbitrary roles, and we don't want to throw an error if they aren't all configured
        non_experiment_roles = [
            role for role in roles if not role.startswith("experiment-")
        ]

        associated_state_roles = (
            session.query(StateRolePermissions.role)
            .filter_by(state_code=f"{state_code.upper()}")
            .where(StateRolePermissions.role.in_(non_experiment_roles))
            .all()
        )
        if len(associated_state_roles) != len(non_experiment_roles):
            raise ValueError(
                f"Roster contains a row that with a role that does not exist in the default state role permissions. Offending row has email {email}"
            )

        # Enforce casing for columns where we have a preference.
        row["state_code"] = state_code.upper()
        row["email_address"] = email

        if row.get("external_id") is not None:
            row["external_id"] = row["external_id"].upper()
            if row.get("pseudonymized_id") is None:
                row["pseudonymized_id"] = generate_pseudonymized_id(
                    row["state_code"], row["external_id"]
                )
        row["roles"] = roles
        row["user_hash"] = generate_user_hash(row["email_address"])
        # update existing row or add new row
        _upsert(session, row, table)

    session.commit()


# Roles whose holders should be removed from Roster (when not present in the
# ingested CSV) without being blocked, because they are still active users.
_NOT_LINE_STAFF_ROLES = [
    "facilities_leadership",
    "supervision_leadership",
    "supervision_regional_leadership",
    "state_leadership",
    "facilities_manager",
    "facilities_segregation_staff",
    "facilities_non_primary_staff",
]

# Roles that, when ``supervision_only`` is True, should also be carved out of
# the block-on-removal flow because they aren't supervision staff.
_FACILITIES_ROLES = [
    "facilities_line_staff",
    "facilities_non_primary_staff",
]


def _log_dry_run(message: str, *args: Any) -> None:
    logging.info("[DRY RUN] " + message, *args)


def _diff_user_override_upsert(
    session: Session, row: dict[str, Any]
) -> tuple[str, dict[str, Any]]:
    """Returns ("create" | "update" | "noop", changes_dict) describing what an
    ``_upsert(..., UserOverride, update_null_only=True)`` call with ``row``
    would do against the current session state."""
    existing = (
        session.query(UserOverride)
        .filter_by(state_code=row["state_code"], email_address=row["email_address"])
        .first()
    )
    if existing is None:
        return ("create", dict(row))
    changes: dict[str, Any] = {}
    for key, value in row.items():
        if getattr(existing, key) is None and value is not None:
            changes[key] = value
    return ("update" if changes else "noop", changes)


def process_ingested_users_post_upsert(
    session: Session,
    state_code: str,
    ingested_emails: list[str],
    supervision_only: bool,
    dry_run: bool,
) -> None:
    """Run the post-CSV-upsert reconciliation that lives at the tail of the
    ``import_ingested_users`` flow.

    The CSV upsert phase is expected to have already populated ``Roster`` for
    every email in ``ingested_emails`` before this function is called. This
    function then:

      1. Clears any future ``blocked_on`` on UserOverride rows for ingested
         users.
      2. Finds Roster rows for the state that are NOT in the CSV, copies them
         into UserOverride (with a 1-week future block, except for users with
         leadership / facilities-carve-out roles), and deletes them from
         Roster.
      3. Runs ``cleanup_user_overrides`` to drop UserOverride fields that are
         now redundant with Roster.

    When ``dry_run`` is True, no mutations are issued. Instead, each would-be
    change is printed and ``cleanup_user_overrides`` is invoked with
    ``dry_run=True`` so its output prints alongside.
    """
    # Step 1: unblock any upcoming blocks for ingested users.
    if dry_run:
        unblock_emails = (
            session.execute(
                select(UserOverride.email_address).where(
                    UserOverride.state_code == state_code,
                    UserOverride.email_address.in_(ingested_emails),
                    UserOverride.blocked_on.isnot(None),
                    UserOverride.blocked_on > func.now(),
                )
            )
            .scalars()
            .all()
        )
        _log_dry_run(
            "would clear future blocked_on for %d UserOverride row(s): %s",
            len(unblock_emails),
            sorted(unblock_emails),
        )
    else:
        session.execute(
            update(UserOverride)
            .where(
                UserOverride.state_code == state_code,
                UserOverride.email_address.in_(ingested_emails),
                UserOverride.blocked_on.isnot(None),
                UserOverride.blocked_on > func.now(),
            )
            .values(blocked_on=None),
            execution_options={"synchronize_session": False},
        )

    # Step 2: find Roster rows for this state that aren't in the CSV.
    roster_user_deletion_criteria = [
        Roster.state_code == state_code,
        Roster.email_address.not_in(ingested_emails),
    ]
    roster_users_to_delete = (
        session.execute(select(Roster).where(*roster_user_deletion_criteria))
        .scalars()
        .all()
    )

    # Step 3: figure out which of those should NOT receive a future block.
    if supervision_only:
        facilities_users = (
            session.execute(
                select(UserOverride.email_address).where(
                    UserOverride.email_address.in_(
                        [user.email_address for user in roster_users_to_delete]
                    ),
                    UserOverride.roles.overlap(_FACILITIES_ROLES),
                )
            )
            .scalars()
            .all()
        )
    else:
        facilities_users = []

    not_line_staff_users = (
        session.execute(
            select(UserOverride.email_address).where(
                UserOverride.email_address.in_(
                    [user.email_address for user in roster_users_to_delete]
                ),
                UserOverride.roles.overlap(_NOT_LINE_STAFF_ROLES),
            )
        )
        .scalars()
        .all()
    )

    # Step 4: build the per-user dict that will be upserted into UserOverride.
    roster_users_as_dicts = [
        {
            **convert_user_object_to_dict(user),
            "blocked_on": datetime.now(tzlocal()) + timedelta(weeks=1)
            if user.email_address not in not_line_staff_users + facilities_users
            else None,
        }
        for user in roster_users_to_delete
    ]

    if dry_run:
        creates: list[dict[str, Any]] = []
        updates: list[tuple[str, dict[str, Any]]] = []
        noops: list[str] = []
        for user in roster_users_as_dicts:
            action, changes = _diff_user_override_upsert(session, user)
            if action == "create":
                creates.append(changes)
            elif action == "update":
                updates.append((user["email_address"], changes))
            else:
                noops.append(user["email_address"])

        _log_dry_run(
            "would create %d UserOverride row(s) (carrying Roster data forward):",
            len(creates),
        )
        for row in creates:
            _log_dry_run("  + %s -> %s", row["email_address"], row)
        _log_dry_run(
            "would update %d existing UserOverride row(s) (filling NULL fields only):",
            len(updates),
        )
        for email, changes in updates:
            _log_dry_run("  ~ %s -> %s", email, changes)
        if noops:
            _log_dry_run(
                "would leave %d existing UserOverride row(s) unchanged: %s",
                len(noops),
                sorted(noops),
            )

        _log_dry_run(
            "would delete %d Roster row(s): %s",
            len(roster_users_to_delete),
            sorted(user.email_address for user in roster_users_to_delete),
        )
    else:
        for user in roster_users_as_dicts:
            _upsert(session, user, UserOverride, update_null_only=True)

        session.execute(
            delete(Roster).where(*roster_user_deletion_criteria),
            execution_options={"synchronize_session": False},
        )

        session.commit()

    # Step 5: clean up duplicative UserOverride data. Users with upcoming
    # blocks set above are unaffected because they have been removed from
    # Roster.
    cleanup_user_overrides(session=session, dry_run=dry_run, state_code=state_code)
