# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""
Tool for removing entries from the UserOverride table where the data is equivalent to a row in the
Roster table. If a single column is equivalent, it sets it to null in the override. If all relevant
columns are equivalent, it removes the entry.

This is run automatically after roster sync, or it can be run manually via tools/auth/cleanup_user_overrides.py.
"""

import logging

from sqlalchemy import and_, delete, func, or_, select, text, update
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import BinaryExpression

from recidiviz.persistence.database.schema.case_triage.schema import (
    Roster,
    UserOverride,
)

equivalent_sorted_roles_columns = """
    array(
        SELECT unnest(roster.roles) ORDER BY 1
    ) = array(
        SELECT unnest(user_override.roles) ORDER BY 1
    )
"""

_COLUMN_TO_COMPARISON_CLAUSE: dict[str, BinaryExpression] = {
    "external_id": Roster.external_id == UserOverride.external_id,
    "district": Roster.district == UserOverride.district,
    "first_name": Roster.first_name.ilike(UserOverride.first_name),
    "last_name": Roster.last_name.ilike(UserOverride.last_name),
    "roles": or_(
        text(equivalent_sorted_roles_columns),
        and_(
            or_(
                Roster.roles.any("supervision_officer"),
                Roster.roles.any("supervision_officer_supervisor"),
            ),
            UserOverride.roles == {"supervision_staff"},
        ),
        and_(
            or_(
                Roster.roles.any("supervision_leadership"),
                Roster.roles.any("state_leadership"),
            ),
            UserOverride.roles == {"leadership_role"},
        ),
    ),
}


def _run_update_stmt(
    session: Session, dry_run: bool, state_code: str, key: str, clause: BinaryExpression
) -> None:
    where_clause = and_(
        Roster.email_address == UserOverride.email_address,
        Roster.state_code == state_code,
        clause,
    )
    if dry_run:
        results = session.execute(select(UserOverride).where(where_clause)).scalars()
        users_to_modify = [user.email_address for user in results]
        logging.info(
            "[DRY RUN] would set %s to null for %d rows: %s",
            key,
            len(users_to_modify),
            users_to_modify,
        )
    else:
        results = session.execute(
            update(UserOverride)
            .where(where_clause)
            .values(**{key: None})
            .execution_options(synchronize_session=False)
            .returning(UserOverride.email_address)
        ).all()
        updated_users = [user.email_address for user in results]
        logging.info(
            "set %s to null for %d rows: %s", key, len(updated_users), updated_users
        )


def cleanup_user_overrides(session: Session, dry_run: bool, state_code: str) -> None:
    """Clean up the overrides. First, update equivalent columns to null. Then, delete any entries
    where all relevant columns are null."""
    for column, comparison in _COLUMN_TO_COMPARISON_CLAUSE.items():
        _run_update_stmt(session, dry_run, state_code, column, comparison)

    if dry_run:
        # In dry run mode we need to check if the Roster/UserOverride columns have equivalent values
        # because we haven't nulled out the values. When running outside of dry run, we only need to
        # check if the column is null because we've already set equivalent values to null.
        similar_clauses = [
            or_(getattr(UserOverride, column).is_(None), clause)
            for column, clause in _COLUMN_TO_COMPARISON_CLAUSE.items()
        ]
        results = session.execute(
            select(UserOverride)
            .join(Roster, UserOverride.email_address == Roster.email_address)
            .where(
                and_(
                    *similar_clauses,
                    or_(
                        UserOverride.blocked_on.is_(None),
                        UserOverride.blocked_on > func.now(),
                    ),
                    Roster.state_code == state_code
                )
            )
            .order_by(UserOverride.email_address)
        ).scalars()
        users_to_delete = [user.email_address for user in results]
        logging.info(
            "[DRY RUN] would delete %d rows: %s", len(users_to_delete), users_to_delete
        )
    else:
        results = session.execute(
            delete(UserOverride)
            .where(
                and_(
                    *[
                        getattr(UserOverride, column).is_(None)
                        for column in _COLUMN_TO_COMPARISON_CLAUSE
                    ],
                    # Blocked users are kept track of in User Overrides, so a blocked user can still
                    # have all other attributes be null
                    or_(
                        UserOverride.blocked_on.is_(None),
                        UserOverride.blocked_on > func.now(),
                    ),
                    UserOverride.state_code == state_code
                )
            )
            .execution_options(synchronize_session=False)
            .returning(UserOverride.email_address)
        )
        deleted_users = [user.email_address for user in results]
        logging.info("deleted %d rows: %s", len(deleted_users), deleted_users)
