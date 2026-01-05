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
Tool for deleting product users who have been blocked for at least 30 days. Blocking can
occur either via the admin panel or during roster sync. During roster sync, if an existing
user is not included in the ingested file, their block date is set, their information is 
copied to the UserOverride table, and they are removed from the Roster table. If they 
are still not in the Roster table 30 days after they are blocked, we can assume that they 
should no longer have access and can be deleted. Users are deleted from both UserOverride
and PermissionsOverride to prevent accidental reassignment of previous permissions if the user
is ever re-added.

The script can be run against a local database or against one running in Cloud SQL. The
script will prompt for confirmation with a list of affected users before deleting.

Usage against default development database (docker-compose v1) after `docker-compose up` has been
run: docker exec -it pulse-data_admin_panel_backend_1 uv run python -m
recidiviz.tools.auth.delete_blocked_users --state_code US_XX

Usage against default development database (docker-compose v2) after `docker-compose up` has been
run: docker exec -it pulse-data-admin_panel_backend-1 uv run python -m
recidiviz.tools.auth.delete_blocked_users --state_code US_XX

To run against Cloud SQL, specify the project id: python -m
recidiviz.tools.auth.delete_blocked_users --project_id recidiviz-staging --state_code US_XX

You can also optionally include a comma-separated list of email addresses for the specific users you would like
to delete: docker exec -it pulse-data-admin_panel_backend-1 uv run python -m
recidiviz.tools.auth.delete_blocked_users --state_code US_XX --emails user1@recidiviz.org,user2@recidiviz.org
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from typing import List, Optional, Tuple, Type

from dateutil.tz import tzlocal
from sqlalchemy import and_, delete, select
from sqlalchemy.orm import Session

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.case_triage.schema import (
    PermissionsOverride,
    Roster,
    UserOverride,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import (
    GCP_PROJECT_PRODUCTION,
    GCP_PROJECT_STAGING,
    in_development,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_list


def format_user_strings(users: List[Tuple[str, datetime]]) -> str:
    indentation = " " * 4
    user_strings = "\n".join(
        [f"{users[0][0]} (blocked on {users[0][1].strftime('%Y-%m-%d')})"]
        + [
            f"{indentation}{email} (blocked on {date.strftime('%Y-%m-%d')})"
            for email, date in users[1:]
        ]
    )
    return user_strings


def delete_users_from_table(
    session: Session,
    table: Type[UserOverride] | Type[PermissionsOverride],
    users: List[Tuple[str, datetime]],
) -> None:
    user_emails = [email_address for email_address, _ in users]
    results = session.execute(
        delete(table)
        .where(table.email_address.in_(user_emails))
        .execution_options(synchronize_session=False)
        .returning(table.email_address)
    )
    deleted_users = [user.email_address for user in results]
    logging.info(
        "Deleted %d rows from %s: %s",
        len(deleted_users),
        table.__tablename__,
        deleted_users,
    )


def get_eligible_users(
    session: Session, state_code: str, user_emails: Optional[List[str]] = None
) -> List[Tuple[str, datetime]]:
    thirty_days_ago = datetime.now(tzlocal()) - timedelta(days=30)

    conditions = [
        Roster.email_address.is_(None),
        UserOverride.state_code == state_code,
        UserOverride.blocked_on <= thirty_days_ago,
    ]

    if user_emails:
        conditions.append(UserOverride.email_address.in_(user_emails))

    results = session.execute(
        select(UserOverride)
        .outerjoin(Roster, UserOverride.email_address == Roster.email_address)
        .where(and_(*conditions))
        .order_by(UserOverride.email_address)
    ).scalars()
    return [(user.email_address, user.blocked_on) for user in results]


def delete_blocked_users(
    session: Session, state_code: str, user_emails: Optional[List[str]] = None
) -> None:
    """Delete from UserOverride and PermissionsOverride any users that have been blocked
    for 30+ days and don't exist in Roster"""
    users_to_delete = get_eligible_users(session, state_code, user_emails)

    if not users_to_delete:
        logging.info("No users found to delete, exiting")
        return

    confirmation_prompt = f"""
    Are you sure you want to delete the following {len(users_to_delete)} users?
    {format_user_strings(users_to_delete)}
    """

    if not prompt_for_confirmation(confirmation_prompt):
        return

    delete_users_from_table(session, UserOverride, users_to_delete)
    delete_users_from_table(session, PermissionsOverride, users_to_delete)


def parse_arguments(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    """Parses the arguments needed to call the delete_blocked_users function."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=False,
    )

    parser.add_argument(
        "--state_code",
        type=StateCode,
        choices=list(StateCode),
        required=True,
    )

    parser.add_argument(
        "--emails",
        type=str_to_list,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    db_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
    if known_args.project_id:
        with local_project_id_override(
            known_args.project_id
        ), cloudsql_proxy_control.connection(
            schema_type=SchemaType.CASE_TRIAGE,
        ), SessionFactory.for_proxy(
            db_key
        ) as global_session:
            delete_blocked_users(
                global_session, known_args.state_code.value, known_args.emails
            )
    else:
        if not in_development():
            raise RuntimeError(
                "Expected to be called inside a docker container or with the --project_id argument. See usage in docstring"
            )
        SQLAlchemyEngineManager.init_engine(db_key)
        with SessionFactory.using_database(db_key) as global_session:
            delete_blocked_users(
                global_session, known_args.state_code.value, known_args.emails
            )
