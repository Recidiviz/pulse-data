# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""A script to prepare a state's roster data for roster sync.

To run this script, first add the state code this will be running with to the list of
`state_staff_states` in the `ingested_product_users` BQ view, and load that view into a sandbox in
the project this script will be run with.

Next, run the script in dry-run mode to verify the changes it's making. For example:
    python -m recidiviz.tools.auth.prep_roster_sync --project_id recidiviz-staging --state_code US_CA --sandbox_prefix dana_roster --dry_run

If you are satisfied with the output, run it again without the --dry_run flag to make the changes
proposed by the script. The script will prompt for confirmation just in case.
"""

import argparse
import itertools
import logging
import sys
from datetime import UTC, datetime

from dateutil.relativedelta import relativedelta
from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from recidiviz.auth.auth0_client import Auth0Client
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.case_triage.schema import (
    Roster,
    UserOverride,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_RECENTLY_LOGGED_IN_TIMEDELTA = relativedelta(years=1)
_RECENTLY_ADDED_TO_ROSTER_TIMEDETLA = relativedelta(months=3)

_EXISTING_USER_QUERY = select(
    func.coalesce(UserOverride.state_code, Roster.state_code).label("state_code"),
    func.coalesce(UserOverride.email_address, Roster.email_address).label(
        "email_address"
    ),
    func.coalesce(UserOverride.external_id, Roster.external_id).label("external_id"),
    func.coalesce(UserOverride.role, Roster.role).label("role"),
    func.coalesce(UserOverride.roles, Roster.roles).label("roles"),
    func.coalesce(UserOverride.district, Roster.district).label("district"),
    func.coalesce(UserOverride.first_name, Roster.first_name).label("first_name"),
    func.coalesce(UserOverride.last_name, Roster.last_name).label("last_name"),
    UserOverride.blocked,
    func.coalesce(
        UserOverride.user_hash,
        Roster.user_hash,
    ).label("user_hash"),
    func.coalesce(
        UserOverride.pseudonymized_id,
        Roster.pseudonymized_id,
    ).label("pseudonymized_id"),
).join_from(
    Roster,
    UserOverride,
    UserOverride.email_address == Roster.email_address,
    full=True,
)


def get_roster_sync_output(
    bq_client: BigQueryClient, project_id: str, state_code: str, sandbox_prefix: str
) -> list[Roster]:
    roster_sync_query = f"""
    SELECT * FROM `{project_id}.{sandbox_prefix}_reference_views.ingested_product_users_materialized`
    WHERE state_code="{state_code}"
    """
    query_results = bq_client.run_query_async(
        query_str=roster_sync_query, use_query_cache=True
    )
    return [Roster(**row) for row in query_results]


def get_recently_logged_in_users_by_email(
    auth0_client: Auth0Client, emails: list[str]
) -> list[str]:
    auth0_users = auth0_client.get_all_users_by_email_addresses(emails)
    return [
        user["email"].lower()
        for user in auth0_users
        if datetime.fromisoformat(user["last_login"])
        >= datetime.now(tz=UTC) - _RECENTLY_LOGGED_IN_TIMEDELTA
    ]


def get_existing_users_missing_from_roster_sync(
    session: Session,
    auth0_client: Auth0Client,
    roster_sync_users: list[Roster],
    state_code: str,
) -> tuple[list[dict], set[str]]:
    """If a user has logged in within the last year but is not in the roster sync query, add
    them to UserOverride with their current Roster+UserOverride merged data so that they still
    appear in the admin panel once roster sync is turned on (since they won't appear in the Roster
    table then)."""
    roster_users = (
        session.execute(select(Roster).filter(Roster.state_code == state_code))
        .scalars()
        .all()
    )
    user_override_users = (
        session.execute(
            select(UserOverride).filter(UserOverride.state_code == state_code)
        )
        .scalars()
        .all()
    )

    # Look for existing roster / user_override users who won't be synced
    existing_user_emails = set(
        user.email_address.lower()
        for user in itertools.chain(roster_users, user_override_users)
    )
    roster_sync_user_emails = set(
        user.email_address.lower() for user in roster_sync_users
    )
    existing_emails_missing_from_roster_sync = (
        existing_user_emails - roster_sync_user_emails
    )

    # Of the missing users, find which ones have logged in in the last year so we can make sure they
    # don't get accidentally removed
    missing_users_who_logged_in_in_last_year = set(
        get_recently_logged_in_users_by_email(
            auth0_client, list(existing_emails_missing_from_roster_sync)
        )
    )

    # Also keep ones who were added to the roster in the last 3 months in case they just haven't had
    # an opportunity to log in yet. Since non-roster-sync users are only added to UserOverride now,
    # we don't need to query Roster for these users, and we also don't need to add them back to
    # UserOverride (since they're already there)
    existing_recently_added_users = {
        user.email_address.lower()
        for user in user_override_users
        if user.created_datetime >= datetime.now() - _RECENTLY_ADDED_TO_ROSTER_TIMEDETLA
    }

    # TODO(#25566): Add logic to keep TN D20 users because we're going to start out not syncing them

    # query the merged roster/user override table results for the users we want to keep, and return
    # them as a list of UserOverrides to add back in.
    users_to_add_to_user_override_results = session.execute(
        _EXISTING_USER_QUERY.filter(
            func.coalesce(UserOverride.email_address, Roster.email_address).in_(
                missing_users_who_logged_in_in_last_year
            )
        )
    ).all()

    # Keep users who logged in in the last year and ones who were added to our user mgmt recently,
    # and delete the rest of the users who don't show up in roster sync.
    users_who_will_be_deleted = (
        existing_emails_missing_from_roster_sync
        - missing_users_who_logged_in_in_last_year
        - existing_recently_added_users
    )

    return (
        # Transform to a dict instead of a UserOverride object because we need to use the
        # postgres-specific insert API to allow upsert, and that takes a dict, and it's easier to
        # get a dict out of a row than out of a UserOverride.
        [row._asdict() for row in users_to_add_to_user_override_results],
        users_who_will_be_deleted,
    )


def remove_users(session: Session, user_emails_to_delete: set[str]) -> None:
    session.execute(
        delete(UserOverride).where(
            UserOverride.email_address.in_(user_emails_to_delete)
        )
    )
    session.execute(
        delete(Roster).where(Roster.email_address.in_(user_emails_to_delete))
    )
    session.commit()


def add_user_overrides(session: Session, user_overrides: list[dict]) -> None:
    for user_override in user_overrides:
        session.execute(
            insert(UserOverride)
            .values(user_override)
            .on_conflict_do_update(index_elements=["email_address"], set_=user_override)
        )
    session.commit()


def prepare_for_roster_sync(
    session: Session,
    dry_run: bool,
    project_id: str,
    state_code: str,
    sandbox_prefix: str,
    bq_client: BigQueryClient,
    auth0_client: Auth0Client,
) -> None:
    """Prepare a state for roster sync by handling diffs between what roster sync would produce for
    that state and what's currently in the Roster/UserOverride tables.

    If a user is missing from the roster sync output and we think they should still be in the roster
    (because they've logged in recently or were recently added), add a UserOverride for them so we
    don't delete them.

    TODO(#32585): Handle role differences to make sure we preserve users' current levels of access
    TODO(#32585): Handle diffs for non-role fields so overrides are updated to match the roster sync values
    """

    # Read in what the output of roster sync will be for a state.
    roster_sync_users = get_roster_sync_output(
        bq_client, project_id, state_code, sandbox_prefix
    )

    # Handle users who are missing from the roster sync output but should remain in the roster
    (
        users_missing_from_roster_sync_who_should_remain,
        users_who_will_be_deleted,
    ) = get_existing_users_missing_from_roster_sync(
        session, auth0_client, roster_sync_users, state_code
    )

    logging.info(
        """\n\n✂️ Deleting the following users:\n%s
        \n\n🛟 Adding missing users to UserOverride:\n%s
        """,
        "\n".join(users_who_will_be_deleted),
        "\n".join(
            [str(user) for user in users_missing_from_roster_sync_who_should_remain]
        ),
    )
    if dry_run:
        return

    if not prompt_for_confirmation("Proceed?"):
        return

    remove_users(session, users_who_will_be_deleted)
    add_user_overrides(session, users_missing_from_roster_sync_who_should_remain)


def parse_arguments(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    """Parses the arguments needed to call the cleanup_user_overrides function."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )

    parser.add_argument(
        "--state_code",
        type=StateCode,
        choices=list(StateCode),
        required=True,
    )

    parser.add_argument(
        "--sandbox_prefix",
        type=str,
        required=True,
    )

    parser.add_argument("--dry_run", dest="dry_run", action="store_true")

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    db_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
    with local_project_id_override(
        known_args.project_id
    ), cloudsql_proxy_control.connection(
        schema_type=SchemaType.CASE_TRIAGE,
    ), SessionFactory.for_proxy(
        db_key
    ) as global_session:
        prepare_for_roster_sync(
            session=global_session,
            dry_run=known_args.dry_run,
            project_id=known_args.project_id,
            state_code=known_args.state_code.value,
            sandbox_prefix=known_args.sandbox_prefix,
            bq_client=BigQueryClientImpl(),
            auth0_client=Auth0Client(),
        )
