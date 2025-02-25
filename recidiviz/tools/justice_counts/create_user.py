# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
Creates a new UserAccount in our DB for given email address.

python -m recidiviz.tools.justice_counts.create_user \
  --email=EMAIL_ADDRESS \
  --project-id=PROJECT_ID \
  --dry-run=true
"""

import argparse
import logging

from recidiviz.justice_counts.control_panel.routes.admin import _get_auth0_client
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.environment import (
    GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
    GCP_PROJECT_JUSTICE_COUNTS_STAGING,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-id",
        choices=[
            GCP_PROJECT_JUSTICE_COUNTS_STAGING,
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
        ],
        help="Used to select which GCP project in which to run this script.",
        required=True,
    )
    parser.add_argument(
        "--email",
        type=str,
        help="Email address of the user to create.",
        required=True,
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=False)
    return parser


def create_user_account(dry_run: bool, email: str) -> None:
    """
    Creates a new UserAccount in our DB for given email address.
    NOTE: There must already be an Auth0 account for this email address.
    """
    schema_type = SchemaType.JUSTICE_COUNTS
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)

    auth0_client = _get_auth0_client()
    matching_users = auth0_client.get_all_users_by_email_addresses(
        email_addresses=[email]
    )
    if len(matching_users) != 1:
        raise ValueError(f"{len(matching_users)} found in Auth0.")

    matching_user = matching_users[0]
    name = matching_user.get("name")
    auth0_user_id = matching_user["user_id"]

    logging.info(
        "Found matching user in Auth0 with auth0_user_id=%s and name=%s",
        auth0_user_id,
        name,
    )

    with cloudsql_proxy_control.connection(
        schema_type=schema_type, secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX
    ):
        with SessionFactory.for_proxy(
            database_key=database_key,
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
            autocommit=False,
        ) as session:
            db_user = UserAccountInterface.get_user_by_email(
                session=session, email=email
            )
            if db_user:
                raise ValueError("User with this email already exists in our DB.")

            user = schema.UserAccount(
                name=name,
                auth0_user_id=auth0_user_id,
                email=email,
            )

            logger.info(
                "Creating new user with name=%s, auth0_user_id=%s, and email=%s",
                name,
                auth0_user_id,
                email,
            )

            if not dry_run:
                session.add(user)
                session.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        create_user_account(
            email=args.email,
            dry_run=args.dry_run,
        )
