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
This script adds a Recidiviz user to all previously created agencies with the role of 
JUSTICE_COUNTS_ADMIN. This script is meant to be run 1-time to update older
agencies as Recidiviz users are automatically added to newly created agencies.

python -m recidiviz.tools.justice_counts.add_recidiviz_user_older_agencies \
  --user-id=<user_id> \
  --project-id=justice-counts-staging \
  --dry-run=true
"""

import argparse
import logging

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema.justice_counts.schema import UserAccountRole
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
        "--user-id",
        required=True,
        help="The id of the user you would like to add to all agencies.",
    )
    parser.add_argument(
        "--project-id",
        choices=[
            GCP_PROJECT_JUSTICE_COUNTS_STAGING,
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
        ],
        help="Used to select which GCP project in which to run this script.",
        required=True,
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    return parser


def add_recidiviz_user_to_agencies(user_id: int, dry_run: bool) -> None:
    """
    This function retrieves all agencies and adds a specified Recidiviz users to all
    agencies with the JUSTICE_COUNTS_ADMIN role.
    """
    schema_type = SchemaType.JUSTICE_COUNTS
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)

    with cloudsql_proxy_control.connection(
        schema_type=schema_type, secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX
    ):
        with SessionFactory.for_proxy(
            database_key=database_key,
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
            autocommit=False,
        ) as session:
            user = UserAccountInterface.get_user_by_id(
                session=session, user_account_id=user_id
            )
            if "@recidiviz.org" not in user.email:
                logging.info(
                    "Please provide a user_id that is associated with a Recidiviz user."
                )
                return

            all_agencies = AgencyInterface.get_agencies(
                session=session, with_users=False, with_settings=False
            )
            if dry_run is False:
                # Add user to agencies as Justice Counts Admins
                UserAccountInterface.add_or_update_user_agency_association(
                    session=session,
                    user=user,
                    agencies=all_agencies,
                    role=UserAccountRole.JUSTICE_COUNTS_ADMIN,
                )
                session.commit()
                logging.info(
                    "Added %s to %s total agencies", user.name, len(all_agencies)
                )
            else:
                logging.info(
                    "Would add %s to %s total agencies", user.name, len(all_agencies)
                )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        add_recidiviz_user_to_agencies(
            user_id=args.user_id,
            dry_run=args.dry_run,
        )
