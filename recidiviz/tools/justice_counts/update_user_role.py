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
Updates the UserAccount in our DB with given email address to have the given
role for the given agency.

python -m recidiviz.tools.justice_counts.update_user_role \
  --email=EMAIL_ADDRESS \
  --agency="Law Enforcement" \
  --role=JUSTICE_COUNTS_ADMIN \
  --project-id=PROJECT_ID \
  --dry-run=true
"""

import argparse
import logging

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Used to select which GCP project in which to run this script.",
        required=True,
    )
    parser.add_argument(
        "--email",
        type=str,
        help="Email address of the user to update.",
        required=True,
    )
    parser.add_argument(
        "--agency",
        type=str,
        help="Name of the agency for which to update the user's role.",
        required=True,
    )
    parser.add_argument(
        "--role",
        choices=[e.value for e in schema.UserAccountRole],
        help="New role for the user.",
        required=True,
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    return parser


def update_user_role(email: str, agency: str, role: str, dry_run: bool) -> None:
    """
    Updates the UserAccount in our DB with given email address to have the given
    role for the given agency. If the user does not belong to the agency already,
    the corresponding association is created.
    NOTE: There must already be a corresponding UserAccount and Agency in the DB.
    Use the `create_user` script to create a UserAccount and the Admin Panel
    to create an agency.
    """
    schema_type = SchemaType.JUSTICE_COUNTS
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)
    with cloudsql_proxy_control.connection(schema_type=schema_type):
        with SessionFactory.for_proxy(database_key) as session:
            user = UserAccountInterface.get_user_by_email(session=session, email=email)
            if not user:
                raise ValueError(
                    "No user with this email exists in our DB. Create the user first using the `create_user` script."
                )

            agency = AgencyInterface.get_agency_by_name(session=session, name=agency)
            if not agency:
                raise ValueError(
                    "No agency with this name exists in our DB. Create the agency first using the Admin Panel."
                )

            assoc = next(
                (assoc for assoc in user.agency_assocs if assoc.agency_id == agency.id),
                None,
            )

            if not assoc:
                assoc = schema.AgencyUserAccountAssociation(
                    user_id=user.id, agency_id=agency.id
                )
                logging.info("User is not a member of the agency; adding them.")
            else:
                logging.info(
                    "User is currently a member of this agency with role %s", assoc.role
                )

            role_enum = schema.UserAccountRole[role]
            logging.info("Updating role to %s", role_enum.value)
            assoc.role = role_enum

            if not dry_run:
                session.add(assoc)
                session.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        update_user_role(
            email=args.email,
            agency=args.agency,
            role=args.role,
            dry_run=args.dry_run,
        )
