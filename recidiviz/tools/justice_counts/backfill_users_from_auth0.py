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

"""Backfills UserAccount tuples with agency information and email by querying auth0.

python -m recidiviz.tools.justice_counts.backfill_users_from_auth0 \
  --project-id=PROJECT_ID \
  --field=email \
  --dry-run=true
"""

import argparse
import logging

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.control_panel.routes.admin import _get_auth0_client
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
        "--field",
        choices=["agencies", "email", "all"],
        help="Used to select which field to backfill.",
        required=True,
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=True)

    return parser


def backfill_user_agencies(
    dry_run: bool, update_email: bool, update_agencies: bool, update_all: bool
) -> None:
    """
    Backfills a users agencies by querying auth0 and saving them to the JC database.
    """
    schema_type = SchemaType.JUSTICE_COUNTS
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)
    with cloudsql_proxy_control.connection(schema_type=schema_type):
        with SessionFactory.for_proxy(
            database_key,
            autocommit=False,
        ) as session:
            auth0_users = _get_auth0_client().get_all_users()
            for user in auth0_users:
                existing_user = (
                    session.query(schema.UserAccount)
                    .filter(schema.UserAccount.auth0_user_id == user["user_id"])
                    .one_or_none()
                )

                if existing_user is None:
                    continue

                agency_ids = user.get("app_metadata", {}).get("agency_ids", [])
                name = user["name"]
                email = user["email"]
                agencies = AgencyInterface.get_agencies_by_id(
                    session=session,
                    agency_ids=agency_ids,
                )
                if dry_run:
                    if update_all or update_agencies:
                        logging.info(
                            "[DRY RUN] Would update %s with %d agencies",
                            name,
                            len(agencies),
                        )
                    if update_all or update_email:
                        logging.info(
                            "[DRY RUN] Would update %s email to %s",
                            name,
                            email,
                        )
                else:
                    if update_all or update_agencies:
                        logger.info(
                            "Updating %s's account with the %d agencies.",
                            name,
                            len(agency_ids),
                        )
                        existing_user.agencies = agencies
                    if update_all or update_email:
                        logger.info(
                            "Updating %s's email to %s",
                            name,
                            email,
                        )
                        existing_user.email = email
                    if not dry_run:
                        session.add(existing_user)
            if not dry_run:
                session.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        backfill_user_agencies(
            dry_run=args.dry_run,
            update_agencies=args.field == "agencies",
            update_email=args.field == "email",
            update_all=args.field == "all",
        )
