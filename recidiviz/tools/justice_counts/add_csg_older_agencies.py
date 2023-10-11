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
This script adds all CSG users to all previously created agencies. If the agency exists
in production, we set their roles to READ_ONLY. If the agency exists in staging, we set
their roles to AGENCY_ADMIN. This script is meant to be run 1-time to update older
agencies as CSG users are automatically added to newly created agencies.

python -m recidiviz.tools.justice_counts.add_csg_older_agencies \
  --project-id=justice-counts-staging \
  --dry-run=true \
  --preserve-role=true
"""

import argparse
import logging

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
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
        "--project-id",
        choices=[
            GCP_PROJECT_JUSTICE_COUNTS_STAGING,
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
        ],
        help="Used to select which GCP project in which to run this script.",
        required=True,
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    parser.add_argument("--preserve-role", type=str_to_bool, default=True)
    return parser


def add_csg_users_to_agencies(
    dry_run: bool, project_id: str, preserve_role: bool
) -> None:
    """
    This function retrieves all agencies (filtering out test agencies),
    as well as all CSG users, and adds all CSG users to all agencies. If the agency exists
    in production, we set their roles to READ_ONLY. If the agency exists in staging, we set
    their roles to AGENCY_ADMIN. If a CSG user has been previously assigned a role
    for a given agency, we preserve that role unless preserve_role is passed in as False.
    """
    schema_type = SchemaType.JUSTICE_COUNTS
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)

    with cloudsql_proxy_control.connection(schema_type=schema_type):
        with SessionFactory.for_proxy(
            database_key=database_key, autocommit=False
        ) as session:
            all_agencies = AgencyInterface.get_agencies(
                session=session, with_users=False, with_settings=False
            )
            logging.info("%s total agencies", len(all_agencies))
            csg_users = UserAccountInterface.get_csg_users(session=session)
            logging.info("%s total CSG users", len(csg_users))
            for csg_user in csg_users:
                if dry_run is False:
                    role = (
                        "AGENCY_ADMIN"
                        if project_id == GCP_PROJECT_JUSTICE_COUNTS_STAGING
                        else "READ_ONLY"
                    )
                    UserAccountInterface.add_or_update_user_agency_association(
                        session=session,
                        user=csg_user,
                        agencies=all_agencies,
                        role=UserAccountRole[role],
                        preserve_role=preserve_role,
                    )
                    logging.info("Added %s to agencies", csg_user.name)
                else:
                    associations_agency_ids_by_role = {
                        user_associations.agency_id: user_associations.role
                        for user_associations in csg_user.agency_assocs
                    }
                    for agency in all_agencies:
                        if associations_agency_ids_by_role.get(agency.id) is None:
                            logging.info(
                                "No association for %s, %s", csg_user.name, agency.name
                            )
                        else:
                            role = associations_agency_ids_by_role.get(agency.id)  # type: ignore[assignment]
                            if (
                                project_id == GCP_PROJECT_JUSTICE_COUNTS_STAGING
                                and role != UserAccountRole.AGENCY_ADMIN
                            ):
                                logging.info(
                                    "Will override %s, %s, %s",
                                    csg_user.name,
                                    agency.name,
                                    role,
                                )
                            elif (
                                project_id == GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION
                                and role != UserAccountRole.READ_ONLY
                            ):
                                logging.info(
                                    "Will override %s, %s, %s",
                                    csg_user.name,
                                    agency.name,
                                    role,
                                )
            if dry_run is False:
                session.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        add_csg_users_to_agencies(
            dry_run=args.dry_run,
            project_id=args.project_id,
            preserve_role=args.preserve_role,
        )
