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
Ingests a spreadsheet with a list of agency names and a list of 
User IDs and roles to create new child agencies in bulk.
Example sheet can be found at the link below
https://docs.google.com/spreadsheets/d/10JtZYCzezFBe7eqpZ0dwnItJp2NCmrwCqYqn2KABSE0/edit#gid=1833236576 

python -m recidiviz.tools.justice_counts.add_child_agencies \
  --file_path=<path>\
  --super_agency_id=<int>
  --system=<SYSTEM>\
  --project-id=PROJECT_ID \
  --dry-run=true
"""

import argparse
import logging
from typing import Dict, List

import pandas as pd

from recidiviz.common.constants.states import StateCode
from recidiviz.justice_counts.agency import AgencyInterface
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
from recidiviz.utils.params import str_to_bool, str_to_list

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
        "--file_path",
        type=str,
        help="Path of spreadsheet with agency names and user id / roles",
        required=True,
    )
    parser.add_argument(
        "--systems",
        type=str_to_list,
        help="System of child agencies",
        required=True,
    )
    parser.add_argument(
        "--super_agency_id",
        type=int,
        help="Agency id of super agency being updated",
        required=True,
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=False)
    return parser


def add_child_agencies(
    child_agency_names: List[str],
    dry_run: bool,
    super_agency_id: int,
    user_id_to_role: Dict[int, str],
    systems: List[str],
) -> None:
    """
    Creates new child agencies for the specified super agency
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
            super_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=super_agency_id
            )
            if super_agency.is_superagency is not True:
                super_agency.is_superagency = True

            msg = "" if dry_run is False else "DRY RUN:"
            msg += "Adding %s child agencies for %s"
            logger.info(
                msg,
                str(len(child_agency_names)),
                super_agency.name,
            )
            for child_agency_name in child_agency_names:

                existing_agency = AgencyInterface.get_agency_by_name_state_and_systems(
                    session=session,
                    systems=systems,
                    name=child_agency_name,
                    state_code=super_agency.state_code,
                )

                name = child_agency_name
                if existing_agency is not None:
                    updated_name = (
                        existing_agency.name
                        + f" ({StateCode[existing_agency.state_code.upper()].get_state().abbr})"
                    )
                    msg = "" if dry_run is False else "DRY RUN:"
                    msg += "Agency with name %s already exists. Updating existing agency from %s -> %s\n"
                    logger.info(msg, name, existing_agency.name, updated_name)
                    existing_agency.name = updated_name
                    name = (
                        child_agency_name
                        + f" ({StateCode[super_agency.state_code.upper()].get_state().abbr})"
                    )

                child_agency = schema.Agency(
                    name=child_agency_name,
                    super_agency_id=super_agency_id,
                    state_code=super_agency.state_code,
                    systems=systems,
                )
                msg = "" if dry_run is False else "DRY RUN:"
                msg += "Adding Child Agency: %s"
                logger.info(msg, name)
                agency_user_account_associations = []
                for user_id, role in user_id_to_role.items():
                    assoc = schema.AgencyUserAccountAssociation(
                        agency=child_agency,
                        user_account_id=user_id,
                        role=schema.UserAccountRole[role],
                    )
                    agency_user_account_associations.append(assoc)
                if dry_run is False:
                    session.add_all([child_agency] + agency_user_account_associations)
            if dry_run is False:
                session.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    xls = pd.ExcelFile(args.file_path)

    # Extract the 'agency names' sheet into a list of strings
    agency_names_df = pd.read_excel(xls, sheet_name="agencies", usecols=["name"])
    agency_names = agency_names_df["name"].tolist()

    # Extract the 'users' sheet into a dictionary mapping user IDs to roles
    users_df = pd.read_excel(xls, sheet_name="users", usecols=["user_id", "role"])
    user_id_to_role_dict = dict(zip(users_df["user_id"], users_df["role"]))
    with local_project_id_override(args.project_id):
        add_child_agencies(
            dry_run=args.dry_run,
            child_agency_names=agency_names,
            user_id_to_role=user_id_to_role_dict,
            systems=args.systems,
            super_agency_id=args.super_agency_id,
        )
