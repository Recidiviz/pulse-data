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
 --file_path=<path> \
  --super_agency_id=<int> \
  --system=<SYSTEM> \
  --project-id=PROJECT_ID \
  --dry-run=true
"""

import argparse
import datetime
import logging
from typing import Dict, List

import pandas as pd

from recidiviz.common.fips import get_county_fips_to_county_code, sanitize_county_name
from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.agency_user_account_association import (
    AgencyUserAccountAssociationInterface,
)
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
    child_agency_data: List[
        Dict[str, str]
    ],  # we expect each dictionary to have at least
    #  a "name" key and optionally a "county" and "custom_name" key.
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
                str(len(child_agency_data)),
                super_agency.name,
            )
            county_fips_to_county_code = get_county_fips_to_county_code()
            county_codes = set(county_fips_to_county_code.values())
            for row in child_agency_data:
                child_agency_name = row.get("name")
                county_fips = row.get("county")
                custom_child_agency_name = row.get("custom_name")
                child_agency_county_code = None

                if county_fips is not None and county_fips in county_codes:
                    child_agency_county_code = sanitize_county_name(county_fips)

                elif county_fips in county_fips_to_county_code:
                    child_agency_county_code = sanitize_county_name(
                        county_fips_to_county_code[county_fips]
                    )

                if child_agency_name is None:
                    continue

                existing_agency = AgencyInterface.get_agency_by_name_state_and_systems(
                    session=session,
                    systems=systems,
                    name=child_agency_name,
                    state_code=super_agency.state_code,
                )
                msg = "" if dry_run is False else "DRY RUN: "
                if existing_agency is not None:
                    msg += f"Agency with name {child_agency_name} already exists "
                    if existing_agency.super_agency_id == super_agency_id:
                        msg += "and is already a child agency for the superagency. "
                    else:
                        existing_agency.super_agency_id = super_agency_id
                        msg += "but is not a child agency for the superagency. "
                    existing_agency.custom_child_agency_name = custom_child_agency_name
                    existing_agency.fips_county_code = child_agency_county_code
                    child_agency = existing_agency
                    msg += f"Updating Existing Child Agency with county {child_agency_county_code} and custom name {custom_child_agency_name}\n\n"
                else:
                    child_agency = schema.Agency(
                        name=child_agency_name,
                        super_agency_id=super_agency_id,
                        state_code=super_agency.state_code,
                        systems=systems,
                        fips_county_code=child_agency_county_code,
                        custom_child_agency_name=custom_child_agency_name,
                        created_at=datetime.datetime.now(tz=datetime.timezone.utc),
                        is_dashboard_enabled=True,
                    )
                    msg += f"Adding Child Agency: {child_agency_name} with county {child_agency_county_code} and custom name {custom_child_agency_name}"

                logger.info(msg)
                agency_user_account_associations: List[
                    schema.AgencyUserAccountAssociation
                ] = []
                users = UserAccountInterface.get_users_by_id(
                    session=session, user_account_ids=set(user_id_to_role.keys())
                )
                for user in users:
                    AgencyUserAccountAssociationInterface.update_user_role(
                        session=session,
                        agency=child_agency,
                        user=user,
                        role=user_id_to_role[user.id],
                    )
                if dry_run is False:
                    session.add_all([child_agency] + agency_user_account_associations)
            if dry_run is False:
                session.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    xls = pd.ExcelFile(args.file_path)

    # Check for presence of 'county' column. We expect this column to be either None or hold
    # the county FIPS codes
    agency_columns = ["name"]
    if "county" in pd.read_excel(xls, sheet_name="agencies", nrows=0).columns:
        agency_columns.append("county")
    if "custom_name" in pd.read_excel(xls, sheet_name="agencies", nrows=0).columns:
        agency_columns.append("custom_name")

    # Extract the 'agencies' sheet into a list of dictionaries with 'name' and optionally 'county' keys
    agency_names_df = pd.read_excel(xls, sheet_name="agencies", usecols=agency_columns)
    agency_data = agency_names_df.to_dict(orient="records")

    # Extract the 'users' sheet into a dictionary mapping user IDs to roles
    users_df = pd.read_excel(xls, sheet_name="users", usecols=["user_id", "role"])
    user_id_to_role_dict = dict(zip(users_df["user_id"], users_df["role"]))
    with local_project_id_override(args.project_id):
        add_child_agencies(
            dry_run=args.dry_run,
            child_agency_data=agency_data,
            user_id_to_role=user_id_to_role_dict,
            systems=args.systems,
            super_agency_id=args.super_agency_id,
        )
