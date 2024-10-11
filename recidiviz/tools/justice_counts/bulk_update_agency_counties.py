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
Ingests a spreadsheet with a list of agency ids and county fips and updates 
the counties of the existing agencies in bulk.

python -m recidiviz.tools.justice_counts.bulk_update_agency_counties \
 --file_path=<path> \
  --project-id=PROJECT_ID \
  --dry-run=true
"""

import argparse
import logging
from typing import Dict

import pandas as pd

from recidiviz.common.fips import get_county_fips_to_county_code, sanitize_county_name
from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
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
        "--file_path",
        type=str,
        help="Path of spreadsheet with agency ids and county fips",
        required=True,
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=False)
    return parser


def bulk_update_agency_county(
    agency_id_to_county_fips: Dict[int, str],
    dry_run: bool,
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
            county_fips_to_county_code = get_county_fips_to_county_code()
            county_codes = set(county_fips_to_county_code.values())
            for agency_id, county_fips in agency_id_to_county_fips.items():
                agency = AgencyInterface.get_agency_by_id(
                    session=session, agency_id=agency_id
                )
                child_agency_county_code = None

                if county_fips is not None and county_fips in county_codes:
                    child_agency_county_code = sanitize_county_name(county_fips)

                elif county_fips in county_fips_to_county_code:
                    child_agency_county_code = sanitize_county_name(
                        county_fips_to_county_code[county_fips]
                    )

                agency.fips_county_code = child_agency_county_code

                msg = "" if dry_run is False else "DRY RUN: "
                msg += f"Updating {agency.name} with county {child_agency_county_code}.\n\n"
                logger.info(msg)

                if dry_run is False:
                    session.add(agency)
            if dry_run is False:
                session.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    xls = pd.ExcelFile(args.file_path)

    sheet_df = pd.read_excel(xls)
    # Extract the 'agencies' sheet into a list of dictionaries with 'name' and optionally 'county_fips' keys
    agency_id_to_county_fips_dict = dict(
        zip(sheet_df["agency_id"], sheet_df["county_fips"])
    )
    with local_project_id_override(args.project_id):
        bulk_update_agency_county(
            dry_run=args.dry_run,
            agency_id_to_county_fips=agency_id_to_county_fips_dict,
        )
