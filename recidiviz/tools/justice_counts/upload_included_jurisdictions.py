# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.p
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
This script reads a spreadsheet containing agency IDs and included geoids, then
updates jurisdictions in the database for those agencies.


Usage:
  python -m recidiviz.tools.justice_counts.upload_included_jurisdictions --project-id=<PROJECT_ID> --file-path=<FILE_PATH> --dry-run=true

Arguments:
  --project-id: Required, specifies the GCP project to use.
  --file-path: Optional, path to the spreadsheet with agency IDs and FIPS codes.
  --dry-run: Optional, default is True. If True, changes are not committed.
"""

import argparse
import logging
from typing import Dict, List

import pandas as pd
from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert

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
logging.basicConfig(level=logging.INFO)


def create_parser() -> argparse.ArgumentParser:
    """Creates and returns an argument parser for the script."""
    parser = argparse.ArgumentParser(description="Bulk update included jurisdictions.")
    parser.add_argument(
        "--project-id",
        choices=[
            GCP_PROJECT_JUSTICE_COUNTS_STAGING,
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
        ],
        help="Select which GCP project in which to run this script.",
        required=True,
    )
    parser.add_argument(
        "--file-path",
        type=str,
        help="Path of the spreadsheet with agency IDs and county FIPS codes.",
        required=True,
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    return parser


def bulk_update_included_jurisdictions(
    agency_id_to_geoids: Dict[int, List[str]],
    dry_run: bool,
) -> None:
    """
    Updates or backfills included jurisdictions for the given agencies.

    Args:
        agency_id_to_geoids: A dictionary mapping agency IDs to lists of FIPS geoids.
        dry_run: If True, changes are not committed to the database.
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

            if dry_run is False:
                logger.info(
                    "Removing existing included jurisdictions for agencies in the sheet."
                )
                # Delete only existing included jurisdiction records for the agencies in the input data
                delete_statement = delete(schema.AgencyJurisdiction).where(
                    schema.AgencyJurisdiction.source_id.in_(agency_id_to_geoids.keys()),
                    schema.AgencyJurisdiction.membership
                    == schema.AgencyJurisdictionType.INCLUDE.value,
                )
                session.execute(delete_statement)

            # Update jurisdictions for provided agencies
            values = []
            for agency_id, included_jurisdictions in agency_id_to_geoids.items():
                logger.info(
                    "Processing agency_id: %s with the following geoids: %s.",
                    agency_id,
                    included_jurisdictions,
                )

                values += [
                    {
                        "source_id": agency_id,
                        "geoid": geoid,
                        "membership": schema.AgencyJurisdictionType.INCLUDE.value,
                    }
                    for geoid in included_jurisdictions
                ]
            try:
                insert_statement = insert(schema.AgencyJurisdiction).values(values)
                session.execute(insert_statement)
            except Exception as e:
                logger.error("Failed to update jurisdictions %s", e)

            if dry_run is False:
                logger.info("Committing changes to the database.")
                session.commit()
            else:
                logger.info("Dry run enabled. No changes committed.")


if __name__ == "__main__":
    args = create_parser().parse_args()

    # Read the Excel file and extract data if file path is provided
    id_to_geoids = {}

    try:
        logger.info("Reading Excel file from path: %s", args.file_path)
        df = pd.read_excel(args.file_path)
        id_to_geoids = df.groupby("agency_id")["geoid_include"].apply(list).to_dict()
        logger.info("Loaded %s agencies from file.", len(id_to_geoids))
    except Exception as e:
        logger.error("Failed to read Excel file: %s", e)

    # Run the bulk update process
    with local_project_id_override(args.project_id):
        bulk_update_included_jurisdictions(
            agency_id_to_geoids=id_to_geoids,
            dry_run=args.dry_run,
        )
