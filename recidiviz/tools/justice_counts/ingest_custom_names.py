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
Ingest custom child agency names from a spreadsheet into the source
database of the specified project.

Usage:
    python -m recidiviz.tools.justice_counts.ingest_custom_names \
        --project-id=PROJECT_ID \
        --path=<PATH> \
        --dry-run=true

This script reads custom child agency names from a spreadsheet at the given path
and updates the relevant agencies in the Justice Counts database. 
The script can operate in dry-run mode to test the data ingestion without
committing changes.

Arguments:
    --project-id: The GCP project ID where the script runs.
    --path: The path to the spreadsheet containing the custom names data.
    --dry-run: If set to true, the script does not commit changes to the database.
"""

import argparse
import logging

import pandas as pd

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
    parser = argparse.ArgumentParser(
        description="Script to ingest custom child agency names into the Justice Counts database."
    )
    parser.add_argument(
        "--project-id",
        choices=[
            GCP_PROJECT_JUSTICE_COUNTS_STAGING,
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
        ],
        help="Selects which GCP project to run the script in.",
        required=True,
    )
    parser.add_argument(
        "--path",
        type=str,
        help="Path to the spreadsheet containing custom names data.",
        required=True,
    )
    parser.add_argument(
        "--dry-run",
        type=str_to_bool,
        default=True,
        help="Set to true for dry-run mode.",
    )
    return parser


def ingest_custom_child_agency_name(data: pd.DataFrame, dry_run: bool) -> None:
    """
    Ingests custom child agency names into the Justice Counts database.

    Args:
        agency_ids: List of agency IDs to update.
        rows: List of dictionary records containing custom names data.
        dry-run: If True, does not commit changes to the database.
    """
    logger.info("Establishing database connection.")

    schema_type = SchemaType.JUSTICE_COUNTS
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)
    with cloudsql_proxy_control.connection(
        schema_type=schema_type,
        secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
    ):
        with SessionFactory.for_proxy(
            database_key,
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
            autocommit=False,
        ) as session:
            agency_ids = data["agency_id"].tolist()
            rows = data.to_dict("records")
            agencies = AgencyInterface.get_agencies_by_id(
                session=session, agency_ids=agency_ids
            )
            agency_id_to_agency = {a.id: a for a in agencies}
            for row in rows:
                agency_id = row.get("agency_id")
                if agency_id not in agency_id_to_agency:
                    logger.warning("Agency ID %s not found in the database.", agency_id)
                    continue

                agency = agency_id_to_agency[agency_id]
                updated_agency = AgencyInterface.update_custom_child_agency_name(
                    agency=agency,
                    custom_name=row["custom_name"],
                )
                if updated_agency is None:
                    logger.info("%s is not a child agency, skipping", agency.name)
                    continue
                logger.info(
                    "Set custom name '%s' for agency ID %s.",
                    row["custom_name"],
                    agency_id,
                )

            if dry_run is False:
                session.commit()
                logger.info("Database changes committed.")
            else:
                logger.info("Dry-run mode - no database changes committed.")


def validate_columns(data: pd.DataFrame) -> None:
    # Check if the data has exactly two columns
    if len(data.columns) != 2:
        raise ValueError(f"Expected exactly 2 columns, but got {len(data.columns)}")

    # Check if the column names match the expected names
    if set(data.columns) != {"custom_name", "agency_id"}:
        raise ValueError(
            f"Expected columns 'custom_name', 'agency_id', but got {list(data.columns)}"
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        logger.info("Reading spreadsheet from path: %s", args.path)
        dataframe = pd.read_excel(args.path)
        validate_columns(data=dataframe)
        # Drop rows with missing data in key columns
        data_filtered = dataframe.dropna()
        ingest_custom_child_agency_name(data=data_filtered, dry_run=args.dry_run)
