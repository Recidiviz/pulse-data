# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
This script backfills metric reporting agency IDs and updates their corresponding agency 
settings based on the data provided in an Excel spreadsheet.

The script processes rows of agency data, updating metric reporting agency IDs, self-reporting
status, and agency homepage URLs for each agency and metric pair listed. 


Usage:
    python -m recidiviz.tools.justice_counts.backfill_reporting_agency_id \
        --file_path=<path> \
        --project-id=PROJECT_ID \
        --dry-run=true
"""

import argparse
import logging
import math

import pandas as pd

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.agency_setting import AgencySettingInterface
from recidiviz.justice_counts.metric_setting import MetricSettingInterface
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
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
    parser = argparse.ArgumentParser(
        description="Backfill reporting agency IDs and update agency settings."
    )
    parser.add_argument(
        "--project-id",
        choices=[
            GCP_PROJECT_JUSTICE_COUNTS_STAGING,
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
        ],
        help="GCP project in which to run this script.",
        required=True,
    )
    parser.add_argument(
        "--file_path",
        type=str,
        help="Path to the spreadsheet containing agency data.",
        required=True,
    )
    parser.add_argument(
        "--dry-run",
        type=str_to_bool,
        default=False,
        help="Run the script without committing changes.",
    )
    return parser


def backfill_reporting_agency_ids(
    sheet_df: pd.DataFrame,
    dry_run: bool,
) -> None:
    """
    Backfills reporting agency IDs and updates agency settings based on the provided rows.

    Args:
        rows: List of dictionaries representing the agency data to be processed.
        dry_run: Boolean flag to indicate if changes should be committed (False) or not (True).
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

            agency_ids = sheet_df["id"].tolist()
            agencies = AgencyInterface.get_agencies_by_id(
                session=session, agency_ids=agency_ids
            )

            agency_id_to_agency = {r.id: r for r in agencies}

            rows = sheet_df.to_dict(orient="records")
            for row in rows:
                msg = "" if dry_run is False else "DRY RUN:"
                metric_interface_update = MetricInterface(
                    key=row["metric_key"],
                )

                reporting_agency_id = (
                    None
                    if math.isnan(row["reporting_agency_id"])
                    else int(row["reporting_agency_id"])
                )
                if not math.isnan(row["reporting_agency_id"]):
                    metric_interface_update.reporting_agency_id = reporting_agency_id
                if isinstance(row["is_self_reporting"], bool):
                    metric_interface_update.is_self_reported = row["is_self_reporting"]

                MetricSettingInterface.add_or_update_agency_metric_setting(
                    session=session,
                    agency=agency_id_to_agency[row["id"]],
                    agency_metric_updates=metric_interface_update,
                )

                msg += f"{agency_id_to_agency[row['id']].name}: Updating {row['metric_key']} -> reporting_agency_id to {reporting_agency_id}, is_self_reported {row['is_self_reporting']} \n\n "

                if isinstance(row["reporting_agency_url"], str):
                    AgencySettingInterface.create_or_update_agency_setting(
                        session=session,
                        agency_id=row["reporting_agency_id"],
                        setting_type=schema.AgencySettingType.HOMEPAGE_URL,
                        value=row["reporting_agency_url"],
                    )
                    msg += f"Updating url to {row['reporting_agency_url']}\n\n"

                logger.info(msg)

            if not dry_run:
                session.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    xls = pd.ExcelFile(args.file_path)

    with local_project_id_override(args.project_id):
        backfill_reporting_agency_ids(
            dry_run=args.dry_run,
            sheet_df=pd.read_excel(xls),
        )
