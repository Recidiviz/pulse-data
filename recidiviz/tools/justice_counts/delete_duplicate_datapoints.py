# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
This script identifies and removes duplicate datapoints in the Justice Counts database 
by retaining the most recently updated datapoint among duplicates. 

Usage:
python -m recidiviz.tools.justice_counts.delete_duplicate_datapoints \
  --dry-run=true \
  --project-id=justice-counts-staging
"""

import argparse
import datetime
import json
import logging
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from recidiviz.justice_counts.utils.datapoint_utils import filter_deprecated_datapoints
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
        "--dry-run",
        type=str_to_bool,
        default=False,
        help="Run the script without making changes",
    )
    return parser


def get_duplicate_datapoints(
    session: Session,
    dry_run: bool,
) -> List[Tuple[schema.Datapoint, schema.Datapoint]]:
    """
    Identifies duplicate datapoints in the database and returns a list of tuples
    indicating which datapoint to delete and which to keep.

    This function retrieves all report datapoints from the database, filters out
    deprecated datapoints, and checks for duplicates based on specific key attributes.
    If duplicates are found, it determines which datapoint should be retained by comparing
    their `last_updated` timestamps, with preference given to the most recently updated datapoint.
    """

    logger.info(
        "%sFetching all datapoints from the database.", "DRY RUN: " if dry_run else ""
    )
    all_datapoints = (
        session.query(schema.Datapoint)
        .filter(schema.Datapoint.is_report_datapoint.is_(True))
        .all()
    )
    all_datapoints = filter_deprecated_datapoints(all_datapoints)
    logger.info(
        "%sFetched %d datapoints.", "DRY RUN: " if dry_run else "", len(all_datapoints)
    )

    existing_datapoint_dict: Dict[
        Tuple[
            Optional[datetime.date],  # start_date
            Optional[datetime.date],  # end_date
            int,  # report_id
            int,  # metric_definition_key
            Optional[int],  # context_key
            Optional[str],  # dimension_identifier_to_member
        ],
        schema.Datapoint,  # value
    ] = {}

    # A list of tuples where each tuple contains two datapoints:
    # the first is the datapoint marked for deletion, and the
    # second is the datapoint to retain.
    datapoint_deletion_tuples = []

    for datapoint in all_datapoints:
        datapoint_key = (
            datapoint.start_date,
            datapoint.end_date,
            datapoint.report_id,
            datapoint.metric_definition_key,
            datapoint.context_key,
            (
                json.dumps(datapoint.dimension_identifier_to_member)
                if datapoint.dimension_identifier_to_member
                else None
            ),
        )

        if datapoint_key not in existing_datapoint_dict:
            existing_datapoint_dict[datapoint_key] = datapoint
        else:
            existing_datapoint = existing_datapoint_dict[datapoint_key]
            current_last_updated = datapoint.last_updated or datapoint.created_at
            existing_last_updated = (
                existing_datapoint.last_updated or existing_datapoint.created_at
            )

            if current_last_updated is None and existing_last_updated is None:
                logger.info(
                    "%sBoth datapoints have no timestamps. Falling back to compare values for decision.",
                    "DRY RUN: " if dry_run else "",
                )
                if datapoint.value is not None and existing_datapoint.value is None:
                    logger.info(
                        "%sChoosing datapoint ID: %s to retain because it has a value while the existing datapoint ID: %s does not.",
                        "DRY RUN: " if dry_run else "",
                        datapoint.id,
                        existing_datapoint.id,
                    )
                    datapoint_deletion_tuples.append((existing_datapoint, datapoint))
                    existing_datapoint_dict[datapoint_key] = datapoint
                elif existing_datapoint.value is not None and datapoint.value is None:
                    logger.info(
                        "%sChoosing datapoint ID: %s to retain because it has a value while the existing datapoint ID: %s does not.",
                        "DRY RUN: " if dry_run else "",
                        existing_datapoint.id,
                        datapoint.id,
                    )
                    datapoint_deletion_tuples.append((datapoint, existing_datapoint))
                else:
                    logger.info(
                        "%sRetaining datapoint ID: %s as the first occurrence of the duplicate with ID: %s. Both are missing 'created_at', 'updated_at', and 'value'.",
                        "DRY RUN: " if dry_run else "",
                        existing_datapoint.id,
                        datapoint.id,
                    )
                    datapoint_deletion_tuples.append((datapoint, existing_datapoint))
            else:
                current_last_updated = (
                    current_last_updated
                    if current_last_updated is not None
                    else datetime.datetime.min
                )
                existing_last_updated = (
                    existing_last_updated
                    if existing_last_updated is not None
                    else datetime.datetime.min
                )
                if current_last_updated > existing_last_updated:
                    logger.info(
                        "%sChoosing datapoint ID: %s to retain because it is more recent than the duplicate with ID: %s.",
                        "DRY RUN: " if dry_run else "",
                        datapoint.id,
                        existing_datapoint.id,
                    )
                    datapoint_deletion_tuples.append((existing_datapoint, datapoint))
                    existing_datapoint_dict[datapoint_key] = datapoint
                else:
                    logger.info(
                        "%sChoosing datapoint ID: %s to retain because it is more recent than the duplicate with ID: %s.",
                        "DRY RUN: " if dry_run else "",
                        existing_datapoint.id,
                        datapoint.id,
                    )
                    datapoint_deletion_tuples.append((datapoint, existing_datapoint))

    logger.info(
        "%sIdentified %d duplicate datapoints.",
        "DRY RUN: " if dry_run else "",
        len(datapoint_deletion_tuples),
    )
    return datapoint_deletion_tuples


def delete_duplicate_datapoints(dry_run: bool) -> None:
    """
    Deletes duplicate datapoints in the Justice Counts database.

    Args:
        dry_run (bool): If True, logs the actions that would be taken without making changes.
    """
    schema_type = SchemaType.JUSTICE_COUNTS
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)

    logger.info(
        "%sConnecting to the Justice Counts database.", "DRY RUN: " if dry_run else ""
    )
    with cloudsql_proxy_control.connection(
        schema_type=schema_type, secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX
    ):
        with SessionFactory.for_proxy(
            database_key=database_key,
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
            autocommit=False,
        ) as session:
            logger.info(
                "%sRetrieving duplicate datapoints.", "DRY RUN: " if dry_run else ""
            )
            datapoint_deletion_tuples = get_duplicate_datapoints(
                session=session, dry_run=dry_run
            )

            if dry_run is True:
                logger.info("DRY RUN: No changes will be made.")
            else:
                for to_delete, _ in datapoint_deletion_tuples:
                    logger.info(
                        "Deleting datapoint with ID: %s",
                        to_delete.id,
                    )
                    session.delete(to_delete)
                session.commit()
                logger.info("Deleted duplicate datapoints and committed changes.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()

    with local_project_id_override(args.project_id):
        delete_duplicate_datapoints(dry_run=args.dry_run)
