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
This script backfills jurisdictions in the database by populating the geoid field.

Usage:
  python -m recidiviz.tools.justice_counts.backfill_geoids  --project-id=justice-counts-staging --dry-run=true 

Arguments:
  --project-id: Required, specifies the GCP project to use.
  --dry-run: Optional, default is True. If True, changes are not committed.
"""

import argparse
import logging
from typing import Dict, Optional

from recidiviz.justice_counts.utils.geoid import get_fips_code_to_geoid
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
    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    return parser


def backfill_geoids(
    dry_run: bool,
) -> None:
    """
    Backfills existing jurisdictions with geoids

     Args:
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
            # fips_code_to_geoid maps fips code to it's corresponding GEOID. Unfortunately,
            # our data sources add inconsistent padding to the fips codes and so
            # format_fips_code_for_geoid_lookup to standardize the FIPS code for lookup.
            fips_code_to_geoid: Dict[str, str] = get_fips_code_to_geoid()

            try:
                existing_jurisdictions = session.query(schema.AgencyJurisdiction).all()
                for jurisdiction in existing_jurisdictions:
                    if jurisdiction.geoid is not None:
                        continue

                    # Note: jursidiction_id (str) represents the FIPS code of the jurisdiction
                    formatted_fips_code = format_fips_code_for_geoid_lookup(
                        jurisdiction.jurisdiction_id, fips_code_to_geoid
                    )

                    if formatted_fips_code is None:
                        logger.warning(
                            "Unable to map jurisdiction_id %s to a geoid using formatted FIPS code %s.",
                            jurisdiction.jurisdiction_id,
                            formatted_fips_code,
                        )
                        continue

                    geoid = fips_code_to_geoid.get(formatted_fips_code)

                    jurisdiction.geoid = geoid
                    logger.info(
                        "Backfilled geoid %s for agency_id: %s, fips code: %s",
                        geoid,
                        jurisdiction.source_id,
                        jurisdiction.jurisdiction_id,
                    )

                if not dry_run:
                    logger.info("Committing changes to the database.")
                    session.commit()
                else:
                    logger.info("Dry run enabled. No changes committed.")
            except Exception as e:
                logger.error("Error during backfill: %s", e)
                session.rollback()


def format_fips_code_for_geoid_lookup(
    fips_code: str, fips_code_to_geoid: Dict[str, str]
) -> Optional[str]:
    """
    Formats the FIPS code for lookup in the fips_code_to_geoid dictionary.

    Args:
        fips_code: The raw FIPS code from the database.
        fips_code_to_geoid: The mapping of FIPS codes to GEOIDs.

    Returns:
        A formatted FIPS code suitable for GEOID lookup, or None if no match is found.
    """
    # Direct match: Check if the exact FIPS code exists in the mapping
    if fips_code in fips_code_to_geoid:
        return fips_code

    # Remove all trailing zeros and check if the modified FIPS code exists in the mapping
    if fips_code.rstrip("0") in fips_code_to_geoid:
        return fips_code.rstrip("0")

    # Handle state-level FIPS codes:
    # After stripping trailing zeros, ensure the code is zero-padded to two characters
    # and check if it exists in the mapping
    state_fips = fips_code.rstrip("0").zfill(2)
    if state_fips in fips_code_to_geoid:
        return state_fips

    # If no match is found, return None (caller can log a warning or handle appropriately)
    return None


if __name__ == "__main__":
    args = create_parser().parse_args()

    # Run the bulk update process
    with local_project_id_override(args.project_id):
        backfill_geoids(
            dry_run=args.dry_run,
        )
