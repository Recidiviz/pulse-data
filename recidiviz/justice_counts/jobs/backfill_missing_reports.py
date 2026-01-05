#!/usr/bin/env bash

# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
Script for backfilling missing Reports between the month ranges for each Justice Counts Agency.
Missing reports are created at the first of every month. However, if that job fails for
a period of time, this script can be used to backfill.

Local Usage: docker exec pulse-data-control_panel_backend-1 uv run python -m recidiviz.justice_counts.jobs.backfill_missing_reports --start_month {} --end_month_inclusive {} --year {}
Remote Usage: Execute the `justice-counts-backfill-missing-reports` Cloud Run Job
"""

import argparse
import logging
import sys
from typing import List, Tuple

import sentry_sdk
from sqlalchemy.engine import Engine

from recidiviz.justice_counts.utils.constants import JUSTICE_COUNTS_SENTRY_DSN
from recidiviz.justice_counts.utils.create_new_report import create_new_report
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)

logger = logging.getLogger(__name__)


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start_month",
        type=int,
        help="Backfill data for all months between `start_month`/`year` and `end_month_inclusive`/`year`. Represent month as integer with January as 1 and December as 12.",
        required=True,
    )
    parser.add_argument(
        "--end_month_inclusive",
        type=int,
        help="Backfill data for all months between `start_month`/`year` and `end_month_inclusive`/`year`. Represent month as integer with January as 1 and December as 12.",
        required=True,
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Backfill data for all months between `start_month`/`year` and `end_month_inclusive`/`year`. Represent month as integer with January as 1 and December as 12.",
        required=True,
    )
    return parser.parse_known_args(argv)


def validate_input(start_month: int, end_month_inclusive: int) -> None:
    """Validate month input ranges."""
    if start_month < 1:
        raise ValueError("start_month must be greater than 1.")
    if end_month_inclusive > 12:
        raise ValueError("end_month_inclusive must be less than or equal to 12.")
    if start_month > end_month_inclusive:
        raise ValueError(
            "start_month must be less than or equal to end_month_inclusive."
        )


def create_new_reports(
    engine: Engine, start_month: int, end_month_inclusive: int, year: int
) -> None:
    """
    For each month in the month range [start_month, end_month_inclusive] for `year`,
    create a new report if the previous month is missing.
    """
    session = Session(bind=engine)
    logger.info("Generating new Reports in database %s", engine.url)
    for month in range(start_month, end_month_inclusive + 1):
        create_new_report(month, year, session, logger)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sentry_sdk.init(
        dsn=JUSTICE_COUNTS_SENTRY_DSN,
        # Enable performance monitoring
        enable_tracing=True,
    )
    known_args, _ = parse_arguments(sys.argv)
    validate_input(known_args.start_month, known_args.end_month_inclusive)

    database_key = SQLAlchemyDatabaseKey.for_schema(
        SchemaType.JUSTICE_COUNTS,
    )
    justice_counts_engine = SQLAlchemyEngineManager.init_engine(
        database_key=database_key,
        secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
    )
    create_new_reports(
        justice_counts_engine,
        known_args.start_month,
        known_args.end_month_inclusive,
        known_args.year,
    )
