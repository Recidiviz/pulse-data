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
"""Script for generating Reports for the latest month and/or year for each Justice Counts Agency.

Local Usage: docker exec pulse-data_control_panel_backend_1 pipenv run python -m recidiviz.justice_counts.jobs.create_new_reports
Remote Usage: Execute the `justice-counts-recurring-reports` Cloud Run Job
"""

import datetime
import logging

from sqlalchemy.engine import Engine

from recidiviz.justice_counts.report import ReportInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)

logger = logging.getLogger(__name__)


def main(engine: Engine) -> None:
    """For each agency, this function checks if a monthly report exists for the current
    month and year. If the monthly report already exists, nothing is done. If the
    monthly report does not exist, one is created.

    Additionally, for each agency, this function checks if an annual report exists for
    the current month and year. If the annual report already exists, nothing is done. If
    the annual report does not exists AND it is January, an annual report is created."""

    session = Session(bind=engine)
    current_dt = datetime.datetime.now(tz=datetime.timezone.utc)
    current_month = current_dt.month
    current_year = current_dt.year

    logger.info("Generating new Reports in database %s", engine.url)
    for agency in session.query(schema.Agency).all():
        logger.info("Generating Reports for Agency %s", agency.name)

        monthly_report, yearly_report = ReportInterface.create_new_reports(
            session=session,
            agency_id=agency.id,
            user_account_id=None,
            current_month=current_month,
            current_year=current_year,
        )
        if monthly_report:
            logger.info(
                "Generated Monthly Report for Agency %s, Month %s, Year %s",
                agency.name,
                current_month,
                current_year,
            )
        else:
            logger.info(
                "Monthly Report for Agency %s, Month %s, Year %s already exists.",
                agency.name,
                current_month,
                current_year,
            )

        if yearly_report:
            logger.info(
                "Generated Annual Report for Agency %s, Month %s, Year %s",
                agency.name,
                current_month,
                current_year,
            )
        elif current_month == 1:
            logger.info(
                "Annual Report for Agency %s, Month %s, Year %s already exists.",
                agency.name,
                current_month,
                current_year,
            )
        else:
            logger.info(
                "Annual Report for Agency %s, Month %s, Year %s not created (it is not January).",
                agency.name,
                current_month,
                current_year,
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS)
    justice_counts_engine = SQLAlchemyEngineManager.init_engine(database_key)

    main(justice_counts_engine)
