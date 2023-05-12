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

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)

logger = logging.getLogger(__name__)


def main(engine: Engine) -> None:
    """For each agency, this function checks if a monthly report exists for the most recent previous
    month/year. If the monthly report already exists, nothing is done. If the
    monthly report does not exist, one is created.

    Additionally, for each agency, this function checks if an annual report exists for
    the most recent previous month/year. If the annual report already exists, nothing is done. If
    the annual report does not exists AND it is specified month to create an annual report,
    an annual report is created."""

    session = Session(bind=engine)
    current_dt = datetime.datetime.now(tz=datetime.timezone.utc)
    current_month = current_dt.month
    current_year = current_dt.year
    # Calculate most recent previous month and year
    if current_month > 1:
        previous_month = current_month - 1
    else:
        previous_month = 12
    previous_year = current_year - 1

    logger.info("Generating new Reports in database %s", engine.url)
    for agency in session.query(schema.Agency).all():
        child_agencies = AgencyInterface.get_child_agencies_by_super_agency_id(
            session=session, agency_id=agency.id
        )
        if len(child_agencies) > 0:
            continue

        logger.info("Generating Reports for Agency %s", agency.name)

        agency_datapoints = DatapointInterface.get_agency_datapoints(
            session=session, agency_id=agency.id
        )
        metric_key_to_datapoints = DatapointInterface.build_metric_key_to_datapoints(
            agency_datapoints
        )

        (
            monthly_report,
            yearly_report,
            monthly_metric_defs,
            annual_metric_defs,
        ) = ReportInterface.create_new_reports(
            session=session,
            agency_id=agency.id,
            user_account_id=None,
            current_month=current_month,
            current_year=current_year,
            previous_month=previous_month,
            previous_year=previous_year,
            systems={schema.System[sys] for sys in agency.systems},
            metric_key_to_datapoints=metric_key_to_datapoints,
        )

        if monthly_report is not None:
            logger.info(
                "Generated Monthly Report for Agency %s, Month %s, Year %s",
                agency.name,
                previous_month,
                previous_year if previous_month == 12 else current_year,
            )
        elif monthly_report is None and len(monthly_metric_defs) > 0:
            logger.info(
                "Monthly Report for Agency %s, Month %s, Year %s already exists.",
                agency.name,
                previous_month,
                previous_year if previous_month == 12 else current_year,
            )
        elif monthly_report is None and len(monthly_metric_defs) == 0:
            logger.info(
                "No metrics are included in Monthly Report for Agency %s, Month %s, Year %s.",
                agency.name,
                previous_month,
                previous_year if previous_month == 12 else current_year,
            )

        if yearly_report is not None:
            logger.info(
                "Generated Annual Report for Agency %s, Month %s, Year %s",
                agency.name,
                current_month,
                previous_year,
            )
        elif yearly_report is None and len(annual_metric_defs) > 0:
            logger.info(
                "Annual Report for Agency %s, Month %s, Year %s already exists.",
                agency.name,
                current_month,
                previous_year,
            )
        elif yearly_report is None and len(annual_metric_defs) == 0:
            logger.info(
                "No metrics are included in Annual Report for Agency %s, Month %s, Year %s.",
                agency.name,
                current_month,
                previous_year,
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS)
    justice_counts_engine = SQLAlchemyEngineManager.init_engine(database_key)

    main(justice_counts_engine)
