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
"""Utilities for creating new reports."""

import logging
from typing import Any

from recidiviz.justice_counts.metric_setting import MetricSettingInterface
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.persistence.database.schema.justice_counts import schema


def create_new_report(
    month: int, year: int, session: Any, logger: logging.Logger
) -> None:
    """For each agency, this function checks if a monthly report exists for the previous
    month/year. If the monthly report already exists, nothing is done. If the
    monthly report does not exist, one is created.

    Additionally, for each agency, this function checks if an annual report exists for
    the most recent previous month/year. If the annual report already exists, nothing is done. If
    the annual report does not exists AND it is specified month to create an annual report,
    an annual report is created."""

    # Calculate most recent previous month and year
    if month > 1:
        previous_month = month - 1
    else:
        previous_month = 12
    previous_year = year - 1

    for agency in session.query(schema.Agency).filter(
        # Skip child agencies -- safe to assume their reports will be
        # created via bulk upload
        schema.Agency.super_agency_id.is_(None)
    ):
        logger.info("Generating Reports for Agency %s", agency.name)

        (
            monthly_report,
            yearly_report,
            monthly_metric_defs,
            annual_metric_defs,
        ) = ReportInterface.create_new_reports(
            session=session,
            agency_id=agency.id,
            user_account_id=None,
            current_month=month,
            current_year=year,
            previous_month=previous_month,
            previous_year=previous_year,
            systems={schema.System[sys] for sys in agency.systems},
            metric_key_to_metric_interface=MetricSettingInterface.get_metric_key_to_metric_interface(
                session=session,
                agency=agency,
            ),
        )

        if monthly_report is not None:
            logger.info(
                "Generated Monthly Report for Agency %s, Month %s, Year %s",
                agency.name,
                previous_month,
                previous_year if previous_month == 12 else year,
            )
        elif monthly_report is None and len(monthly_metric_defs) > 0:
            logger.info(
                "Monthly Report for Agency %s, Month %s, Year %s already exists.",
                agency.name,
                previous_month,
                previous_year if previous_month == 12 else year,
            )
        elif monthly_report is None and len(monthly_metric_defs) == 0:
            logger.info(
                "No metrics are included in Monthly Report for Agency %s, Month %s, Year %s.",
                agency.name,
                previous_month,
                previous_year if previous_month == 12 else year,
            )

        if yearly_report is not None:
            logger.info(
                "Generated Annual Report for Agency %s, Month %s, Year %s",
                agency.name,
                month,
                previous_year,
            )
        elif yearly_report is None and len(annual_metric_defs) > 0:
            logger.info(
                "Annual Report for Agency %s, Month %s, Year %s already exists.",
                agency.name,
                month,
                previous_year,
            )
        elif yearly_report is None and len(annual_metric_defs) == 0:
            logger.info(
                "No metrics are included in Annual Report for Agency %s, Month %s, Year %s.",
                agency.name,
                month,
                previous_year,
            )
