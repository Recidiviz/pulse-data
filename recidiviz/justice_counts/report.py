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
"""Interface for working with the Reports model."""
import datetime
from typing import Any, Dict, List

from sqlalchemy.orm import Session

from recidiviz.justice_counts.exceptions import JusticeCountsPermissionError
from recidiviz.justice_counts.metrics.metric_definition import ReportingFrequency
from recidiviz.justice_counts.metrics.reported_metric import ReportedMetric
from recidiviz.justice_counts.report_table_definition import (
    ReportTableDefinitionInterface,
)
from recidiviz.justice_counts.report_table_instance import ReportTableInstanceInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts import schema


class ReportInterface:
    """Contains methods for setting and getting Report info."""

    @staticmethod
    def _raise_if_user_is_unauthorized(
        session: Session, agency_id: int, user_account_id: int
    ) -> None:
        user = UserAccountInterface.get_user_by_id(
            session=session, user_account_id=user_account_id
        )
        if not agency_id in {a.id for a in user.agencies}:
            raise JusticeCountsPermissionError(
                code="justice_counts_agency_permission",
                description=f"User (user_id: {user_account_id}) does not have access to reports from current agency (agency_id: {agency_id}).",
            )

    @staticmethod
    def get_reports_by_agency_id(
        session: Session, agency_id: int, user_account_id: int
    ) -> List[schema.Report]:
        ReportInterface._raise_if_user_is_unauthorized(
            session=session, agency_id=agency_id, user_account_id=user_account_id
        )
        return (
            session.query(schema.Report)
            .filter(schema.Report.source_id == agency_id)
            .order_by(schema.Report.date_range_end.desc())
            .all()
        )

    @staticmethod
    def _get_report_instance(
        report_type: str,
        date_range_start: datetime.date,
    ) -> str:
        if report_type == ReportingFrequency.MONTHLY.value:
            month = date_range_start.strftime("%m")
            return f"{month} {str(date_range_start.year)} Metrics"
        return f"{str(date_range_start.year)} Annual Metrics"

    @staticmethod
    def create_report(
        session: Session,
        agency_id: int,
        user_account_id: int,
        year: int,
        month: int,
        frequency: str,
    ) -> schema.Report:
        """Creates empty report in Justice Counts DB"""
        ReportInterface._raise_if_user_is_unauthorized(
            session=session, agency_id=agency_id, user_account_id=user_account_id
        )
        report_type = (
            ReportingFrequency.MONTHLY.value
            if frequency == ReportingFrequency.MONTHLY.value
            else ReportingFrequency.ANNUAL.value
        )
        date_range_start = datetime.date(year, month, 1)
        date_range_end = (
            datetime.date(year, month + 1, 1)
            if frequency == ReportingFrequency.MONTHLY.value
            else datetime.date(year + 1, month, 1)
        )
        report = schema.Report(
            source_id=agency_id,
            type=report_type,
            instance=ReportInterface._get_report_instance(
                report_type=report_type, date_range_start=date_range_start
            ),
            created_at=datetime.date.today(),
            acquisition_method=schema.AcquisitionMethod.CONTROL_PANEL,
            project=schema.Project.JUSTICE_COUNTS_CONTROL_PANEL,
            status=schema.ReportStatus.NOT_STARTED,
            date_range_start=date_range_start,
            date_range_end=date_range_end,
        )
        session.add(report)
        session.commit()
        return report

    @staticmethod
    def to_json_response(session: Session, report: schema.Report) -> Dict[str, Any]:
        editor_names = [
            UserAccountInterface.get_user_by_id(
                session=session, user_account_id=id
            ).name
            for id in report.modified_by or []
        ]
        reporting_frequency = report.get_reporting_frequency()
        return {
            "id": report.id,
            "year": report.date_range_start.year,
            "month": report.date_range_start.month
            if reporting_frequency == ReportingFrequency.MONTHLY
            else None,
            "frequency": reporting_frequency.value,
            "last_modified_at": report.last_modified_at,
            "editors": editor_names,
            "status": report.status.value,
        }

    @staticmethod
    def add_or_update_metric(
        session: Session, report: schema.Report, reported_metric: ReportedMetric
    ) -> None:
        """Given a Report and a ReportedMetric, either add this metric
        to the report, or if the metric already exists on the report,
        update the existing metric in-place.

        Adding (or updating) a metric to a report actually involves
        adding (or updating) several different database objects:
            - ReportTableDefinition that defines the aggregated metric
              (e.g. total arrests)
            - ReportTableInstance that defines the time period for the
              aggregated metric
              (e.g. total arrests between Jan and Feb)
            - Cell that contains the value for the aggregated metric
              over that time period
              (e.g. 1000 total arrests between Jan and Feb)
            - ReportTableDefinition that defines the disaggregated metric
              (e.g. arrests broken down by gender)
            - ReportTableInstance that defines the time period for the
              disaggregated metric
              (e.g. arrests broken down by gender between Jan and Feb)
            - Cells that contain the values for the disaggregated metric
              (e.g. 100 arrests of men, 23 arrests of women, etc,
              each in their own Cell object)
        """

        # First, define a ReportTableDefinition + Instance + Cells for the
        # aggregate metric value (summed across all dimensions).
        report_table_definition = (
            ReportTableDefinitionInterface.create_or_update_from_reported_metric(
                session=session, reported_metric=reported_metric
            )
        )
        # TODO(#12051): Enable updating of reported metrics
        ReportTableInstanceInterface.create_from_reported_metric(
            session=session,
            report=report,
            report_table_definition=report_table_definition,
            reported_metric=reported_metric,
        )

        # Next, define a ReportTableDefinition + Instance + Cells for
        # each disaggregated dimension of the metric.
        for dimension in reported_metric.aggregated_dimensions or []:
            report_table_definition = (
                ReportTableDefinitionInterface.create_or_update_from_reported_metric(
                    session=session,
                    reported_metric=reported_metric,
                    aggregated_dimension=dimension,
                )
            )
            ReportTableInstanceInterface.create_from_reported_metric(
                session=session,
                report=report,
                report_table_definition=report_table_definition,
                reported_metric=reported_metric,
                aggregated_dimension=dimension,
            )
