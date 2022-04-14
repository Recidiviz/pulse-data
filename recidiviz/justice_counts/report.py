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
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts.schema import (
    AcquisitionMethod,
    Project,
    Report,
    ReportStatus,
)


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
    ) -> List[Report]:
        ReportInterface._raise_if_user_is_unauthorized(
            session=session, agency_id=agency_id, user_account_id=user_account_id
        )
        return (
            session.query(Report)
            .filter(Report.source_id == agency_id)
            .order_by(Report.date_range_end.desc())
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
    ) -> Report:
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
        report = Report(
            source_id=agency_id,
            type=report_type,
            instance=ReportInterface._get_report_instance(
                report_type=report_type, date_range_start=date_range_start
            ),
            created_at=datetime.date.today(),
            acquisition_method=AcquisitionMethod.CONTROL_PANEL,
            project=Project.JUSTICE_COUNTS_CONTROL_PANEL,
            status=ReportStatus.NOT_STARTED,
            date_range_start=date_range_start,
            date_range_end=date_range_end,
        )
        session.add(report)
        session.commit()
        return report

    @staticmethod
    def to_json_response(session: Session, report: Report) -> Dict[str, Any]:
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
