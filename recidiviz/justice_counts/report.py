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
from typing import Any, Dict, List

from sqlalchemy.orm import Session

from recidiviz.justice_counts.exceptions import JusticeCountsPermissionError
from recidiviz.justice_counts.metrics.metric_definition import ReportingFrequency
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts.schema import Report


class ReportInterface:
    """Contains methods for setting and getting Report info."""

    @staticmethod
    def get_reports_by_agency_id(
        session: Session, agency_id: int, user_account_id: int
    ) -> List[Report]:
        user = UserAccountInterface.get_user_by_id(
            session=session, user_account_id=user_account_id
        )
        if not agency_id in {a.id for a in user.agencies}:
            raise JusticeCountsPermissionError(
                code="justice_counts_agency_permission",
                description=f"User (user_id: {user_account_id}) does not have access to reports from current agency (agency_id: {agency_id}).",
            )

        return (
            session.query(Report)
            .filter(Report.source_id == agency_id)
            .order_by(Report.date_range_end.desc())
            .all()
        )

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
