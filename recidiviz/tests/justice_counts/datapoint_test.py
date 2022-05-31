# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""This class implements tests for the Justice Counts DatapointInterface."""


import datetime

from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.exceptions import JusticeCountsDataError
from recidiviz.justice_counts.metrics import law_enforcement
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.persistence.database.schema.justice_counts.schema import (
    Report,
    ReportStatus,
    UserAccount,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)


class TestReportInterface(JusticeCountsDatabaseTestCase):
    """Implements tests for the UserAccountInterface."""

    def setUp(self) -> None:
        super().setUp()
        self.test_schema_objects = JusticeCountsSchemaTestObjects()

    def test_save_invalid_dataopint(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            monthly_report = self.test_schema_objects.test_report_monthly
            user = self.test_schema_objects.test_user_A
            session.add_all([monthly_report, user])
            current_time = datetime.datetime.utcnow()

            try:
                # When a report is in Draft mode, no errors are raised when the value is invalid
                DatapointInterface.add_datapoint(
                    session=session,
                    report=monthly_report,
                    value="123abc",
                    user_account=user,
                    metric_definition_key=law_enforcement.annual_budget.key,
                    current_time=current_time,
                )
                assert True
            except ValueError:
                assert False

            with self.assertRaises(JusticeCountsDataError):
                # When a report is Published, an errors is raised when the value is invalid
                monthly_report = session.query(Report).one()
                user = session.query(UserAccount).one()
                ReportInterface.update_report_metadata(
                    session=session,
                    report_id=monthly_report.id,
                    editor_id=user.id,
                    status=ReportStatus.PUBLISHED.value,
                )
                DatapointInterface.add_datapoint(
                    session=session,
                    report=monthly_report,
                    value="123abc",
                    user_account=user,
                    metric_definition_key=law_enforcement.annual_budget.key,
                    current_time=current_time,
                )
