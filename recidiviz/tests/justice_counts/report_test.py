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
"""This class implements tests for the Justice Counts ReportInterface."""

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
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

    def test_get_reports(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            monthly_report = self.test_schema_objects.test_report_monthly
            annual_report = self.test_schema_objects.test_report_annual
            monthly_report_id = monthly_report.id
            annual_report_id = annual_report.id
            for agency_name in ["Agency Alpha", "Agency Beta"]:
                AgencyInterface.create_agency(session=session, name=agency_name)
            for user in [
                self.test_schema_objects.test_user_A,
                self.test_schema_objects.test_user_B,
            ]:
                UserAccountInterface.create_or_update_user(
                    session=session,
                    email_address=user.email_address,
                    name=user.name,
                    auth0_user_id=user.auth0_user_id,
                    agency_ids=[a.id for a in user.agencies],
                )

            session.add_all([monthly_report, annual_report])

        with SessionFactory.using_database(self.database_key) as session:
            reports_agency_A = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=1,
                user_account_id=1,
            )
            self.assertEqual(len(reports_agency_A), 1)
            self.assertEqual(reports_agency_A[0].id, monthly_report_id)
            reports_agency_B = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=2,
                user_account_id=2,
            )
            self.assertEqual(len(reports_agency_B), 1)
            self.assertEqual(reports_agency_B[0].id, annual_report_id)
