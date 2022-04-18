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


import datetime

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts import schema
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
            session.add_all(
                [
                    monthly_report,
                    annual_report,
                ]
            )

        with SessionFactory.using_database(self.database_key) as session:
            user_A = UserAccountInterface.get_user_by_auth0_user_id(
                session=session, auth0_user_id="auth0_id_A"
            )
            agency_A = AgencyInterface.get_agency_by_name(
                session=session, name="Agency Alpha"
            )
            reports_agency_A = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=agency_A.id,
                user_account_id=user_A.id,
            )
            self.assertEqual(reports_agency_A[0].source_id, agency_A.id)
            user_B = UserAccountInterface.get_user_by_auth0_user_id(
                session=session, auth0_user_id="auth0_id_B"
            )
            agency_B = AgencyInterface.get_agency_by_name(
                session=session, name="Agency Beta"
            )
            reports_agency_B = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=agency_B.id,
                user_account_id=user_B.id,
            )
            self.assertEqual(reports_agency_B[0].source_id, agency_B.id)

    def test_create_report(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(
                self.test_schema_objects.test_agency_A,
                self.test_schema_objects.test_user_A,
            )

        with SessionFactory.using_database(self.database_key) as session:
            agency_id = 1
            new_monthly_report = ReportInterface.create_report(
                session=session,
                agency_id=agency_id,
                user_account_id=1,
                month=2,
                year=2022,
                frequency=schema.ReportingFrequency.MONTHLY.value,
            )
            self.assertEqual(new_monthly_report.source_id, agency_id)
            self.assertEqual(
                new_monthly_report.type, schema.ReportingFrequency.MONTHLY.value
            )
            self.assertEqual(
                new_monthly_report.date_range_start, datetime.date(2022, 2, 1)
            )
            self.assertEqual(
                new_monthly_report.date_range_end, datetime.date(2022, 3, 1)
            )
            new_annual_report = ReportInterface.create_report(
                session=session,
                agency_id=agency_id,
                user_account_id=1,
                year=2022,
                month=3,
                frequency=schema.ReportingFrequency.ANNUAL.value,
            )
            self.assertEqual(new_annual_report.source_id, agency_id)
            self.assertEqual(
                new_annual_report.type, schema.ReportingFrequency.ANNUAL.value
            )
            self.assertEqual(
                new_annual_report.date_range_start, datetime.date(2022, 3, 1)
            )
            self.assertEqual(
                new_annual_report.date_range_end, datetime.date(2023, 3, 1)
            )

    def test_add_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                reported_metric=self.test_schema_objects.reported_budget_metric,
            )

            # We should have two definitions, one for the aggregated Law Enforcement budget
            # and one for the Law Enforcement budget disaggregated by budget type
            queried_definitions = (
                session.query(schema.ReportTableDefinition)
                .order_by(schema.ReportTableDefinition.id)
                .all()
            )
            self.assertEqual(len(queried_definitions), 2)
            self.assertEqual(
                queried_definitions[0].label,
                "LAW_ENFORCEMENT_BUDGET__metric/law_enforcement/budget/type_AGGREGATED",
            )
            self.assertEqual(
                queried_definitions[1].label,
                "LAW_ENFORCEMENT_BUDGET__metric/law_enforcement/budget/type",
            )

            # We should have two instances, one for the aggregated Law Enforcement budget
            # and one for the Law Enforcement budget disaggregated by budget type
            queried_instances = (
                session.query(schema.ReportTableInstance)
                .order_by(schema.ReportTableInstance.id)
                .all()
            )
            self.assertEqual(len(queried_instances), 2)

            # The aggregated instance should have one cell with the total budget value
            aggregate_cells = queried_instances[0].cells
            self.assertEqual(len(aggregate_cells), 1)
            self.assertEqual(aggregate_cells[0].value, 100000)

            # The disaggregated instance should have two cells, one with the budget
            # value for each type
            disaggregated_cells = sorted(
                queried_instances[1].cells, key=lambda x: x.value
            )
            self.assertEqual(len(disaggregated_cells), 2)
            self.assertEqual(
                disaggregated_cells[0].aggregated_dimension_values, ["PATROL"]
            )
            self.assertEqual(disaggregated_cells[0].value, 40000)
            self.assertEqual(
                disaggregated_cells[1].aggregated_dimension_values, ["DETENTION"]
            )
            self.assertEqual(disaggregated_cells[1].value, 60000)
