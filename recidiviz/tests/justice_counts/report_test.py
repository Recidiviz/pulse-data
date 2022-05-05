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
from recidiviz.justice_counts.dimensions.corrections import PopulationType
from recidiviz.justice_counts.dimensions.location import Agency, County, State
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

            session.flush()
            session.refresh(self.test_schema_objects.test_agency_A)
            session.refresh(self.test_schema_objects.test_agency_A)

            user_id = self.test_schema_objects.test_agency_A.id
            agency_id = self.test_schema_objects.test_agency_A.id

            new_monthly_report = ReportInterface.create_report(
                session=session,
                agency_id=agency_id,
                user_account_id=user_id,
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
                user_account_id=user_id,
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

    def test_add_budget_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.reported_budget_metric,
                user_account=self.test_schema_objects.test_user_A,
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
                "LAW_ENFORCEMENT_BUDGET__metric/law_enforcement/budget/type",
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
            self.assertEqual(disaggregated_cells[0].value, 33334)
            self.assertEqual(
                disaggregated_cells[1].aggregated_dimension_values, ["DETENTION"]
            )
            self.assertEqual(disaggregated_cells[1].value, 66666)

            # The aggregated instance should have one context
            contexts = queried_instances[0].contexts
            self.assertEqual(len(contexts), 1)
            self.assertEqual(contexts[0].key, "PRIMARY_FUNDING_SOURCE")
            self.assertEqual(contexts[0].value, "government")

    def test_add_empty_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.get_reported_budget_metric(
                    value=None,
                    detention_value=None,
                    patrol_value=None,
                    include_contexts=False,
                ),
                user_account=self.test_schema_objects.test_user_A,
            )

            # Two ReportTableInstances should be saved, but both should be empty.
            queried_instances = (
                session.query(schema.ReportTableInstance)
                .order_by(schema.ReportTableInstance.id)
                .all()
            )
            self.assertEqual(len(queried_instances), 2)
            self.assertEqual(len(queried_instances[0].cells), 0)
            self.assertEqual(len(queried_instances[1].cells), 0)
            self.assertEqual(len(queried_instances[0].contexts), 0)
            self.assertEqual(len(queried_instances[1].contexts), 0)

    def test_add_incomplete_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.get_reported_budget_metric(
                    value=None,
                    detention_value=50,
                    patrol_value=None,
                ),
                user_account=self.test_schema_objects.test_user_A,
            )

            # Two ReportTableInstances should be saved, but the first should
            # only have a context attached, not any cells.
            queried_instances = (
                session.query(schema.ReportTableInstance)
                .order_by(schema.ReportTableInstance.id)
                .all()
            )
            self.assertEqual(len(queried_instances), 2)
            self.assertEqual(len(queried_instances[0].cells), 0)
            self.assertEqual(len(queried_instances[0].contexts), 1)
            self.assertEqual(len(queried_instances[1].cells), 1)
            self.assertEqual(queried_instances[1].cells[0].value, 50)

    def test_add_calls_for_service_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.reported_calls_for_service_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            # We should have two instances, one for the aggregated and one for disaggregated
            queried_instances = (
                session.query(schema.ReportTableInstance)
                .order_by(schema.ReportTableInstance.id)
                .all()
            )
            self.assertEqual(len(queried_instances), 2)

            # The aggregated instance should have one context
            contexts = queried_instances[0].contexts
            self.assertEqual(len(contexts), 1)
            self.assertEqual(contexts[0].key, "ALL_CALLS_OR_CALLS_RESPONDED")
            self.assertEqual(contexts[0].value, True)

    def test_add_population_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            report = self.test_schema_objects.test_report_monthly
            ReportInterface.add_or_update_metric(
                session=session,
                report=report,
                report_metric=self.test_schema_objects.reported_residents_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            # We should have two definitions, one for aggregated (total) residents
            # and one for residents disaggregated by race and ethnicity
            queried_definitions = (
                session.query(schema.ReportTableDefinition)
                .order_by(schema.ReportTableDefinition.id)
                .all()
            )
            self.assertEqual(len(queried_definitions), 2)

            # Both of these definitions should have the same filtered dimensions:
            # namely, filtered to the same location (country, state, and county)
            # and to PopulationType.RESIDENTS
            for definition in queried_definitions:
                filtered_dimensions = definition.filtered_dimensions
                filtered_dimension_values = definition.filtered_dimension_values
                self.assertEqual(
                    filtered_dimensions,
                    [
                        PopulationType.dimension_identifier(),
                        State.dimension_identifier(),
                        County.dimension_identifier(),
                        Agency.dimension_identifier(),
                    ],
                )
                self.assertEqual(
                    filtered_dimension_values,
                    [
                        PopulationType.RESIDENTS.value,
                        report.source.state_code,
                        report.source.fips_county_code,
                        report.source.name,
                    ],
                )

    def test_update_metric_no_change(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.reported_budget_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            # This should be a no-op, because the metric definition is the same
            # according to our unique constraints, so we update the existing records
            # (which does nothing, since nothing has changed)
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.reported_budget_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            queried_definitions = session.query(schema.ReportTableDefinition).all()
            self.assertEqual(len(queried_definitions), 2)

            queried_instances = session.query(schema.ReportTableInstance).all()
            self.assertEqual(len(queried_instances), 2)

    def test_update_metric_with_new_values(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.reported_budget_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            # This should result in an update to the existing database objects
            report_metric = JusticeCountsSchemaTestObjects.get_reported_budget_metric(
                value=1000,
                detention_value=600,
                patrol_value=400,
            )
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=report_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            queried_instances = (
                session.query(schema.ReportTableInstance)
                .order_by(schema.ReportTableInstance.id)
                .all()
            )
            self.assertEqual(len(queried_instances), 2)

            aggregate_cells = queried_instances[0].cells
            self.assertEqual(len(aggregate_cells), 1)
            self.assertEqual(aggregate_cells[0].value, 1000)

            disaggregated_cells = sorted(
                queried_instances[1].cells, key=lambda x: x.value
            )
            self.assertEqual(len(disaggregated_cells), 2)
            self.assertEqual(
                disaggregated_cells[0].aggregated_dimension_values, ["PATROL"]
            )
            self.assertEqual(disaggregated_cells[0].value, 400)
            self.assertEqual(
                disaggregated_cells[1].aggregated_dimension_values, ["DETENTION"]
            )
            self.assertEqual(disaggregated_cells[1].value, 600)

    def test_update_metric_with_new_contexts(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.reported_calls_for_service_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            # Add a new context
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.get_reported_calls_for_service_metric(
                    agencies_available_for_response="agency0"
                ),
                user_account=self.test_schema_objects.test_user_A,
            )
            queried_instances = (
                session.query(schema.ReportTableInstance)
                .order_by(schema.ReportTableInstance.id)
                .all()
            )

            # The aggregated instance should have two contexts
            contexts = sorted(queried_instances[0].contexts, key=lambda x: x.key)
            self.assertEqual(len(contexts), 2)
            self.assertEqual(contexts[0].value, "agency0")

            # Update a context
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.get_reported_calls_for_service_metric(
                    agencies_available_for_response="agency0, agency1"
                ),
                user_account=self.test_schema_objects.test_user_A,
            )
            queried_instances = (
                session.query(schema.ReportTableInstance)
                .order_by(schema.ReportTableInstance.id)
                .all()
            )

            contexts = sorted(queried_instances[0].contexts, key=lambda x: x.key)
            self.assertEqual(len(contexts), 2)
            self.assertEqual(contexts[0].value, "agency0, agency1")

    def test_remove_disaggregation_from_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.reported_budget_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            # User decides they don't want to report the disaggregation
            report_metric = JusticeCountsSchemaTestObjects.get_reported_budget_metric(
                include_disaggregations=False
            )
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=report_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            # Should only have one instance and cell in the db
            # (corresponding to the aggregated value)
            queried_instances = session.query(schema.ReportTableInstance).all()
            self.assertEqual(len(queried_instances), 1)

            queried_instances = session.query(schema.Cell).all()
            self.assertEqual(len(queried_instances), 1)

    def test_get_metrics_for_empty_report(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add_all(
                [
                    self.test_schema_objects.test_report_monthly,
                    self.test_schema_objects.test_user_A,
                ]
            )
            session.flush()
            report_id = self.test_schema_objects.test_report_monthly.id
            user_account_id = self.test_schema_objects.test_user_A.id
            metrics = sorted(
                ReportInterface.get_metrics_by_report_id(
                    session=session,
                    report_id=report_id,
                    user_account_id=user_account_id,
                ),
                key=lambda x: x.key,
            )
            calls_for_service = metrics[0]
            population = metrics[1]

            # Population metric should be blank
            self.assertEqual(population.value, None)

            # Calls for service metric should be blank
            self.assertEqual(calls_for_service.value, None)

    def test_get_metrics_for_nonempty_report(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add_all(
                [
                    self.test_schema_objects.test_report_monthly,
                    self.test_schema_objects.test_user_A,
                ]
            )
            session.flush()
            report_id = self.test_schema_objects.test_report_monthly.id
            user_account_id = self.test_schema_objects.test_user_A.id

            report = ReportInterface.get_report_by_id(
                session=session, report_id=report_id
            )
            ReportInterface.add_or_update_metric(
                session=session,
                report=report,
                report_metric=self.test_schema_objects.reported_calls_for_service_metric,
                user_account=self.test_schema_objects.test_user_A,
            )
            metrics = ReportInterface.get_metrics_by_report_id(
                session=session, report_id=report_id, user_account_id=user_account_id
            )
            self.assertEqual(len(metrics), 6)
            calls_for_service = [
                metric
                for metric in metrics
                if metric.key
                == self.test_schema_objects.reported_calls_for_service_metric.key
            ].pop()

            population = [
                metric
                for metric in metrics
                if metric.key == self.test_schema_objects.reported_residents_metric.key
            ].pop()

            self.assertIsNotNone(calls_for_service)
            self.assertIsNotNone(population)

            # Population metric should be blank
            self.assertEqual(population.value, None)

            # Calls for service metric should be populated
            self.assertEqual(
                calls_for_service.value,
                self.test_schema_objects.reported_calls_for_service_metric.value,
            )
            self.assertEqual(
                calls_for_service.aggregated_dimensions,
                self.test_schema_objects.reported_calls_for_service_metric.aggregated_dimensions,
            )

    def test_cell_histories(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.reported_budget_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            # First update
            report_metric = JusticeCountsSchemaTestObjects.get_reported_budget_metric(
                value=1000,
                detention_value=600,
                patrol_value=400,
            )
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=report_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            # Second update
            report_metric = JusticeCountsSchemaTestObjects.get_reported_budget_metric(
                value=100,
                detention_value=60,
                patrol_value=40,
            )
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=report_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            cell_histories = (
                session.query(schema.CellHistory)
                .order_by(schema.CellHistory.cell_id, schema.CellHistory.timestamp)
                .all()
            )
            self.assertEqual(len(cell_histories), 6)

            # Aggregated value goes from 100000 -> 1000 -> 100
            self.assertEqual(cell_histories[0].old_value, 100000)
            self.assertEqual(cell_histories[0].new_value, 1000)
            self.assertEqual(cell_histories[1].old_value, 1000)
            self.assertEqual(cell_histories[1].new_value, 100)

            # First disaggregated value goes from 66666 -> 600 -> 60
            self.assertEqual(cell_histories[2].old_value, 66666)
            self.assertEqual(cell_histories[2].new_value, 600)
            self.assertEqual(cell_histories[3].old_value, 600)
            self.assertEqual(cell_histories[3].new_value, 60)

            # Second disaggregated value goes from 33334 -> 400 -> 40
            self.assertEqual(cell_histories[4].old_value, 33334)
            self.assertEqual(cell_histories[4].new_value, 400)
            self.assertEqual(cell_histories[5].old_value, 400)
            self.assertEqual(cell_histories[5].new_value, 40)
