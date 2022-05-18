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

from freezegun import freeze_time

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.dimensions.law_enforcement import (
    CallType,
    SheriffBudgetType,
)
from recidiviz.justice_counts.dimensions.person import RaceAndEthnicity
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)
from recidiviz.utils.types import assert_type


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
            agency_A = AgencyInterface.get_agency_by_name(
                session=session, name="Agency Alpha"
            )
            reports_agency_A = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=agency_A.id,
            )
            self.assertEqual(reports_agency_A[0].source_id, agency_A.id)
            agency_B = AgencyInterface.get_agency_by_name(
                session=session, name="Agency Beta"
            )
            reports_agency_B = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=agency_B.id,
            )
            self.assertEqual(reports_agency_B[0].source_id, agency_B.id)

    def test_create_report(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add_all(
                [
                    self.test_schema_objects.test_agency_A,
                    self.test_schema_objects.test_user_A,
                ]
            )

            session.flush()
            session.refresh(self.test_schema_objects.test_agency_A)
            session.refresh(self.test_schema_objects.test_user_A)

            user_id = UserAccountInterface.get_user_by_auth0_user_id(
                session=session,
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
            ).id
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
            self.assertEqual(new_monthly_report.modified_by, [user_id])
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

    def test_update_report_metadata(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add_all(
                [
                    self.test_schema_objects.test_agency_A,
                    self.test_schema_objects.test_user_A,
                    self.test_schema_objects.test_user_C,
                ]
            )
            session.commit()
            session.refresh(self.test_schema_objects.test_agency_A)

            agency_id = self.test_schema_objects.test_agency_A.id
            update_datetime = datetime.datetime(2022, 2, 1, 0, 0, 0)

            with freeze_time(update_datetime):
                user_a_id = UserAccountInterface.get_user_by_auth0_user_id(
                    session=session,
                    auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
                ).id
                report = ReportInterface.create_report(
                    session=session,
                    agency_id=agency_id,
                    user_account_id=user_a_id,
                    month=2,
                    year=2022,
                    frequency=schema.ReportingFrequency.MONTHLY.value,
                )
                self.assertEqual(report.status, schema.ReportStatus.NOT_STARTED)
                updated_report = ReportInterface.update_report_metadata(
                    session=session,
                    report_id=report.id,
                    editor_id=user_a_id,
                    status=schema.ReportStatus.DRAFT.value,
                )
                self.assertEqual(updated_report.status, schema.ReportStatus.DRAFT)
                self.assertEqual(updated_report.modified_by, [user_a_id])
                self.assertEqual(updated_report.last_modified_at, update_datetime)
                user_c_id = UserAccountInterface.get_user_by_auth0_user_id(
                    session=session,
                    auth0_user_id=self.test_schema_objects.test_user_C.auth0_user_id,
                ).id
                updated_report = ReportInterface.update_report_metadata(
                    session=session,
                    report_id=report.id,
                    editor_id=user_c_id,
                    status=schema.ReportStatus.DRAFT.value,
                )
                self.assertEqual(updated_report.status, schema.ReportStatus.DRAFT)
                self.assertEqual(updated_report.modified_by, [user_a_id, user_c_id])
                updated_report = ReportInterface.update_report_metadata(
                    session=session,
                    report_id=report.id,
                    status=schema.ReportStatus.PUBLISHED.value,
                    editor_id=user_a_id,
                )
                self.assertEqual(updated_report.status, schema.ReportStatus.PUBLISHED)
                self.assertEqual(
                    updated_report.modified_by,
                    [user_c_id, user_a_id],
                )

    def test_add_budget_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.reported_budget_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            # We should have four datapoints, one for the aggregated Law Enforcement budget
            # two for each breakdown (DETENTION and PATROL) and one for the context.
            queried_datapoints = session.query(schema.Datapoint).all()
            self.assertEqual(len(queried_datapoints), 4)
            report_metric = self.test_schema_objects.reported_budget_metric
            self.assertEqual(
                queried_datapoints[0].get_value(),
                report_metric.value,
            )
            aggregated_dimensions = assert_type(
                report_metric.aggregated_dimensions, list
            )
            self.assertEqual(
                queried_datapoints[1].get_value(),
                aggregated_dimensions[0].dimension_to_value[
                    SheriffBudgetType.DETENTION
                ],
            )
            self.assertEqual(
                queried_datapoints[2].get_value(),
                aggregated_dimensions[0].dimension_to_value[SheriffBudgetType.PATROL],
            )
            self.assertEqual(
                queried_datapoints[3].get_value(),
                assert_type(report_metric.contexts, list)[0].value,
            )

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

            # We should have no datapoints
            queried_datapoints = (
                session.query(schema.Datapoint).order_by(schema.Datapoint.id).all()
            )
            self.assertEqual(len(queried_datapoints), 0)

    def test_add_incomplete_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            incomplete_report = self.test_schema_objects.get_reported_budget_metric(
                value=None,
                detention_value=50,
                patrol_value=None,
            )

            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=incomplete_report,
                user_account=self.test_schema_objects.test_user_A,
            )

            # There should be two datapoints, one containing the detention value, the other with the context
            queried_datapoints = session.query(schema.Datapoint).all()

            self.assertEqual(len(queried_datapoints), 2)
            reported_aggregated_dimensions = assert_type(
                incomplete_report.aggregated_dimensions, list
            )
            self.assertEqual(
                queried_datapoints[0].get_value(),
                reported_aggregated_dimensions[0].dimension_to_value[
                    SheriffBudgetType.DETENTION
                ],
            )
            self.assertEqual(
                queried_datapoints[1].get_value(),
                assert_type(incomplete_report.contexts, list)[0].value,
            )

    def test_add_calls_for_service_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            report_metric = self.test_schema_objects.reported_calls_for_service_metric
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=report_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            # We should have five datapoints, one aggregate value, three breakdowns, and one context.
            queried_datapoints = session.query(schema.Datapoint).all()

            self.assertEqual(len(queried_datapoints), 5)

            self.assertEqual(
                queried_datapoints[0].get_value(),
                report_metric.value,
            )
            aggregated_dimensions = assert_type(
                report_metric.aggregated_dimensions, list
            )
            self.assertEqual(
                queried_datapoints[1].get_value(),
                aggregated_dimensions[0].dimension_to_value[CallType.EMERGENCY],
            )
            self.assertEqual(
                queried_datapoints[2].get_value(),
                aggregated_dimensions[0].dimension_to_value[CallType.NON_EMERGENCY],
            )
            self.assertEqual(
                queried_datapoints[3].get_value(),
                aggregated_dimensions[0].dimension_to_value[CallType.UNKNOWN],
            )
            self.assertEqual(
                queried_datapoints[4].get_value(),
                assert_type(report_metric.contexts, list)[0].value,
            )

    def test_add_population_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            report = self.test_schema_objects.test_report_monthly
            report_metric = self.test_schema_objects.reported_residents_metric
            ReportInterface.add_or_update_metric(
                session=session,
                report=report,
                report_metric=report_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            # We should have nine datapoints, one for aggregated (total) residents
            # and eight for each race and ethnicity
            queried_datapoints = (
                session.query(schema.Datapoint).order_by(schema.Datapoint.id).all()
            )

            self.assertEqual(len(queried_datapoints), 9)

            self.assertEqual(
                queried_datapoints[0].get_value(),
                report_metric.value,
            )
            aggregated_dimensions = assert_type(
                report_metric.aggregated_dimensions, list
            )
            self.assertEqual(
                queried_datapoints[1].get_value(),
                aggregated_dimensions[0].dimension_to_value[
                    RaceAndEthnicity.AMERICAN_INDIAN_ALASKAN_NATIVE
                ],
            )
            self.assertEqual(
                queried_datapoints[2].get_value(),
                aggregated_dimensions[0].dimension_to_value[RaceAndEthnicity.ASIAN],
            )
            self.assertEqual(
                queried_datapoints[3].get_value(),
                aggregated_dimensions[0].dimension_to_value[RaceAndEthnicity.BLACK],
            )
            self.assertEqual(
                queried_datapoints[3].get_value(),
                aggregated_dimensions[0].dimension_to_value[RaceAndEthnicity.BLACK],
            )
            self.assertEqual(
                queried_datapoints[4].get_value(),
                aggregated_dimensions[0].dimension_to_value[
                    RaceAndEthnicity.EXTERNAL_UNKNOWN
                ],
            )
            self.assertEqual(
                queried_datapoints[5].get_value(),
                aggregated_dimensions[0].dimension_to_value[RaceAndEthnicity.HISPANIC],
            )
            self.assertEqual(
                queried_datapoints[6].get_value(),
                aggregated_dimensions[0].dimension_to_value[
                    RaceAndEthnicity.NATIVE_HAWAIIAN_PACIFIC_ISLANDER
                ],
            )
            self.assertEqual(
                queried_datapoints[7].get_value(),
                aggregated_dimensions[0].dimension_to_value[RaceAndEthnicity.OTHER],
            )
            self.assertEqual(
                queried_datapoints[8].get_value(),
                aggregated_dimensions[0].dimension_to_value[RaceAndEthnicity.WHITE],
            )

    def test_update_metric_no_change(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            report = self.test_schema_objects.test_report_monthly
            session.add(report)
            report = session.query(schema.Report).one_or_none()
            ReportInterface.add_or_update_metric(
                session=session,
                report=report,
                report_metric=self.test_schema_objects.reported_budget_metric,
                user_account=self.test_schema_objects.test_user_A,
            )
            queried_datapoints = (
                session.query(schema.Datapoint).order_by(schema.Datapoint.id).all()
            )

            self.assertEqual(len(queried_datapoints), 4)
            # This shoulxd be a no-op, because the metric definition is the same
            # according to our unique constraints, so we update the existing records
            # (which does nothing, since nothing has changed)
            ReportInterface.add_or_update_metric(
                session=session,
                report=report,
                report_metric=self.test_schema_objects.reported_budget_metric,
                user_account=self.test_schema_objects.test_user_A,
            )
            queried_datapoints_no_op = (
                session.query(schema.Datapoint).order_by(schema.Datapoint.id).all()
            )
            self.assertEqual(len(queried_datapoints_no_op), 4)
            self.assertAlmostEqual(queried_datapoints, queried_datapoints_no_op)

    def test_update_metric_with_new_values(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(self.test_schema_objects.test_user_A)
            report_metric = self.test_schema_objects.reported_budget_metric
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=report_metric,
                user_account=self.test_schema_objects.test_user_A,
            )
            queried_datapoints = (
                session.query(schema.Datapoint).order_by(schema.Datapoint.id).all()
            )

            self.assertEqual(len(queried_datapoints), 4)
            aggregated_dimensions = assert_type(
                report_metric.aggregated_dimensions, list
            )
            self.assertEqual(
                queried_datapoints[0].get_value(),
                report_metric.value,
            )
            self.assertEqual(
                queried_datapoints[1].get_value(),
                aggregated_dimensions[0].dimension_to_value[
                    SheriffBudgetType.DETENTION
                ],
            )
            self.assertEqual(
                queried_datapoints[2].get_value(),
                aggregated_dimensions[0].dimension_to_value[SheriffBudgetType.PATROL],
            )
            self.assertEqual(
                queried_datapoints[3].get_value(),
                assert_type(report_metric.contexts, list)[0].value,
            )
            # This should result in an update to the existing database objects
            new_report_metric = (
                JusticeCountsSchemaTestObjects.get_reported_budget_metric(
                    value=1000,
                    detention_value=600,
                    patrol_value=400,
                )
            )
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=new_report_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            queried_datapoints = (
                session.query(schema.Datapoint).order_by(schema.Datapoint.id).all()
            )

            self.assertEqual(len(queried_datapoints), 4)

            self.assertEqual(
                queried_datapoints[0].get_value(),
                new_report_metric.value,
            )
            aggregated_dimensions = assert_type(
                new_report_metric.aggregated_dimensions, list
            )
            self.assertEqual(
                queried_datapoints[1].get_value(),
                aggregated_dimensions[0].dimension_to_value[
                    SheriffBudgetType.DETENTION
                ],
            )
            self.assertEqual(
                queried_datapoints[2].get_value(),
                aggregated_dimensions[0].dimension_to_value[SheriffBudgetType.PATROL],
            )
            self.assertEqual(
                queried_datapoints[3].get_value(),
                assert_type(new_report_metric.contexts, list)[0].value,
            )

    def test_update_metric_with_new_contexts(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(self.test_schema_objects.test_user_A)
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
            queried_datapoints = (
                session.query(schema.Datapoint).order_by(schema.Datapoint.id).all()
            )

            self.assertEqual(len(queried_datapoints), 6)

            # There should be two contexts associated with the metric
            contexts = [d for d in queried_datapoints if d.context_key is not None]
            self.assertEqual(len(contexts), 2)

            # Update a context
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.get_reported_calls_for_service_metric(
                    agencies_available_for_response="agency0, agency1"
                ),
                user_account=self.test_schema_objects.test_user_A,
            )
            queried_datapoints = (
                session.query(schema.Datapoint).order_by(schema.Datapoint.id).all()
            )

            contexts = [d for d in queried_datapoints if d.context_key is not None]
            self.assertEqual(len(contexts), 2)
            self.assertEqual(contexts[0].get_value(), True)
            self.assertEqual(contexts[1].get_value(), "agency0, agency1")

    def test_remove_disaggregation_from_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            report = self.test_schema_objects.test_report_monthly
            session.add(report)
            report_with_id = session.query(schema.Report).one()
            ReportInterface.add_or_update_metric(
                session=session,
                report=report_with_id,
                report_metric=self.test_schema_objects.reported_budget_metric,
                user_account=self.test_schema_objects.test_user_A,
            )
            queried_datapoints = session.query(schema.Datapoint).all()
            self.assertEqual(len(queried_datapoints), 4)
            # User decides they don't want to report the disaggregation
            report_metric = JusticeCountsSchemaTestObjects.get_reported_budget_metric(
                include_disaggregations=False
            )
            ReportInterface.add_or_update_metric(
                session=session,
                report=report_with_id,
                report_metric=report_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            # Should only have one datapoint for aggregated value and one datapoint for context
            # (corresponding to the aggregated value)
            queried_datapoints = session.query(schema.Datapoint).all()
            self.assertEqual(len(queried_datapoints), 2)
            self.assertEqual(queried_datapoints[0].get_value(), report_metric.value)
            self.assertEqual(
                queried_datapoints[1].get_value(),
                assert_type(report_metric.contexts, list)[0].value,
            )

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
            metrics = sorted(
                ReportInterface.get_metrics_by_report_id(
                    session=session,
                    report_id=report_id,
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
                session=session, report_id=report_id
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

    def test_datapoint_histories(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(self.test_schema_objects.test_user_A)
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

            datapoint_history = (
                session.query(schema.DatapointHistory)
                .order_by(
                    schema.DatapointHistory.datapoint_id,
                    schema.DatapointHistory.timestamp,
                )
                .all()
            )
            self.assertEqual(len(datapoint_history), 6)

            # Aggregated value goes from 100000 -> 1000 -> 100
            self.assertEqual(datapoint_history[0].old_value, str(100000))
            self.assertEqual(datapoint_history[0].new_value, str(1000))
            self.assertEqual(datapoint_history[1].old_value, str(1000))
            self.assertEqual(datapoint_history[1].new_value, str(100))

            # First disaggregated value goes from 66666 -> 600 -> 60
            self.assertEqual(datapoint_history[2].old_value, str(66666))
            self.assertEqual(datapoint_history[2].new_value, str(600))
            self.assertEqual(datapoint_history[3].old_value, str(600))
            self.assertEqual(datapoint_history[3].new_value, str(60))

            # Second disaggregated value goes from 33334 -> 400 -> 40
            self.assertEqual(datapoint_history[4].old_value, str(33334))
            self.assertEqual(datapoint_history[4].new_value, str(400))
            self.assertEqual(datapoint_history[5].old_value, str(400))
            self.assertEqual(datapoint_history[5].new_value, str(40))
