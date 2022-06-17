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
from recidiviz.justice_counts.dimensions.dimension_registry import (
    DIMENSION_IDENTIFIER_TO_DIMENSION,
)
from recidiviz.justice_counts.dimensions.law_enforcement import (
    CallType,
    OffenseType,
    SheriffBudgetType,
)
from recidiviz.justice_counts.dimensions.person import RaceAndEthnicity
from recidiviz.justice_counts.metrics import law_enforcement
from recidiviz.justice_counts.metrics.report_metric import (
    ReportedAggregatedDimension,
    ReportedContext,
)
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

    def test_report_dates(self) -> None:
        # November, Monthly
        report = ReportInterface.create_report_object(
            agency_id=0,
            user_account_id=0,
            year=2022,
            month=11,
            frequency=schema.ReportingFrequency.MONTHLY.value,
        )
        self.assertEqual(report.date_range_end, datetime.date(2022, 12, 1))
        self.assertEqual(
            report.get_reporting_frequency(), schema.ReportingFrequency.MONTHLY
        )

        # December, Monthly
        report = ReportInterface.create_report_object(
            agency_id=0,
            user_account_id=0,
            year=2022,
            month=12,
            frequency=schema.ReportingFrequency.MONTHLY.value,
        )
        self.assertEqual(report.date_range_end, datetime.date(2023, 1, 1))
        self.assertEqual(
            report.get_reporting_frequency(), schema.ReportingFrequency.MONTHLY
        )

        # January, Annually
        report = ReportInterface.create_report_object(
            agency_id=0,
            user_account_id=0,
            year=2022,
            month=1,
            frequency=schema.ReportingFrequency.ANNUAL.value,
        )
        self.assertEqual(report.date_range_end, datetime.date(2023, 1, 1))
        self.assertEqual(
            report.get_reporting_frequency(), schema.ReportingFrequency.ANNUAL
        )

        # July, Annually
        report = ReportInterface.create_report_object(
            agency_id=0,
            user_account_id=0,
            year=2022,
            month=7,
            frequency=schema.ReportingFrequency.ANNUAL.value,
        )
        self.assertEqual(report.date_range_end, datetime.date(2023, 7, 1))
        self.assertEqual(
            report.get_reporting_frequency(), schema.ReportingFrequency.ANNUAL
        )

    def test_create_recurring_report(self) -> None:
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

            recurring_report = ReportInterface.create_report(
                session=session,
                agency_id=agency_id,
                user_account_id=user_id,
                month=1,
                year=2022,
                frequency=schema.ReportingFrequency.MONTHLY.value,
                is_recurring=True,
            )

            self.assertEqual(recurring_report.is_recurring, True)
            self.assertIsNone(recurring_report.recurring_report)

            child_report = ReportInterface.create_report(
                session=session,
                agency_id=agency_id,
                user_account_id=user_id,
                month=2,
                year=2022,
                frequency=schema.ReportingFrequency.MONTHLY.value,
                is_recurring=False,
                recurring_report=recurring_report,
            )

            self.assertEqual(child_report.is_recurring, False)
            self.assertEqual(child_report.recurring_report, recurring_report)
            self.assertEqual(recurring_report.children, [child_report])

            child_report_2 = ReportInterface.create_report(
                session=session,
                agency_id=agency_id,
                user_account_id=user_id,
                month=3,
                year=2022,
                frequency=schema.ReportingFrequency.MONTHLY.value,
                is_recurring=False,
                recurring_report=recurring_report,
            )

            self.assertEqual(recurring_report.children, [child_report, child_report_2])

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

            update_datetime = datetime.datetime(2022, 2, 1, 1, 0, 0)
            with freeze_time(update_datetime):
                updated_report = ReportInterface.update_report_metadata(
                    session=session,
                    report_id=report.id,
                    status=schema.ReportStatus.PUBLISHED.value,
                    editor_id=user_a_id,
                )
                self.assertEqual(
                    updated_report.last_modified_at,
                    update_datetime,
                )

    def test_add_budget_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.reported_budget_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            total_datapoints = session.query(schema.Datapoint).all()
            self.assertEqual(len(total_datapoints), 5)

            # We should have four datapoints that have a value associated with them, one
            # for the aggregated Law Enforcement budget two for each breakdown (DETENTION and PATROL)
            # and one for the context.
            datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .order_by(schema.Datapoint.id)
                .all()
            )
            self.assertEqual(len(datapoints_with_value), 4)
            report_metric = self.test_schema_objects.reported_budget_metric
            self.assertEqual(
                datapoints_with_value[0].get_value(),
                report_metric.value,
            )
            aggregated_dimensions = assert_type(
                report_metric.aggregated_dimensions, list
            )
            self.assertEqual(
                datapoints_with_value[1].get_value(),
                aggregated_dimensions[0].dimension_to_value[SheriffBudgetType.PATROL],
            )
            self.assertEqual(
                datapoints_with_value[2].get_value(),
                aggregated_dimensions[0].dimension_to_value[
                    SheriffBudgetType.DETENTION
                ],
            )
            self.assertEqual(
                datapoints_with_value[3].get_value(),
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
            total_datapoints = session.query(schema.Datapoint).all()
            self.assertEqual(len(total_datapoints), 5)
            # There should be no datapoints with values associated with the metric
            queried_datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .all()
            )
            self.assertEqual(len(queried_datapoints_with_value), 0)
            queried_datapoints_without_value = session.query(schema.Datapoint).all()
            self.assertEqual(len(queried_datapoints_without_value), 5)

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

            total_datapoints = session.query(schema.Datapoint).all()
            self.assertEqual(len(total_datapoints), 5)

            # There should be two datapoints with non-null values: one containing the detention value,
            # the other with the context
            datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .all()
            )

            self.assertEqual(len(datapoints_with_value), 2)
            reported_aggregated_dimensions = assert_type(
                incomplete_report.aggregated_dimensions, list
            )
            self.assertEqual(
                datapoints_with_value[0].get_value(),
                reported_aggregated_dimensions[0].dimension_to_value[
                    SheriffBudgetType.DETENTION
                ],
            )
            self.assertEqual(
                datapoints_with_value[1].get_value(),
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

            total_datapoints = session.query(schema.Datapoint).all()
            self.assertEqual(len(total_datapoints), 7)
            # We should have five datapoints with non-null values:
            # one aggregate value, three breakdowns, and one context.
            datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .all()
            )

            self.assertEqual(len(datapoints_with_value), 5)

            self.assertEqual(
                datapoints_with_value[0].get_value(),
                report_metric.value,
            )
            aggregated_dimensions = assert_type(
                report_metric.aggregated_dimensions, list
            )
            self.assertEqual(
                datapoints_with_value[1].get_value(),
                aggregated_dimensions[0].dimension_to_value[CallType.EMERGENCY],
            )
            self.assertEqual(
                datapoints_with_value[2].get_value(),
                aggregated_dimensions[0].dimension_to_value[CallType.NON_EMERGENCY],
            )
            self.assertEqual(
                datapoints_with_value[3].get_value(),
                aggregated_dimensions[0].dimension_to_value[CallType.UNKNOWN],
            )
            self.assertEqual(
                datapoints_with_value[4].get_value(),
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

            total_datapoints = session.query(schema.Datapoint).all()
            self.assertEqual(len(total_datapoints), 16)

            # We should have nine datapoints with non-null values: one for aggregated (total) residents
            # and eight for each race and ethnicity
            datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .all()
            )

            self.assertEqual(len(datapoints_with_value), 9)

            self.assertEqual(
                datapoints_with_value[0].get_value(),
                report_metric.value,
            )
            aggregated_dimensions = assert_type(
                report_metric.aggregated_dimensions, list
            )
            self.assertEqual(
                datapoints_with_value[1].get_value(),
                aggregated_dimensions[0].dimension_to_value[
                    RaceAndEthnicity.AMERICAN_INDIAN_ALASKAN_NATIVE
                ],
            )
            self.assertEqual(
                datapoints_with_value[2].get_value(),
                aggregated_dimensions[0].dimension_to_value[RaceAndEthnicity.ASIAN],
            )
            self.assertEqual(
                datapoints_with_value[3].get_value(),
                aggregated_dimensions[0].dimension_to_value[RaceAndEthnicity.BLACK],
            )
            self.assertEqual(
                datapoints_with_value[3].get_value(),
                aggregated_dimensions[0].dimension_to_value[RaceAndEthnicity.BLACK],
            )
            self.assertEqual(
                datapoints_with_value[4].get_value(),
                aggregated_dimensions[0].dimension_to_value[
                    RaceAndEthnicity.EXTERNAL_UNKNOWN
                ],
            )
            self.assertEqual(
                datapoints_with_value[5].get_value(),
                aggregated_dimensions[0].dimension_to_value[RaceAndEthnicity.HISPANIC],
            )
            self.assertEqual(
                datapoints_with_value[6].get_value(),
                aggregated_dimensions[0].dimension_to_value[
                    RaceAndEthnicity.NATIVE_HAWAIIAN_PACIFIC_ISLANDER
                ],
            )
            self.assertEqual(
                datapoints_with_value[7].get_value(),
                aggregated_dimensions[0].dimension_to_value[RaceAndEthnicity.OTHER],
            )
            self.assertEqual(
                datapoints_with_value[8].get_value(),
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
            total_datapoints = session.query(schema.Datapoint).all()
            self.assertEqual(len(total_datapoints), 5)

            datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .all()
            )

            self.assertEqual(len(datapoints_with_value), 4)
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
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .order_by(schema.Datapoint.id)
                .all()
            )
            self.assertEqual(len(queried_datapoints_no_op), 4)
            self.assertAlmostEqual(datapoints_with_value, queried_datapoints_no_op)

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

            total_datapoints = session.query(schema.Datapoint).all()
            self.assertEqual(len(total_datapoints), 5)
            datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .all()
            )

            self.assertEqual(len(datapoints_with_value), 4)
            aggregated_dimensions = assert_type(
                report_metric.aggregated_dimensions, list
            )
            self.assertEqual(
                datapoints_with_value[0].get_value(),
                report_metric.value,
            )
            self.assertEqual(
                datapoints_with_value[1].get_value(),
                aggregated_dimensions[0].dimension_to_value[SheriffBudgetType.PATROL],
            )
            self.assertEqual(
                datapoints_with_value[2].get_value(),
                aggregated_dimensions[0].dimension_to_value[
                    SheriffBudgetType.DETENTION
                ],
            )
            self.assertEqual(
                datapoints_with_value[3].get_value(),
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

            datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .order_by(schema.Datapoint.id)
                .all()
            )

            self.assertEqual(len(datapoints_with_value), 4)

            self.assertEqual(
                datapoints_with_value[0].get_value(), new_report_metric.value
            )
            aggregated_dimensions = assert_type(
                new_report_metric.aggregated_dimensions, list
            )
            self.assertEqual(
                datapoints_with_value[1].get_value(),
                aggregated_dimensions[0].dimension_to_value[SheriffBudgetType.PATROL],
            )
            self.assertEqual(
                datapoints_with_value[2].get_value(),
                aggregated_dimensions[0].dimension_to_value[
                    SheriffBudgetType.DETENTION
                ],
            )
            self.assertEqual(
                datapoints_with_value[3].get_value(),
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
            total_datapoints = session.query(schema.Datapoint).all()
            self.assertEqual(len(total_datapoints), 7)

            # Add a new context
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.get_reported_calls_for_service_metric(
                    agencies_available_for_response="agency0"
                ),
                user_account=self.test_schema_objects.test_user_A,
            )
            datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .all()
            )
            self.assertEqual(len(datapoints_with_value), 6)

            # There should be two contexts associated with the metric
            contexts = [d for d in datapoints_with_value if d.context_key is not None]
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
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .all()
            )
            contexts = [d for d in queried_datapoints if d.context_key is not None]
            self.assertEqual(len(contexts), 2)
            self.assertEqual(contexts[0].get_value(), "All calls")
            self.assertEqual(contexts[1].get_value(), "agency0, agency1")

    def test_get_metrics_for_empty_report(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add_all(
                [
                    self.test_schema_objects.test_report_monthly,
                    self.test_schema_objects.test_user_A,
                ]
            )
            session.flush()
            metrics = sorted(
                ReportInterface.get_metrics_by_report(
                    report=self.test_schema_objects.test_report_monthly,
                ),
                key=lambda x: x.key,
            )
            total_arrests = metrics[0]

            # Arrests metric should be blank
            self.assertEqual(total_arrests.value, None)
            self.assertEqual(
                assert_type(total_arrests.aggregated_dimensions, list)[
                    0
                ].dimension_to_value,
                {d: None for d in OffenseType},
            )
            self.assertEqual(
                total_arrests.contexts,
                [
                    ReportedContext(key=context.key, value=None)
                    for context in law_enforcement.total_arrests.contexts
                ],
            )

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
            metrics = ReportInterface.get_metrics_by_report(report=report)
            self.assertEqual(len(metrics), 3)
            calls_for_service = [
                metric
                for metric in metrics
                if metric.key == law_enforcement.calls_for_service.key
            ].pop()

            arrests = [
                metric
                for metric in metrics
                if metric.key == law_enforcement.total_arrests.key
            ].pop()

            self.assertIsNotNone(calls_for_service)
            self.assertIsNotNone(arrests)

            # Population metric should be blank
            self.assertEqual(arrests.value, None)
            self.assertEqual(
                arrests.contexts,
                [
                    ReportedContext(key=c.key, value=None)
                    for c in law_enforcement.total_arrests.contexts
                ],
            )
            self.assertEqual(
                arrests.aggregated_dimensions,
                [
                    ReportedAggregatedDimension(
                        dimension_to_value={
                            d: None
                            for d in DIMENSION_IDENTIFIER_TO_DIMENSION[
                                a.dimension.dimension_identifier()
                            ]
                        }
                    )
                    for a in law_enforcement.total_arrests.aggregated_dimensions or []
                ],
            )

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
                detention_value=None,
                patrol_value=40,
            )
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=report_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            # Third update
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
            self.assertEqual(len(datapoint_history), 7)

            # Aggregated value goes from 100000 -> 1000 -> 100
            self.assertEqual(datapoint_history[0].old_value, str(100000))
            self.assertEqual(datapoint_history[0].new_value, str(1000))
            self.assertEqual(datapoint_history[1].old_value, str(1000))
            self.assertEqual(datapoint_history[1].new_value, str(100))

            # First disaggregated value goes from  33334 -> 400 -> 40
            self.assertEqual(datapoint_history[2].old_value, str(33334))
            self.assertEqual(datapoint_history[2].new_value, str(400))
            self.assertEqual(datapoint_history[3].old_value, str(400))
            self.assertEqual(datapoint_history[3].new_value, str(40))

            # Second disaggregated value goes from 66666 -> 600 -> None -> 60
            self.assertEqual(datapoint_history[4].old_value, str(66666))
            self.assertEqual(datapoint_history[4].new_value, str(600))
            self.assertEqual(datapoint_history[5].old_value, str(600))
            self.assertEqual(datapoint_history[5].new_value, None)
            self.assertEqual(datapoint_history[6].old_value, None)
            self.assertEqual(datapoint_history[6].new_value, str(60))

    def test_delete_datapoint(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(self.test_schema_objects.test_user_A)
            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.reported_budget_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .all()
            )
            self.assertEqual(len(datapoints_with_value), 4)

            report_metric = JusticeCountsSchemaTestObjects.get_reported_budget_metric(
                include_contexts=False
            )

            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=report_metric,
                user_account=self.test_schema_objects.test_user_A,
            )

            datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .order_by(schema.Datapoint.id)
                .all()
            )
            self.assertEqual(len(datapoints_with_value), 3)
            self.assertEqual(datapoints_with_value[0].get_value(), 100000)
            self.assertEqual(datapoints_with_value[1].get_value(), 33334)
            self.assertEqual(datapoints_with_value[2].get_value(), 66666)

            # User does not report metric at all
            report_metric = JusticeCountsSchemaTestObjects.get_reported_budget_metric(
                value=None, include_disaggregations=False, include_contexts=False
            )

            ReportInterface.add_or_update_metric(
                session=session,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=report_metric,
                user_account=self.test_schema_objects.test_user_A,
            )
            datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .all()
            )
            self.assertEqual(len(datapoints_with_value), 0)

    def test_get_metrics_for_supervision_report(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add_all(
                [
                    self.test_schema_objects.test_report_supervision,
                    self.test_schema_objects.test_report_parole,
                    self.test_schema_objects.test_user_A,
                ]
            )
            session.flush()

            supervision_metrics = sorted(
                ReportInterface.get_metrics_by_report(
                    report=self.test_schema_objects.test_report_supervision,
                ),
                key=lambda x: x.key,
            )
            parole_metrics = sorted(
                ReportInterface.get_metrics_by_report(
                    report=self.test_schema_objects.test_report_parole,
                ),
                key=lambda x: x.key,
            )

            # Supervision report should have metrics for Parole and Probation
            supervision_metric_systems = {
                metric.metric_definition.system.value for metric in supervision_metrics
            }
            self.assertEqual(
                supervision_metric_systems,
                {schema.System.PAROLE.value, schema.System.PROBATION.value},
            )

            # Parole report should just have metrics for Parole
            parole_metric_systems = {
                metric.metric_definition.system.value for metric in parole_metrics
            }
            self.assertEqual(parole_metric_systems, {schema.System.PAROLE.value})
