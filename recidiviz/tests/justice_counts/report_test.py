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
from typing import List

from freezegun import freeze_time

from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.dimensions.law_enforcement import CallType
from recidiviz.justice_counts.dimensions.offense import OffenseType
from recidiviz.justice_counts.dimensions.person import RaceAndEthnicity
from recidiviz.justice_counts.metric_setting import MetricSettingInterface
from recidiviz.justice_counts.metrics import law_enforcement
from recidiviz.justice_counts.metrics.custom_reporting_frequency import (
    CustomReportingFrequency,
)
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.justice_counts.utils.constants import UploadMethod
from recidiviz.justice_counts.utils.datapoint_utils import get_value
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)
from recidiviz.utils.types import assert_type


class TestReportInterface(JusticeCountsDatabaseTestCase):
    """Implements tests for the ReportInterface."""

    def setUp(self) -> None:
        super().setUp()
        self.maxDiff = None
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
            session.commit()
            monthly_report_agency_id = monthly_report.source_id
            annual_report_agency_id = annual_report.source_id

            reports_agency_A = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=monthly_report_agency_id,
            )
            self.assertEqual(reports_agency_A, [monthly_report])

            reports_agency_B = ReportInterface.get_reports_by_agency_id(
                session=session,
                agency_id=annual_report_agency_id,
            )
            self.assertEqual(reports_agency_B, [annual_report])

    def test_delete_reports(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            monthly_report = self.test_schema_objects.test_report_monthly
            annual_report = self.test_schema_objects.test_report_annual
            session.add_all(
                [
                    monthly_report,
                    annual_report,
                ]
            )

            session.flush()
            session.refresh(monthly_report)
            session.refresh(annual_report)
            monthly_report_id = monthly_report.id
            annual_report_id = annual_report.id

        with SessionFactory.using_database(self.database_key) as session:
            monthly_report = ReportInterface.get_report_by_id(
                session, report_id=monthly_report_id
            )
            inserts: List[schema.Datapoint] = []
            updates: List[schema.Datapoint] = []
            histories: List[schema.DatapointHistory] = []
            ReportInterface.add_or_update_metric(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=monthly_report,
                report_metric=self.test_schema_objects.funding_metric,
                user_account=self.test_schema_objects.test_user_A,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()
            datapoints = session.query(schema.Datapoint).all()
            self.assertEqual(len(datapoints), 1)

        with SessionFactory.using_database(self.database_key) as session:
            ReportInterface.delete_reports_by_id(
                session, report_ids=[monthly_report_id, annual_report_id]
            )

            reports = session.query(schema.Report).all()
            self.assertEqual(len(reports), 0)

            # Datapoints on the report should be automatically deleted
            # via `cascade`
            datapoints = session.query(schema.Datapoint).all()
            self.assertEqual(len(datapoints), 0)

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
                new_annual_report.date_range_start, datetime.date(2021, 3, 1)
            )
            self.assertEqual(
                new_annual_report.date_range_end, datetime.date(2022, 3, 1)
            )

    def test_create_new_reports_null_user_account_id(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add_all(
                [
                    self.test_schema_objects.test_agency_A,
                ]
            )

            session.flush()
            session.refresh(self.test_schema_objects.test_agency_A)
            agency_id_A = self.test_schema_objects.test_agency_A.id

            (
                new_monthly_report_A,
                new_yearly_report_A,
                monthly_metric_defs_A,
                annual_metric_defs_A,
            ) = ReportInterface.create_new_reports(
                session=session,
                agency_id=agency_id_A,
                user_account_id=None,
                current_month=1,
                current_year=2022,
                previous_month=12,
                previous_year=2021,
                systems={
                    schema.System[sys]
                    for sys in self.test_schema_objects.test_agency_A.systems
                },
                metric_key_to_metric_interface={},
            )
            self.assertIsNotNone(new_monthly_report_A)
            if new_monthly_report_A:
                self.assertEqual(new_monthly_report_A.modified_by, [])
            self.assertIsNotNone(new_yearly_report_A)
            if new_yearly_report_A:
                self.assertEqual(new_yearly_report_A.modified_by, [])
            self.assertIsNotNone(monthly_metric_defs_A)
            self.assertIsNotNone(annual_metric_defs_A)

    def test_create_new_reports(self) -> None:
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

            user_id_A = self.test_schema_objects.test_user_A.id
            agency_id_A = self.test_schema_objects.test_agency_A.id

            # Case 1 (Both Monthly and Annual reports created)
            (
                new_monthly_report_A,
                new_yearly_report_A,
                monthly_metric_defs_A,
                annual_metric_defs_A,
            ) = ReportInterface.create_new_reports(
                session=session,
                agency_id=agency_id_A,
                user_account_id=user_id_A,
                current_month=1,
                current_year=2022,
                previous_month=12,
                previous_year=2021,
                systems={
                    schema.System[sys]
                    for sys in self.test_schema_objects.test_agency_A.systems
                },
                metric_key_to_metric_interface={},
            )
            self.assertIsNotNone(new_monthly_report_A)
            self.assertIsNotNone(new_yearly_report_A)
            self.assertIsNotNone(monthly_metric_defs_A)
            self.assertIsNotNone(annual_metric_defs_A)
            if new_monthly_report_A:
                self.assertEqual(new_monthly_report_A.source_id, agency_id_A)
                self.assertEqual(
                    new_monthly_report_A.type, schema.ReportingFrequency.MONTHLY.value
                )
                self.assertEqual(
                    new_monthly_report_A.date_range_start,
                    datetime.date(2021, 12, 1),
                )
                self.assertEqual(
                    new_monthly_report_A.date_range_end,
                    datetime.date(2022, 1, 1),
                )
            if new_yearly_report_A:
                self.assertEqual(new_yearly_report_A.source_id, agency_id_A)
                self.assertEqual(
                    new_yearly_report_A.type, schema.ReportingFrequency.ANNUAL.value
                )
                self.assertEqual(
                    new_yearly_report_A.date_range_start,
                    datetime.date(2021, 1, 1),
                )
                self.assertEqual(
                    new_yearly_report_A.date_range_end,
                    datetime.date(2022, 1, 1),
                )

            # Case 2 (Neither Monthly nor Annual report created (already exist))
            (
                new_monthly_report_A,
                new_yearly_report_A,
                monthly_metric_defs_A,
                annual_metric_defs_A,
            ) = ReportInterface.create_new_reports(
                session=session,
                agency_id=agency_id_A,
                user_account_id=user_id_A,
                current_month=1,
                current_year=2022,
                previous_month=12,
                previous_year=2021,
                systems={
                    schema.System[sys]
                    for sys in self.test_schema_objects.test_agency_A.systems
                },
                metric_key_to_metric_interface={},
            )
            self.assertEqual(new_monthly_report_A, None)
            self.assertEqual(new_yearly_report_A, None)
            self.assertIsNotNone(monthly_metric_defs_A)
            self.assertIsNotNone(annual_metric_defs_A)

            # Change all metrics for Agency A to annual starting in February
            february_fiscal_frequency = CustomReportingFrequency(
                starting_month=2, frequency=schema.ReportingFrequency.ANNUAL
            )
            metric_interfaces = MetricSettingInterface.get_agency_metric_interfaces(
                agency=self.test_schema_objects.test_agency_A, session=session
            )
            for metric in metric_interfaces:
                metric.custom_reporting_frequency = february_fiscal_frequency

            # Case 3 (Neither Monthly nor Annual report created (no metrics available))
            (
                new_monthly_report_A,
                new_yearly_report_A,
                monthly_metric_defs_A,
                annual_metric_defs_A,
            ) = ReportInterface.create_new_reports(
                session=session,
                agency_id=agency_id_A,
                user_account_id=user_id_A,
                current_month=1,
                current_year=2022,
                previous_month=12,
                previous_year=2021,
                systems={
                    schema.System[sys]
                    for sys in self.test_schema_objects.test_agency_A.systems
                },
                metric_key_to_metric_interface={
                    interface.key: interface for interface in metric_interfaces
                },
            )
            self.assertEqual(new_monthly_report_A, None)
            self.assertEqual(new_yearly_report_A, None)
            self.assertEqual(monthly_metric_defs_A, [])
            self.assertEqual(annual_metric_defs_A, [])

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
            ReportInterface.get_reporting_frequency(report),
            schema.ReportingFrequency.MONTHLY,
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
            ReportInterface.get_reporting_frequency(report),
            schema.ReportingFrequency.MONTHLY,
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
            ReportInterface.get_reporting_frequency(report),
            schema.ReportingFrequency.ANNUAL,
        )

        # July, Annually
        report = ReportInterface.create_report_object(
            agency_id=0,
            user_account_id=0,
            year=2022,
            month=7,
            frequency=schema.ReportingFrequency.ANNUAL.value,
        )
        self.assertEqual(report.date_range_end, datetime.date(2022, 7, 1))
        self.assertEqual(
            ReportInterface.get_reporting_frequency(report),
            schema.ReportingFrequency.ANNUAL,
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
            user_A = self.test_schema_objects.test_user_A
            user_C = self.test_schema_objects.test_user_C
            agency = self.test_schema_objects.test_agency_A
            agency_user_association_A = schema.AgencyUserAccountAssociation(
                agency=agency,
                user_account=user_A,
                role=schema.UserAccountRole.AGENCY_ADMIN,
            )
            agency_user_association_C = schema.AgencyUserAccountAssociation(
                agency=agency,
                user_account=user_C,
                role=schema.UserAccountRole.AGENCY_ADMIN,
            )
            session.add_all(
                [
                    agency,
                    user_A,
                    user_C,
                    agency_user_association_A,
                    agency_user_association_C,
                ]
            )
            session.commit()
            session.refresh(agency)
            session.refresh(user_A)
            session.refresh(user_C)

            userAID = user_A.id
            userCID = user_C.id
            agency_id = agency.id
            update_datetime = datetime.datetime(
                2022, 2, 1, 0, 0, 0, 0, datetime.timezone.utc
            )

            with freeze_time(update_datetime):
                report = ReportInterface.create_report(
                    session=session,
                    agency_id=agency_id,
                    user_account_id=user_A.id,
                    month=2,
                    year=2022,
                    frequency=schema.ReportingFrequency.MONTHLY.value,
                )
                self.assertEqual(report.status, schema.ReportStatus.NOT_STARTED)
                updated_report = ReportInterface.update_report_metadata(
                    report=report,
                    editor_id=user_A.id,
                    status=schema.ReportStatus.DRAFT.value,
                )
                self.assertEqual(updated_report.status, schema.ReportStatus.DRAFT)
                self.assertEqual(updated_report.modified_by, [user_A.id])
                self.assertEqual(
                    updated_report.last_modified_at.timestamp(),
                    update_datetime.timestamp(),
                )

                updated_report = ReportInterface.update_report_metadata(
                    report=updated_report,
                    editor_id=user_C.id,
                    status=schema.ReportStatus.DRAFT.value,
                )

                self.assertEqual(updated_report.status, schema.ReportStatus.DRAFT)
                self.assertEqual(updated_report.modified_by, [user_A.id, user_C.id])

                updated_report = ReportInterface.update_report_metadata(
                    report=updated_report,
                    status=schema.ReportStatus.PUBLISHED.value,
                    editor_id=user_A.id,
                )

                session.add(updated_report)
                session.flush()
                session.refresh(updated_report)
                report_id = updated_report.id

        with SessionFactory.using_database(self.database_key) as session:
            updated_report = ReportInterface.get_report_by_id(session, report_id)
            user_A = UserAccountInterface.get_user_by_id(session, userAID)
            user_C = UserAccountInterface.get_user_by_id(session, userCID)
            self.assertEqual(updated_report.publish_date, update_datetime.date())
            self.assertEqual(updated_report.status, schema.ReportStatus.PUBLISHED)
            self.assertEqual(
                updated_report.modified_by,
                [user_C.id, user_A.id],
            )
            editor_id_to_json = {
                user_C.id: {"name": user_C.name, "role": "AGENCY_ADMIN"},
                user_A.id: {"name": user_A.name, "role": "AGENCY_ADMIN"},
            }
            report_json = ReportInterface.to_json_response(
                report=updated_report,
                editor_id_to_json=editor_id_to_json,
            )
            # Editor names will be displayed in reverse chronological order.
            self.assertEqual(
                report_json["editors"],
                [
                    {"name": user_A.name, "role": "AGENCY_ADMIN"},
                    {"name": user_C.name, "role": "AGENCY_ADMIN"},
                ],
            )

            update_datetime = datetime.datetime(
                2022, 2, 1, 1, 0, 0, 0, datetime.timezone.utc
            )
            with freeze_time(update_datetime):
                updated_report = ReportInterface.update_report_metadata(
                    report=updated_report,
                    status=schema.ReportStatus.PUBLISHED.value,
                    editor_id=user_A.id,
                )
                self.assertEqual(
                    updated_report.last_modified_at.timestamp(),
                    update_datetime.timestamp(),
                )

    def test_add_budget_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            inserts: List[schema.Datapoint] = []
            updates: List[schema.Datapoint] = []
            histories: List[schema.DatapointHistory] = []
            ReportInterface.add_or_update_metric(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.funding_metric,
                user_account=self.test_schema_objects.test_user_A,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()
            total_datapoints = session.query(schema.Datapoint).all()
            self.assertEqual(len(total_datapoints), 1)

            # We should have a datapoint that represents the aggregated Law Enforcement budget
            datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .order_by(schema.Datapoint.id)
                .all()
            )
            self.assertEqual(len(datapoints_with_value), 1)
            report_metric = self.test_schema_objects.funding_metric
            self.assertEqual(
                get_value(datapoints_with_value[0]),
                report_metric.value,
            )

    def test_add_empty_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            inserts: List[schema.Datapoint] = []
            updates: List[schema.Datapoint] = []
            histories: List[schema.DatapointHistory] = []
            ReportInterface.add_or_update_metric(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.get_funding_metric(
                    value=None,
                ),
                user_account=self.test_schema_objects.test_user_A,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()
            total_datapoints = session.query(schema.Datapoint).all()
            self.assertEqual(len(total_datapoints), 1)
            # There should be no datapoints with values associated with the metric
            queried_datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .all()
            )
            self.assertEqual(len(queried_datapoints_with_value), 0)
            queried_datapoints_without_value = session.query(schema.Datapoint).all()
            self.assertEqual(len(queried_datapoints_without_value), 1)

    def test_add_incomplete_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            incomplete_report = (
                self.test_schema_objects.get_reported_calls_for_service_metric(
                    emergency_value=None, unknown_value=None
                )
            )
            inserts: List[schema.Datapoint] = []
            updates: List[schema.Datapoint] = []
            histories: List[schema.DatapointHistory] = []
            ReportInterface.add_or_update_metric(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=incomplete_report,
                user_account=self.test_schema_objects.test_user_A,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()

            total_datapoints = session.query(schema.Datapoint).all()
            # 1 aggregate datapoint, 4 breakdown datapoints
            self.assertEqual(len(total_datapoints), 5)

            # The aggregate value datapoint should have a non-null values
            datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .all()
            )

            self.assertEqual(len(datapoints_with_value), 2)
            reported_aggregated_dimensions = assert_type(
                incomplete_report.aggregated_dimensions, list
            )
            self.assertEqual(get_value(datapoints_with_value[0]), 100)
            self.assertEqual(
                get_value(datapoints_with_value[1]),
                reported_aggregated_dimensions[0].dimension_to_value[
                    CallType.NON_EMERGENCY
                ],
            )

    def test_add_calls_for_service_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            report_metric = self.test_schema_objects.reported_calls_for_service_metric
            inserts: List[schema.Datapoint] = []
            updates: List[schema.Datapoint] = []
            histories: List[schema.DatapointHistory] = []
            ReportInterface.add_or_update_metric(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=report_metric,
                user_account=self.test_schema_objects.test_user_A,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()

            total_datapoints = session.query(schema.Datapoint).all()
            self.assertEqual(len(total_datapoints), 5)
            # We should have four datapoints with non-null values:
            # one aggregate value and three breakdowns.
            datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .all()
            )

            self.assertEqual(len(datapoints_with_value), 4)

            self.assertEqual(
                get_value(datapoints_with_value[0]),
                report_metric.value,
            )
            aggregated_dimensions = assert_type(
                report_metric.aggregated_dimensions, list
            )
            self.assertEqual(
                get_value(datapoints_with_value[1]),
                aggregated_dimensions[0].dimension_to_value[CallType.EMERGENCY],
            )
            self.assertEqual(
                get_value(datapoints_with_value[2]),
                aggregated_dimensions[0].dimension_to_value[CallType.NON_EMERGENCY],
            )
            self.assertEqual(
                get_value(datapoints_with_value[3]),
                aggregated_dimensions[0].dimension_to_value[CallType.UNKNOWN],
            )

    def test_add_population_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            report = self.test_schema_objects.test_report_monthly
            report_metric = self.test_schema_objects.arrests_metric
            inserts: List[schema.Datapoint] = []
            updates: List[schema.Datapoint] = []
            histories: List[schema.DatapointHistory] = []
            ReportInterface.add_or_update_metric(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=report,
                report_metric=report_metric,
                user_account=self.test_schema_objects.test_user_A,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()

            total_datapoints = session.query(schema.Datapoint).all()
            self.assertEqual(len(total_datapoints), 25)

            # We should have nine datapoints with non-null values: one for aggregated (total) residents
            # and eight for each race and ethnicity
            datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .all()
            )

            self.assertEqual(len(datapoints_with_value), 25)

            self.assertEqual(
                get_value(datapoints_with_value[0]),
                report_metric.value,
            )
            aggregated_dimensions = assert_type(
                report_metric.aggregated_dimensions, list
            )
            self.assertEqual(
                get_value(datapoints_with_value[1]),
                aggregated_dimensions[0].dimension_to_value[
                    RaceAndEthnicity.HISPANIC_AMERICAN_INDIAN_ALASKAN_NATIVE
                ],
            )
            self.assertEqual(
                get_value(datapoints_with_value[2]),
                aggregated_dimensions[0].dimension_to_value[
                    RaceAndEthnicity.HISPANIC_ASIAN
                ],
            )
            self.assertEqual(
                get_value(datapoints_with_value[3]),
                aggregated_dimensions[0].dimension_to_value[
                    RaceAndEthnicity.HISPANIC_BLACK
                ],
            )
            self.assertEqual(
                get_value(datapoints_with_value[4]),
                aggregated_dimensions[0].dimension_to_value[
                    RaceAndEthnicity.HISPANIC_MORE_THAN_ONE_RACE
                ],
            )
            self.assertEqual(
                get_value(datapoints_with_value[5]),
                aggregated_dimensions[0].dimension_to_value[
                    RaceAndEthnicity.HISPANIC_NATIVE_HAWAIIAN_PACIFIC_ISLANDER
                ],
            )
            self.assertEqual(
                get_value(datapoints_with_value[6]),
                aggregated_dimensions[0].dimension_to_value[
                    RaceAndEthnicity.HISPANIC_WHITE
                ],
            )
            self.assertEqual(
                get_value(datapoints_with_value[7]),
                aggregated_dimensions[0].dimension_to_value[
                    RaceAndEthnicity.HISPANIC_OTHER
                ],
            )
            self.assertEqual(
                get_value(datapoints_with_value[8]),
                aggregated_dimensions[0].dimension_to_value[
                    RaceAndEthnicity.HISPANIC_UNKNOWN
                ],
            )

    def test_update_metric_no_change(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            report = self.test_schema_objects.test_report_monthly
            session.add(report)
            report = session.query(schema.Report).one_or_none()
            inserts: List[schema.Datapoint] = []
            updates: List[schema.Datapoint] = []
            histories: List[schema.DatapointHistory] = []
            ReportInterface.add_or_update_metric(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=report,
                report_metric=self.test_schema_objects.funding_metric,
                user_account=self.test_schema_objects.test_user_A,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()
            total_datapoints = session.query(schema.Datapoint).all()
            self.assertEqual(len(total_datapoints), 1)

            datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .all()
            )

            self.assertEqual(len(datapoints_with_value), 1)
            # This should be a no-op, because the metric definition is the same
            # according to our unique constraints, so we update the existing records
            # (which does nothing, since nothing has changed)
            inserts.clear()
            updates.clear()
            histories.clear()
            ReportInterface.add_or_update_metric(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=report,
                report_metric=self.test_schema_objects.funding_metric,
                user_account=self.test_schema_objects.test_user_A,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()
            queried_datapoints_no_op = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .order_by(schema.Datapoint.id)
                .all()
            )
            self.assertEqual(len(queried_datapoints_no_op), 1)
            self.assertAlmostEqual(datapoints_with_value, queried_datapoints_no_op)

    def test_update_metric_with_new_values(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(self.test_schema_objects.test_user_A)
            report_metric = self.test_schema_objects.reported_calls_for_service_metric

            inserts: List[schema.Datapoint] = []
            updates: List[schema.Datapoint] = []
            histories: List[schema.DatapointHistory] = []
            ReportInterface.add_or_update_metric(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=report_metric,
                user_account=self.test_schema_objects.test_user_A,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()

            total_datapoints = session.query(schema.Datapoint).all()
            # One aggregate datapoint, 4 breakdowns
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
                get_value(datapoints_with_value[0]),
                report_metric.value,
            )
            self.assertEqual(
                get_value(datapoints_with_value[1]),
                aggregated_dimensions[0].dimension_to_value[CallType.EMERGENCY],
            )
            self.assertEqual(
                get_value(datapoints_with_value[2]),
                aggregated_dimensions[0].dimension_to_value[CallType.NON_EMERGENCY],
            )
            self.assertEqual(
                get_value(datapoints_with_value[3]),
                aggregated_dimensions[0].dimension_to_value[CallType.UNKNOWN],
            )

            # This should result in an update to the existing database objects
            new_report_metric = (
                JusticeCountsSchemaTestObjects.get_reported_calls_for_service_metric(
                    value=1000,
                )
            )
            inserts.clear()
            updates.clear()
            histories.clear()
            ReportInterface.add_or_update_metric(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=new_report_metric,
                user_account=self.test_schema_objects.test_user_A,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()

            datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .order_by(schema.Datapoint.id)
                .all()
            )

            self.assertEqual(len(datapoints_with_value), 4)

            self.assertEqual(
                get_value(datapoints_with_value[0]), new_report_metric.value
            )
            aggregated_dimensions = assert_type(
                new_report_metric.aggregated_dimensions, list
            )
            self.assertEqual(
                get_value(datapoints_with_value[1]),
                aggregated_dimensions[0].dimension_to_value[CallType.EMERGENCY],
            )
            self.assertEqual(
                get_value(datapoints_with_value[2]),
                aggregated_dimensions[0].dimension_to_value[CallType.NON_EMERGENCY],
            )
            self.assertEqual(
                get_value(datapoints_with_value[3]),
                aggregated_dimensions[0].dimension_to_value[CallType.UNKNOWN],
            )

    def test_get_metrics_for_empty_report(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            session.add_all(
                [
                    agency,
                    self.test_schema_objects.test_user_A,
                    self.test_schema_objects.test_report_monthly,
                ]
            )
            session.flush()
            session.refresh(agency)

            MetricSettingInterface.add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric_updates=MetricInterface(
                    key=law_enforcement.calls_for_service.key,
                    custom_reporting_frequency=CustomReportingFrequency(
                        frequency=schema.ReportingFrequency.ANNUAL, starting_month=2
                    ),
                ),
            )

            metrics = sorted(
                ReportInterface.get_metrics_by_report(
                    report=self.test_schema_objects.test_report_monthly,
                    session=session,
                ),
                key=lambda x: x.key,
            )
            # Only the arrests and reported_crime metrics will be included b/c
            # the reporting frequency for the calls_for_service was changed to
            # ANNUAL
            self.assertEqual(len(metrics), 2)

            arrests = metrics[0]

            # Arrests metric should be blank
            self.assertEqual(arrests.key, law_enforcement.arrests.key)
            self.assertEqual(arrests.value, None)
            self.assertEqual(
                assert_type(arrests.aggregated_dimensions, list)[0].dimension_to_value,
                {d: None for d in OffenseType},
            )

            reported_crime = metrics[1]

            # Reported crime metric should be blank
            self.assertEqual(reported_crime.key, law_enforcement.reported_crime.key)
            self.assertEqual(reported_crime.value, None)
            self.assertEqual(
                assert_type(reported_crime.aggregated_dimensions, list)[
                    0
                ].dimension_to_value,
                {d: None for d in OffenseType},
            )

        annual_report_jan = self.test_schema_objects.get_report_for_agency(
            agency=agency,
            frequency="ANNUAL",
        )
        annual_report_feb = self.test_schema_objects.get_report_for_agency(
            agency=agency, frequency="ANNUAL", starting_month_str="02"
        )
        session.add_all([annual_report_jan, annual_report_feb])
        session.flush()
        session.refresh(annual_report_jan)
        session.refresh(annual_report_feb)

        metrics = sorted(
            ReportInterface.get_metrics_by_report(
                report=annual_report_jan,
                session=session,
            ),
            key=lambda x: x.key,
        )
        # There should only be five metrics (expenses, funding, use_of_force_incidents,
        # civilian_complaints_sustained, total_staff) because the annual report is from
        # Jan - Dec and the calls_for_service metric has been changed to only be reported
        # annually starting in February.
        self.assertEqual(len(metrics), 5)

        self.assertEqual(
            metrics[0].key, law_enforcement.civilian_complaints_sustained.key
        )
        self.assertEqual(metrics[1].key, law_enforcement.expenses.key)
        self.assertEqual(metrics[2].key, law_enforcement.funding.key)
        self.assertEqual(metrics[3].key, law_enforcement.staff.key)
        self.assertEqual(metrics[4].key, law_enforcement.use_of_force_incidents.key)

        metrics = sorted(
            ReportInterface.get_metrics_by_report(
                report=annual_report_feb,
                session=session,
            ),
            key=lambda x: x.key,
        )
        # There should only be one metric for the February report, calls_for_service
        self.assertEqual(len(metrics), 1)
        self.assertEqual(metrics[0].key, law_enforcement.calls_for_service.key)

    def test_get_metrics_for_nonempty_report(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            session.add_all(
                [
                    self.test_schema_objects.test_report_monthly,
                    self.test_schema_objects.test_user_A,
                    agency,
                ]
            )
            session.flush()
            session.refresh(agency)
            report_id = self.test_schema_objects.test_report_monthly.id

            report = ReportInterface.get_report_by_id(
                session=session, report_id=report_id
            )
            inserts: List[schema.Datapoint] = []
            updates: List[schema.Datapoint] = []
            histories: List[schema.DatapointHistory] = []
            ReportInterface.add_or_update_metric(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=report,
                report_metric=self.test_schema_objects.reported_calls_for_service_metric,
                user_account=self.test_schema_objects.test_user_A,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            MetricSettingInterface.add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric_updates=self.test_schema_objects.reported_calls_for_service_metric,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()
            metrics = ReportInterface.get_metrics_by_report(
                report=report, session=session
            )
            self.assertEqual(len(metrics), 3)
            calls_for_service = [
                metric
                for metric in metrics
                if metric.key == law_enforcement.calls_for_service.key
            ].pop()

            arrests = [
                metric
                for metric in metrics
                if metric.key == law_enforcement.arrests.key
            ].pop()

            self.assertIsNotNone(calls_for_service)
            self.assertIsNotNone(arrests)
            self.assertEqual(arrests.value, None)

            # Calls for service metric should be populated
            self.assertEqual(
                calls_for_service.value,
                self.test_schema_objects.reported_calls_for_service_metric.value,
            )
            self.assertEqual(
                calls_for_service.aggregated_dimensions,
                self.test_schema_objects.reported_calls_for_service_metric.aggregated_dimensions,
            )

            # Add custom reporting frequency to reported_calls_for_service_metric.
            reported_calls_for_service_metric = (
                self.test_schema_objects.reported_calls_for_service_metric
            )
            reported_calls_for_service_metric.custom_reporting_frequency = (
                CustomReportingFrequency(
                    frequency=schema.ReportingFrequency.ANNUAL,
                )
            )
            MetricSettingInterface.add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric_updates=reported_calls_for_service_metric,
            )
            session.commit()
            # Calls for service reporting frequency should still appear in the
            # report because the report was created and edited before the change.
            metrics = ReportInterface.get_metrics_by_report(
                report=report, session=session
            )
            self.assertEqual(len(metrics), 3)
            calls_for_service = [
                metric
                for metric in metrics
                if metric.key == law_enforcement.calls_for_service.key
            ].pop()

            self.assertIsNotNone(calls_for_service)
            self.assertEqual(
                calls_for_service.value,
                self.test_schema_objects.reported_calls_for_service_metric.value,
            )

            # Add a custom reporting frequency so that now civilian complaints sustained is
            # reported monthly.
            MetricSettingInterface.add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric_updates=MetricInterface(
                    key=law_enforcement.civilian_complaints_sustained.key,
                    custom_reporting_frequency=CustomReportingFrequency(
                        frequency=schema.ReportingFrequency.MONTHLY,
                    ),
                ),
            )
            session.commit()

            # Civilians complaints should still appear in the report
            metrics = ReportInterface.get_metrics_by_report(
                report=report, session=session
            )
            self.assertEqual(len(metrics), 4)
            civilian_complaints_sustained = [
                metric
                for metric in metrics
                if metric.key == law_enforcement.civilian_complaints_sustained.key
            ].pop()

            self.assertIsNotNone(civilian_complaints_sustained)

    def test_datapoint_histories(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(self.test_schema_objects.test_user_A)
            inserts: List[schema.Datapoint] = []
            updates: List[schema.Datapoint] = []
            histories: List[schema.DatapointHistory] = []
            ReportInterface.add_or_update_metric(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.funding_metric,
                user_account=self.test_schema_objects.test_user_A,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()

            # First update
            report_metric = JusticeCountsSchemaTestObjects.get_funding_metric(
                value=1000,
            )
            inserts.clear()
            updates.clear()
            histories.clear()
            ReportInterface.add_or_update_metric(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=report_metric,
                user_account=self.test_schema_objects.test_user_A,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()

            # Second update
            report_metric = JusticeCountsSchemaTestObjects.get_funding_metric(
                value=100,
            )
            inserts.clear()
            updates.clear()
            histories.clear()
            ReportInterface.add_or_update_metric(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=report_metric,
                user_account=self.test_schema_objects.test_user_A,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()

            # Third update
            report_metric = JusticeCountsSchemaTestObjects.get_funding_metric(
                value=10,
            )
            inserts.clear()
            updates.clear()
            histories.clear()
            ReportInterface.add_or_update_metric(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=report_metric,
                user_account=self.test_schema_objects.test_user_A,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()

            datapoint_history = (
                session.query(schema.DatapointHistory)
                .order_by(
                    schema.DatapointHistory.datapoint_id,
                    schema.DatapointHistory.timestamp,
                )
                .all()
            )
            self.assertEqual(len(datapoint_history), 3)

            # Aggregated value goes from 1000 -> 1000 -> 100 -> 10
            self.assertEqual(datapoint_history[0].old_value, str(100000))
            self.assertEqual(datapoint_history[0].new_value, str(1000))
            self.assertEqual(datapoint_history[1].old_value, str(1000))
            self.assertEqual(datapoint_history[1].new_value, str(100))
            self.assertEqual(datapoint_history[2].old_value, str(100))
            self.assertEqual(datapoint_history[2].new_value, str(10))

    def test_delete_datapoint(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(self.test_schema_objects.test_user_A)
            inserts: List[schema.Datapoint] = []
            updates: List[schema.Datapoint] = []
            histories: List[schema.DatapointHistory] = []
            ReportInterface.add_or_update_metric(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=self.test_schema_objects.reported_calls_for_service_metric,
                user_account=self.test_schema_objects.test_user_A,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()

            datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .all()
            )
            self.assertEqual(len(datapoints_with_value), 4)
            self.assertEqual(get_value(datapoints_with_value[0]), 100.0)
            self.assertEqual(get_value(datapoints_with_value[1]), 20.0)
            self.assertEqual(get_value(datapoints_with_value[2]), 60.0)
            self.assertEqual(get_value(datapoints_with_value[3]), 20.0)

            # If user explicitly sets metric value as None, but doesn't include disaggregations or contexts,
            # the metric value will be changed, but disaggregations and contexts will be left alone. You have
            # to explicitly set values to None to delete them.
            report_metric = (
                JusticeCountsSchemaTestObjects.get_reported_calls_for_service_metric(
                    value=None,
                )
            )

            inserts.clear()
            updates.clear()
            histories.clear()
            ReportInterface.add_or_update_metric(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=report_metric,
                user_account=self.test_schema_objects.test_user_A,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()
            datapoints_with_value = (
                session.query(schema.Datapoint)
                .filter(schema.Datapoint.value.is_not(None))
                .all()
            )
            # 3 and not 4 because the aggregate metric value did change to None
            self.assertEqual(len(datapoints_with_value), 3)

            # If user doesn't include contexts at all, this doesn't delete anything.
            # To delete them, you'd have to specifically include them with values
            # of None or empty string. This will delete everything because the contexts and
            # disaggregations will be nullified and the value is already None.
            report_metric = (
                JusticeCountsSchemaTestObjects.get_reported_calls_for_service_metric(
                    value=None, nullify_contexts_and_disaggregations=True
                )
            )
            inserts.clear()
            updates.clear()
            histories.clear()
            ReportInterface.add_or_update_metric(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=self.test_schema_objects.test_report_monthly,
                report_metric=report_metric,
                user_account=self.test_schema_objects.test_user_A,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()
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
                    self.test_schema_objects.test_report_parole_probation,
                    self.test_schema_objects.test_user_A,
                ]
            )
            session.flush()

            supervision_metrics = sorted(
                ReportInterface.get_metrics_by_report(
                    report=self.test_schema_objects.test_report_supervision,
                    session=session,
                ),
                key=lambda x: x.key,
            )
            parole_metrics = sorted(
                ReportInterface.get_metrics_by_report(
                    report=self.test_schema_objects.test_report_parole, session=session
                ),
                key=lambda x: x.key,
            )
            parole_probation_metrics = sorted(
                ReportInterface.get_metrics_by_report(
                    report=self.test_schema_objects.test_report_parole_probation,
                    session=session,
                ),
                key=lambda x: x.key,
            )

            # Supervision report should have one copy of metrics
            supervision_metric_systems = {
                metric.metric_definition.system.value for metric in supervision_metrics
            }
            self.assertEqual(
                supervision_metric_systems,
                {schema.System.SUPERVISION.value},
            )

            # Parole report should just have metrics for Parole
            parole_metric_systems = {
                metric.metric_definition.system.value for metric in parole_metrics
            }
            self.assertEqual(parole_metric_systems, {schema.System.PAROLE.value})

            # Parole/Probation report should have only metrics for Supervision enabled
            # (and the others disabled)  b/c none of the metrics are broken down by the
            # sub-systems.
            parole_probation_enabled_metric_systems = {
                metric.metric_definition.system.value
                for metric in parole_probation_metrics
                if metric.is_metric_enabled is not False
            }
            self.assertEqual(
                parole_probation_enabled_metric_systems,
                {
                    schema.System.SUPERVISION.value,
                },
            )

            parole_probation_disabled_metric_systems = {
                metric.metric_definition.system.value
                for metric in parole_probation_metrics
                if metric.is_metric_enabled is False
            }
            self.assertEqual(
                parole_probation_disabled_metric_systems,
                {
                    schema.System.PAROLE.value,
                    schema.System.PROBATION.value,
                },
            )

    def test_create_reports_for_new_agency(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            with freeze_time(datetime.date(2020, 3, 1)):
                agency = self.test_schema_objects.test_agency_A
                user = self.test_schema_objects.test_user_A
                session.add_all([agency, user])
                session.flush()

                ReportInterface.create_reports_for_new_agency(
                    session=session, agency_id=agency.id, user_account_id=user.id
                )

                reports = session.query(schema.Report).all()
                self.assertEqual(len(reports), 16)
                annual_report = (
                    session.query(schema.Report)
                    .filter(
                        schema.Report.type == schema.ReportingFrequency.ANNUAL.value,
                    )
                    .one()
                )
                self.assertIsNotNone(annual_report)
                monthly_reports = (
                    session.query(schema.Report)
                    .filter(
                        schema.Report.type == schema.ReportingFrequency.MONTHLY.value,
                    )
                    .all()
                )
                # 15 total reports: 5 for the current year (January 2020 - May 2020), 10 for the previous year (Mar 2019 - December 2019)
                self.assertEqual(len(monthly_reports), 15)

    def test_query_for_latest_annual_reports(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            with freeze_time(datetime.date(2023, 8, 1)):
                today = datetime.date.today()
                agency = self.test_schema_objects.test_agency_A
                calendar_year_report = schema.Report(
                    source=agency,
                    type="ANNUAL",
                    instance="Test Annual Calendar-Year Report",
                    status=schema.ReportStatus.NOT_STARTED,
                    acquisition_method=schema.AcquisitionMethod.CONTROL_PANEL,
                    project=schema.Project.JUSTICE_COUNTS_CONTROL_PANEL,
                    date_range_start=datetime.date(month=1, year=today.year - 1, day=1),
                    date_range_end=datetime.date(month=1, year=today.year, day=1),
                )

                fiscal_year_2022_report = schema.Report(
                    source=agency,
                    type="ANNUAL",
                    instance="Test Annual Fiscal-Year Report 2023",
                    status=schema.ReportStatus.NOT_STARTED,
                    acquisition_method=schema.AcquisitionMethod.CONTROL_PANEL,
                    project=schema.Project.JUSTICE_COUNTS_CONTROL_PANEL,
                    date_range_start=datetime.date(month=7, year=today.year - 1, day=1),
                    date_range_end=datetime.date(month=7, year=today.year, day=1),
                )

                fiscal_year_2021_report = schema.Report(
                    source=agency,
                    type="ANNUAL",
                    instance="Test Annual Fiscal-Year Report 2022",
                    status=schema.ReportStatus.NOT_STARTED,
                    acquisition_method=schema.AcquisitionMethod.CONTROL_PANEL,
                    project=schema.Project.JUSTICE_COUNTS_CONTROL_PANEL,
                    date_range_start=datetime.date(month=7, year=today.year - 2, day=1),
                    date_range_end=datetime.date(month=7, year=today.year - 1, day=1),
                )

                session.add_all(
                    [
                        calendar_year_report,
                        fiscal_year_2022_report,
                        fiscal_year_2021_report,
                        agency,
                    ]
                )
                session.commit()
                reports = ReportInterface.get_latest_annual_reports_by_agency_id(
                    session=session,
                    agency_id=agency.id,
                    days_after_time_period_to_send_email=1,
                    today=datetime.date.today(),
                )
                self.assertEqual(len(reports), 2)
                self.assertEqual(
                    reports[0].instance, "Test Annual Fiscal-Year Report 2023"
                )
                self.assertEqual(
                    reports[1].instance, "Test Annual Calendar-Year Report"
                )
