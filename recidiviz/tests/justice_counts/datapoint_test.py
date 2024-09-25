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
from typing import List

from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.dimensions.prisons import FundingType
from recidiviz.justice_counts.dimensions.supervision import DailyPopulationType
from recidiviz.justice_counts.exceptions import JusticeCountsServerError
from recidiviz.justice_counts.metrics import law_enforcement, prisons
from recidiviz.justice_counts.metrics.metric_disaggregation_data import (
    MetricAggregatedDimensionData,
)
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.utils.constants import UploadMethod
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema.justice_counts.schema import (
    Datapoint,
    DatapointHistory,
    Report,
    ReportingFrequency,
    ReportStatus,
    UserAccount,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)


class TestDatapointInterface(JusticeCountsDatabaseTestCase):
    """Implements tests for the DatapointInterface."""

    def setUp(self) -> None:
        super().setUp()
        self.test_schema_objects = JusticeCountsSchemaTestObjects()

    def test_add_report_datapoint(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            monthly_report = self.test_schema_objects.test_report_monthly
            created_datetime = datetime.datetime.now()

            inserts: List[schema.Datapoint] = []
            updates: List[schema.Datapoint] = []
            histories: List[schema.DatapointHistory] = []
            DatapointInterface.add_report_datapoint(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=monthly_report,
                existing_datapoints_dict=ReportInterface.get_existing_datapoints_dict(
                    reports=[monthly_report]
                ),
                value="123abc",
                metric_definition_key=law_enforcement.funding.key,
                current_time=created_datetime,
                upload_method=UploadMethod.MANUAL_ENTRY,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()

            # Update datapoint with new value. Since we are adding two datapoints with
            # the same unique key, we need to insert/update them in two separate steps
            # (a.k.a. not in the same bulk update).
            DatapointInterface.add_report_datapoint(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=monthly_report,
                existing_datapoints_dict=ReportInterface.get_existing_datapoints_dict(
                    reports=[monthly_report]
                ),
                value="456def",
                metric_definition_key=law_enforcement.funding.key,
                current_time=created_datetime + datetime.timedelta(days=2),
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()

            report_id = monthly_report.id
        with SessionFactory.using_database(self.database_key) as session:
            datapoints = DatapointInterface.get_datapoints_by_report_ids(
                session, [report_id]
            )
            self.assertEqual(len(datapoints), 1)
            self.assertEqual(datapoints[0].value, "456def")
            self.assertEqual(datapoints[0].upload_method, "BULK_UPLOAD")
            self.assertEqual(
                datapoints[0].created_at,
                created_datetime,
            )

    def test_save_invalid_datapoint(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            monthly_report = self.test_schema_objects.test_report_monthly
            user = self.test_schema_objects.test_user_A
            session.add_all([monthly_report, user])
            current_time = datetime.datetime.now(tz=datetime.timezone.utc)

            inserts: List[schema.Datapoint] = []
            updates: List[schema.Datapoint] = []
            histories: List[schema.DatapointHistory] = []
            try:
                # When a report is in Draft mode, no errors are raised when the value is invalid
                DatapointInterface.add_report_datapoint(
                    session=session,
                    inserts=inserts,
                    updates=updates,
                    histories=histories,
                    existing_datapoints_dict=ReportInterface.get_existing_datapoints_dict(
                        reports=[monthly_report]
                    ),
                    report=monthly_report,
                    value="123abc",
                    user_account=user,
                    metric_definition_key=law_enforcement.funding.key,
                    current_time=current_time,
                    upload_method=UploadMethod.BULK_UPLOAD,
                )
                DatapointInterface.flush_report_datapoints(
                    session=session,
                    inserts=inserts,
                    updates=updates,
                    histories=histories,
                )
                session.commit()
                assert True
            except ValueError:
                assert False

            with self.assertRaises(JusticeCountsServerError):
                # When a report is Published, an errors is raised when the value is invalid
                monthly_report = session.query(Report).one()
                user = session.query(UserAccount).one()
                ReportInterface.update_report_metadata(
                    report=monthly_report,
                    editor_id=user.id,
                    status=ReportStatus.PUBLISHED.value,
                )
                DatapointInterface.add_report_datapoint(
                    session=session,
                    inserts=inserts,
                    updates=updates,
                    histories=histories,
                    existing_datapoints_dict=ReportInterface.get_existing_datapoints_dict(
                        reports=[monthly_report]
                    ),
                    report=monthly_report,
                    value="123abc",
                    user_account=user,
                    metric_definition_key=law_enforcement.funding.key,
                    current_time=current_time,
                    upload_method=UploadMethod.BULK_UPLOAD,
                )
                DatapointInterface.flush_report_datapoints(
                    session=session,
                    inserts=inserts,
                    updates=updates,
                    histories=histories,
                )
                session.commit()

    def test_get_datapoints(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            monthly_report = self.test_schema_objects.test_report_monthly
            user = self.test_schema_objects.test_user_A
            session.add_all([monthly_report, user])
            session.flush()
            session.refresh(monthly_report)
            current_time = datetime.datetime(2022, 6, 1)

            inserts: List[schema.Datapoint] = []
            updates: List[schema.Datapoint] = []
            histories: List[schema.DatapointHistory] = []
            DatapointInterface.add_report_datapoint(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                existing_datapoints_dict=ReportInterface.get_existing_datapoints_dict(
                    reports=[monthly_report]
                ),
                report=monthly_report,
                value="$123.0",
                user_account=user,
                metric_definition_key=law_enforcement.calls_for_service.key,
                current_time=current_time,
                agency=monthly_report.source,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()

            report_ids = [
                monthly_report.id,
            ]

            datapoints = DatapointInterface.get_datapoints_by_report_ids(
                session=session,
                report_ids=report_ids,
                include_contexts=False,
            )

            assert len(datapoints) == 1
            assert datapoints[0].value == "$123.0"

            datapoint_json = DatapointInterface.to_json_response(
                datapoint=datapoints[0],
                is_published=False,
                frequency=ReportingFrequency.MONTHLY,
                agency_name=monthly_report.source.name,
            )

            self.assertDictEqual(
                datapoint_json,
                {
                    "id": datapoints[0].id,
                    "report_id": monthly_report.id,
                    "agency_name": "Agency Alpha",
                    "frequency": "MONTHLY",
                    "start_date": datetime.date(2022, 6, 1),
                    "end_date": datetime.date(2022, 7, 1),
                    "metric_definition_key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                    "metric_display_name": "Calls for Service",
                    "disaggregation_display_name": None,
                    "dimension_display_name": None,
                    "value": "$123.0",
                    "old_value": None,
                    "is_published": False,
                },
            )

            DatapointInterface.add_report_datapoint(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                existing_datapoints_dict=ReportInterface.get_existing_datapoints_dict(
                    reports=[monthly_report]
                ),
                report=monthly_report,
                value=456.3,
                user_account=user,
                metric_definition_key=law_enforcement.calls_for_service.key,
                current_time=current_time,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()

            report_ids = [
                monthly_report.id,
            ]

            datapoints = DatapointInterface.get_datapoints_by_report_ids(
                session=session,
                report_ids=report_ids,
                include_contexts=False,
            )

            datapoint_json = DatapointInterface.to_json_response(
                datapoint=datapoints[0],
                is_published=False,
                frequency=ReportingFrequency.MONTHLY,
                old_value="$123.0",
                agency_name=monthly_report.source.name,
            )

            self.assertDictEqual(
                datapoint_json,
                {
                    "id": datapoints[0].id,
                    "report_id": monthly_report.id,
                    "agency_name": monthly_report.source.name,
                    "frequency": "MONTHLY",
                    "start_date": datetime.date(2022, 6, 1),
                    "end_date": datetime.date(2022, 7, 1),
                    "metric_definition_key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                    "metric_display_name": "Calls for Service",
                    "disaggregation_display_name": None,
                    "dimension_display_name": None,
                    "value": 456.3,
                    "old_value": "$123.0",
                    "is_published": False,
                },
            )

    def test_report_datapoints_last_updated(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_C
            user = self.test_schema_objects.test_user_C
            report = self.test_schema_objects.test_report_supervision
            session.add_all([user, agency, report])

            current_time = datetime.datetime.now(tz=datetime.timezone.utc)
            # Add 4 datapoints to the report
            existing_datapoints_dict = ReportInterface.get_existing_datapoints_dict(
                reports=[report]
            )
            inserts: List[schema.Datapoint] = []
            updates: List[schema.Datapoint] = []
            histories: List[schema.DatapointHistory] = []
            DatapointInterface.add_report_datapoint(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=report,
                existing_datapoints_dict=existing_datapoints_dict,
                value=100,
                metric_definition_key="SUPERVISION_POPULATION",
                current_time=current_time,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.add_report_datapoint(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=report,
                existing_datapoints_dict=existing_datapoints_dict,
                value=50,
                metric_definition_key="SUPERVISION_POPULATION",
                current_time=current_time,
                dimension=DailyPopulationType["ACTIVE"],
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.add_report_datapoint(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=report,
                existing_datapoints_dict=existing_datapoints_dict,
                value=20,
                metric_definition_key="SUPERVISION_POPULATION",
                current_time=current_time,
                dimension=DailyPopulationType["ADMINISTRATIVE"],
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.add_report_datapoint(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=report,
                existing_datapoints_dict=existing_datapoints_dict,
                value=30,
                metric_definition_key="SUPERVISION_POPULATION",
                current_time=current_time,
                dimension=DailyPopulationType["ABSCONDED"],
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()
            # Make sure datapoints are there
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 4)
            self.assertEqual(int(datapoints[0].value), 100)
            self.assertEqual(
                datapoints[0].created_at.astimezone(tz=datetime.timezone.utc),
                current_time,
            )
            self.assertEqual(
                datapoints[0].last_updated.astimezone(tz=datetime.timezone.utc),
                current_time,
            )
            self.assertEqual(int(datapoints[1].value), 50)
            self.assertEqual(
                datapoints[1].created_at.astimezone(tz=datetime.timezone.utc),
                current_time,
            )
            self.assertEqual(
                datapoints[1].last_updated.astimezone(tz=datetime.timezone.utc),
                current_time,
            )
            self.assertEqual(int(datapoints[2].value), 20)
            self.assertEqual(
                datapoints[2].created_at.astimezone(tz=datetime.timezone.utc),
                current_time,
            )
            self.assertEqual(
                datapoints[2].last_updated.astimezone(tz=datetime.timezone.utc),
                current_time,
            )
            self.assertEqual(int(datapoints[3].value), 30)
            self.assertEqual(
                datapoints[3].created_at.astimezone(tz=datetime.timezone.utc),
                current_time,
            )
            self.assertEqual(
                datapoints[3].last_updated.astimezone(tz=datetime.timezone.utc),
                current_time,
            )

            # Update datapoint values
            updated_time = datetime.datetime.now(tz=datetime.timezone.utc)
            existing_datapoints_dict = ReportInterface.get_existing_datapoints_dict(
                reports=[report]
            )
            DatapointInterface.add_report_datapoint(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=report,
                existing_datapoints_dict=existing_datapoints_dict,
                value=90,
                metric_definition_key="SUPERVISION_POPULATION",
                current_time=updated_time,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.add_report_datapoint(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=report,
                existing_datapoints_dict=existing_datapoints_dict,
                value=40,
                metric_definition_key="SUPERVISION_POPULATION",
                current_time=updated_time,
                dimension=DailyPopulationType["ACTIVE"],
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.add_report_datapoint(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=report,
                existing_datapoints_dict=existing_datapoints_dict,
                value=15,
                metric_definition_key="SUPERVISION_POPULATION",
                current_time=updated_time,
                dimension=DailyPopulationType["ADMINISTRATIVE"],
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.add_report_datapoint(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=report,
                existing_datapoints_dict=existing_datapoints_dict,
                value=35,
                metric_definition_key="SUPERVISION_POPULATION",
                current_time=updated_time,
                dimension=DailyPopulationType["ABSCONDED"],
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()
            # Make sure values and last_updated have changed
            # created_at should have remained the same
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 4)
            self.assertEqual(int(datapoints[0].value), 90)
            self.assertEqual(
                datapoints[0].created_at.astimezone(tz=datetime.timezone.utc),
                current_time,
            )
            self.assertEqual(
                datapoints[0].last_updated.astimezone(tz=datetime.timezone.utc),
                updated_time,
            )
            self.assertEqual(int(datapoints[1].value), 40)
            self.assertEqual(
                datapoints[1].created_at.astimezone(tz=datetime.timezone.utc),
                current_time,
            )
            self.assertEqual(
                datapoints[1].last_updated.astimezone(tz=datetime.timezone.utc),
                updated_time,
            )
            self.assertEqual(int(datapoints[2].value), 15)
            self.assertEqual(
                datapoints[2].created_at.astimezone(tz=datetime.timezone.utc),
                current_time,
            )
            self.assertEqual(
                datapoints[2].last_updated.astimezone(tz=datetime.timezone.utc),
                updated_time,
            )
            self.assertEqual(int(datapoints[3].value), 35)
            self.assertEqual(
                datapoints[3].created_at.astimezone(tz=datetime.timezone.utc),
                current_time,
            )
            self.assertEqual(
                datapoints[3].last_updated.astimezone(tz=datetime.timezone.utc),
                updated_time,
            )

            # If we update datapoints but they have the same values, ensure that
            # both created_at and last_updated do not change
            updated_time_2 = datetime.datetime.now(tz=datetime.timezone.utc)
            self.assertGreater(updated_time_2, updated_time)
            existing_datapoints_dict = ReportInterface.get_existing_datapoints_dict(
                reports=[report]
            )
            DatapointInterface.add_report_datapoint(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=report,
                existing_datapoints_dict=existing_datapoints_dict,
                value=90,
                metric_definition_key="SUPERVISION_POPULATION",
                current_time=updated_time_2,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.add_report_datapoint(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=report,
                existing_datapoints_dict=existing_datapoints_dict,
                value=40,
                metric_definition_key="SUPERVISION_POPULATION",
                current_time=updated_time_2,
                dimension=DailyPopulationType["ACTIVE"],
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.add_report_datapoint(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=report,
                existing_datapoints_dict=existing_datapoints_dict,
                value=15,
                metric_definition_key="SUPERVISION_POPULATION",
                current_time=updated_time_2,
                dimension=DailyPopulationType["ADMINISTRATIVE"],
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.add_report_datapoint(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=report,
                existing_datapoints_dict=existing_datapoints_dict,
                value=35,
                metric_definition_key="SUPERVISION_POPULATION",
                current_time=updated_time_2,
                dimension=DailyPopulationType["ABSCONDED"],
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()

            # Since new values are the same as old values, last_updated should not have
            # changed
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 4)
            self.assertEqual(int(datapoints[0].value), 90)
            self.assertEqual(
                datapoints[0].created_at.astimezone(tz=datetime.timezone.utc),
                current_time,
            )
            self.assertEqual(
                datapoints[0].last_updated.astimezone(tz=datetime.timezone.utc),
                updated_time,
            )
            self.assertEqual(int(datapoints[1].value), 40)
            self.assertEqual(
                datapoints[1].created_at.astimezone(tz=datetime.timezone.utc),
                current_time,
            )
            self.assertEqual(
                datapoints[1].last_updated.astimezone(tz=datetime.timezone.utc),
                updated_time,
            )
            self.assertEqual(int(datapoints[2].value), 15)
            self.assertEqual(
                datapoints[2].created_at.astimezone(tz=datetime.timezone.utc),
                current_time,
            )
            self.assertEqual(
                datapoints[2].last_updated.astimezone(tz=datetime.timezone.utc),
                updated_time,
            )
            self.assertEqual(int(datapoints[3].value), 35)
            self.assertEqual(
                datapoints[3].created_at.astimezone(tz=datetime.timezone.utc),
                current_time,
            )
            self.assertEqual(
                datapoints[3].last_updated.astimezone(tz=datetime.timezone.utc),
                updated_time,
            )

    ## Datapoint History tests.

    def test_record_change_history_for_report_datapoints(self) -> None:
        """
        Change the value of a report datapoint. Test that this change is recorded in the
        datapoint history table.
        """
        with SessionFactory.using_database(self.database_key) as session:
            monthly_report = self.test_schema_objects.test_report_monthly
            created_datetime = datetime.datetime.now()

            inserts: List[schema.Datapoint] = []
            updates: List[schema.Datapoint] = []
            histories: List[schema.DatapointHistory] = []
            DatapointInterface.add_report_datapoint(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=monthly_report,
                existing_datapoints_dict=ReportInterface.get_existing_datapoints_dict(
                    reports=[monthly_report]
                ),
                value="123abc",
                metric_definition_key=law_enforcement.funding.key,
                current_time=created_datetime,
                upload_method=UploadMethod.MANUAL_ENTRY,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()
            session.refresh(monthly_report)

            # No update is written for the first datapoint upload.
            datapoint_histories = session.query(DatapointHistory).all()
            self.assertEqual(len(datapoint_histories), 0)

            # Update datapoint with new value
            DatapointInterface.add_report_datapoint(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=monthly_report,
                existing_datapoints_dict=ReportInterface.get_existing_datapoints_dict(
                    reports=[monthly_report]
                ),
                value="456def",
                metric_definition_key=law_enforcement.funding.key,
                current_time=created_datetime + datetime.timedelta(days=2),
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()

            datapoint_histories = session.query(DatapointHistory).all()
            self.assertEqual(len(datapoint_histories), 1)
            datapoint = datapoint_histories[0]
            self.assertEqual(datapoint.old_value, "123abc")
            self.assertEqual(datapoint.new_value, "456def")
            self.assertEqual(datapoint.old_upload_method, "MANUAL_ENTRY")
            self.assertEqual(datapoint.new_upload_method, "BULK_UPLOAD")

    def test_join_for_aggregate_metric(self) -> None:
        """
        Tests that join_report_datapoints_to_metric_interfaces() correctly adds report
        datapoints that represent a metric's aggregate value to the metric interface.
        """
        metric_key_to_metric_interface = {
            prisons.funding.key: MetricInterface(
                key=prisons.funding.key,
            )
        }
        report_datapoints = [
            Datapoint(
                report_id=11111,
                metric_definition_key=prisons.funding.key,
                is_report_datapoint=True,
                value=10000,
            )
        ]
        result = DatapointInterface.join_report_datapoints_to_metric_interfaces(
            metric_key_to_metric_interface=metric_key_to_metric_interface,
            report_datapoints=report_datapoints,
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[prisons.funding.key].value, 10000)

    def test_join_with_disaggregated_metric_present(self) -> None:
        """
        Tests that join_report_datapoints_to_metric_interfaces() correctly adds report
        datapoints breakdown values to a metric's aggregate value to the metric interface.
        """
        metric_key_to_metric_interface = {
            prisons.funding.key: MetricInterface(
                key=prisons.funding.key,
                aggregated_dimensions=[
                    MetricAggregatedDimensionData(
                        dimension_to_enabled_status={FundingType.GRANTS: True}
                    ),
                ],
            )
        }
        report_datapoints = [
            Datapoint(
                report_id=11111,
                metric_definition_key=prisons.funding.key,
                is_report_datapoint=True,
                dimension_identifier_to_member={
                    "metric/prisons/funding/type": "GRANTS"
                },
                value=7000,
            ),
            Datapoint(
                report_id=11111,
                metric_definition_key=prisons.funding.key,
                is_report_datapoint=True,
                dimension_identifier_to_member={"metric/prisons/funding/type": "OTHER"},
                value=3000,
            ),
        ]
        result = DatapointInterface.join_report_datapoints_to_metric_interfaces(
            metric_key_to_metric_interface=metric_key_to_metric_interface,
            report_datapoints=report_datapoints,
        )

        self.assertEqual(len(result), 1)
        interface = result[prisons.funding.key]
        self.assertEqual(len(interface.aggregated_dimensions), 1)
        self.assertEqual(
            interface.aggregated_dimensions[0].dimension_to_value,
            {FundingType.GRANTS: 7000, FundingType.OTHER: 3000},
        )

    def test_join_handles_absent_metric_interface(self) -> None:
        """
        Tests that join_report_datapoints_to_metric_interfaces() will create an empty
        MetricInterface object if there is no metric interface in `metric_key_to_metric_interface`
        which matches datapoints `metric_definition_key`
        """
        metric_key_to_metric_interface: dict[str, MetricInterface] = {}
        report_datapoints = [
            Datapoint(
                report_id=11111,
                metric_definition_key=prisons.funding.key,
                is_report_datapoint=True,
                value=10000,
            )
        ]
        result = DatapointInterface.join_report_datapoints_to_metric_interfaces(
            metric_key_to_metric_interface=metric_key_to_metric_interface,
            report_datapoints=report_datapoints,
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[prisons.funding.key].value, 10000)

    def test_join_with_disaggregated_metric_not_present(self) -> None:
        """
        Tests that join_report_datapoints_to_metric_interfaces() correctly adds report
        datapoints breakdown values to a metric's aggregate value to the metric interface.
        This test case makes sure that we cover the (unlikely but still possible) case
        where a metric interface does not have agency settings saved for the breakdown
        metric but has a report datapoint for it anyway.
        """
        metric_key_to_metric_interface = {
            prisons.funding.key: MetricInterface(
                key=prisons.funding.key,
            )
        }
        report_datapoints = [
            Datapoint(
                report_id=11111,
                metric_definition_key=prisons.funding.key,
                is_report_datapoint=True,
                dimension_identifier_to_member={
                    "metric/prisons/funding/type": "GRANTS"
                },
                value=7000,
            )
        ]
        result = DatapointInterface.join_report_datapoints_to_metric_interfaces(
            metric_key_to_metric_interface=metric_key_to_metric_interface,
            report_datapoints=report_datapoints,
        )

        self.assertEqual(len(result), 1)
        interface = result[prisons.funding.key]
        self.assertEqual(len(interface.aggregated_dimensions), 1)
        self.assertEqual(
            interface.aggregated_dimensions[0].dimension_to_value,
            {FundingType.GRANTS: 7000},
        )

    def test_join_raises_error_if_not_report_datapoint(self) -> None:
        """
        Tests that join_report_datapoints_to_metric_interfaces() raises an error if a
        non-report datapoint is passed through `report_datapoints`.
        """
        metric_key_to_metric_interface = {
            prisons.funding.key: MetricInterface(
                key=prisons.funding.key,
            )
        }
        not_a_report_datapoint = [
            Datapoint(
                metric_definition_key=prisons.funding.key,
                is_report_datapoint=False,
                dimension_identifier_to_member={
                    "metric/prisons/funding/type": "GRANTS"
                },
                value=7000,
            )
        ]
        try:
            DatapointInterface.join_report_datapoints_to_metric_interfaces(
                metric_key_to_metric_interface=metric_key_to_metric_interface,
                report_datapoints=not_a_report_datapoint,
            )
            assert False
        except ValueError:
            assert True

    def test_to_dashboard_v2_json_response(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_C
            user = self.test_schema_objects.test_user_C
            report = self.test_schema_objects.test_report_supervision
            session.add_all([user, agency, report])

            current_time = datetime.datetime.now(tz=datetime.timezone.utc)
            # Add 4 datapoints to the report
            existing_datapoints_dict = ReportInterface.get_existing_datapoints_dict(
                reports=[report]
            )
            inserts: List[schema.Datapoint] = []
            updates: List[schema.Datapoint] = []
            histories: List[schema.DatapointHistory] = []
            DatapointInterface.add_report_datapoint(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=report,
                existing_datapoints_dict=existing_datapoints_dict,
                value=100,
                metric_definition_key="SUPERVISION_POPULATION",
                current_time=current_time,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.add_report_datapoint(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                report=report,
                existing_datapoints_dict=existing_datapoints_dict,
                value=50,
                metric_definition_key="SUPERVISION_POPULATION",
                current_time=current_time,
                dimension=DailyPopulationType["ACTIVE"],
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.flush_report_datapoints(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
            )
            session.commit()

            datapoints = DatapointInterface.get_datapoints_by_report_ids(
                session=session,
                report_ids=[report.id],
                include_contexts=False,
            )

            datapoint_json = DatapointInterface.to_json_response(
                datapoint=datapoints[0],
                frequency=ReportingFrequency.MONTHLY,
                is_published=True,
                is_v2=True,
            )

            self.assertDictEqual(
                datapoint_json,
                {
                    "id": datapoints[0].id,
                    "frequency": "MONTHLY",
                    "start_date": datetime.date(2022, 6, 1),
                    "end_date": datetime.date(2022, 7, 1),
                    "value": 100,
                },
            )

            datapoint_json = DatapointInterface.to_json_response(
                datapoint=datapoints[1],
                frequency=ReportingFrequency.MONTHLY,
                is_published=True,
                is_v2=True,
            )
            self.assertDictEqual(
                datapoint_json,
                {
                    "id": datapoints[1].id,
                    "frequency": "MONTHLY",
                    "start_date": datetime.date(2022, 6, 1),
                    "end_date": datetime.date(2022, 7, 1),
                    "disaggregation_display_name": "Daily Population Type",
                    "dimension_display_name": "People on Active Supervision",
                    "value": 50,
                },
            )
