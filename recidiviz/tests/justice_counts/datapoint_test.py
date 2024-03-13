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

from recidiviz.common.constants.justice_counts import ContextKey
from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.dimensions.law_enforcement import CallType
from recidiviz.justice_counts.dimensions.prisons import FundingType
from recidiviz.justice_counts.dimensions.supervision import DailyPopulationType
from recidiviz.justice_counts.exceptions import JusticeCountsServerError
from recidiviz.justice_counts.includes_excludes.prisons import (
    PrisonFundingTimeframeIncludesExcludes,
)
from recidiviz.justice_counts.metrics import law_enforcement, prisons, supervision
from recidiviz.justice_counts.metrics.custom_reporting_frequency import (
    CustomReportingFrequency,
)
from recidiviz.justice_counts.metrics.metric_context_data import MetricContextData
from recidiviz.justice_counts.metrics.metric_definition import IncludesExcludesSetting
from recidiviz.justice_counts.metrics.metric_disaggregation_data import (
    MetricAggregatedDimensionData,
)
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.utils.constants import (
    DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS,
    UploadMethod,
)
from recidiviz.justice_counts.utils.datapoint_utils import get_value
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

            DatapointInterface.add_report_datapoint(
                session=session,
                report=monthly_report,
                existing_datapoints_dict=ReportInterface.get_existing_datapoints_dict(
                    reports=[monthly_report]
                ),
                value="123abc",
                metric_definition_key=law_enforcement.funding.key,
                current_time=created_datetime,
                upload_method=UploadMethod.MANUAL_ENTRY,
            )
            session.commit()
            session.refresh(monthly_report)
            report_id = monthly_report.id

            # Update datapoint with new value
            DatapointInterface.add_report_datapoint(
                session=session,
                report=monthly_report,
                existing_datapoints_dict=ReportInterface.get_existing_datapoints_dict(
                    reports=[monthly_report]
                ),
                value="456def",
                metric_definition_key=law_enforcement.funding.key,
                current_time=created_datetime + datetime.timedelta(days=2),
                upload_method=UploadMethod.BULK_UPLOAD,
            )

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

    def test_save_agency_datapoints_turnoff_whole_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            user = self.test_schema_objects.test_user_A
            session.add_all([user, agency])
            agency_metric = self.test_schema_objects.get_agency_metric_interface(
                is_metric_enabled=False
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 4)
            self.assertEqual(datapoints[0].enabled, False)
            self.assertEqual(datapoints[0].metric_definition_key, agency_metric.key)
            self.assertEqual(datapoints[0].dimension_identifier_to_member, None)
            self.assertEqual(datapoints[0].context_key, None)

    def test_save_agency_datapoints_turnoff_disaggregation(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            user = self.test_schema_objects.test_user_A
            session.add_all([user, agency])
            agency_metric = self.test_schema_objects.get_agency_metric_interface(
                include_disaggregation=True
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), len(CallType))
            # top level metric
            self.assertEqual(datapoints[0].enabled, True)
            # dimensions
            self.assertEqual(datapoints[1].enabled, False)
            self.assertEqual(
                datapoints[1].dimension_identifier_to_member,
                {CallType.dimension_identifier(): "EMERGENCY"},
            )
            self.assertEqual(datapoints[2].enabled, False)
            self.assertEqual(
                datapoints[2].dimension_identifier_to_member,
                {CallType.dimension_identifier(): "NON_EMERGENCY"},
            )
            self.assertEqual(
                datapoints[3].dimension_identifier_to_member,
                {CallType.dimension_identifier(): "UNKNOWN"},
            )
            self.assertEqual(datapoints[3].enabled, False)

    def test_save_agency_datapoints_disable_single_breakdown(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            user = self.test_schema_objects.test_user_A
            session.add_all([user, agency])
            agency_metric = self.test_schema_objects.get_agency_metric_interface(
                use_partially_disabled_disaggregation=True, include_disaggregation=True
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 4)
            # top level metric
            self.assertEqual(datapoints[0].enabled, True)
            # dimensions
            self.assertEqual(datapoints[1].enabled, True)
            self.assertEqual(
                datapoints[1].dimension_identifier_to_member,
                {CallType.dimension_identifier(): "EMERGENCY"},
            )
            self.assertEqual(datapoints[2].enabled, False)
            self.assertEqual(
                datapoints[2].dimension_identifier_to_member,
                {CallType.dimension_identifier(): "NON_EMERGENCY"},
            )
            self.assertEqual(
                datapoints[3].dimension_identifier_to_member,
                {CallType.dimension_identifier(): "UNKNOWN"},
            )

            # Turn off EMERGENCY dimension as well
            updated_metric = self.test_schema_objects.get_agency_metric_interface(
                use_partially_disabled_disaggregation=False, include_disaggregation=True
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=updated_metric,
                agency=agency,
                session=session,
                user_account=user,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 4)
            # top level metric
            self.assertEqual(datapoints[0].enabled, True)
            # dimensions
            self.assertEqual(datapoints[1].enabled, False)
            self.assertEqual(
                datapoints[1].dimension_identifier_to_member,
                {CallType.dimension_identifier(): "NON_EMERGENCY"},
            )
            self.assertEqual(datapoints[2].enabled, False)
            self.assertEqual(
                datapoints[2].dimension_identifier_to_member,
                {CallType.dimension_identifier(): "UNKNOWN"},
            )
            self.assertEqual(datapoints[3].enabled, False)
            self.assertEqual(
                datapoints[3].dimension_identifier_to_member,
                {CallType.dimension_identifier(): "EMERGENCY"},
            )

    def test_save_agency_datapoints_reenable_breakdown(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            user = self.test_schema_objects.test_user_A
            session.add_all([user, agency])
            agency_metric = self.test_schema_objects.get_agency_metric_interface(
                include_disaggregation=True
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 4)
            self.assertEqual(datapoints[1].enabled, False)
            # Reenable DETENTION disaggregation
            agency_metric = self.test_schema_objects.get_agency_metric_interface(
                use_partially_disabled_disaggregation=True, include_disaggregation=True
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 4)
            self.assertEqual(datapoints[3].enabled, True)
            self.assertEqual(
                datapoints[3].dimension_identifier_to_member,
                {CallType.dimension_identifier(): "EMERGENCY"},
            )

    def test_save_agency_datapoints_reenable_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            user = self.test_schema_objects.test_user_A
            session.add_all([user, agency])
            agency_metric = self.test_schema_objects.get_agency_metric_interface(
                is_metric_enabled=False
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 4)
            self.assertEqual(datapoints[0].enabled, False)

            agency_metric = self.test_schema_objects.get_agency_metric_interface(
                is_metric_enabled=True
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 4)
            self.assertEqual(datapoints[3].enabled, True)
            self.assertEqual(
                datapoints[3].dimension_identifier_to_member,
                None,
            )

    def test_save_dimension_to_contexts(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_G
            user = self.test_schema_objects.test_user_A
            session.add_all([user, agency])
            agency_metric = MetricInterface(
                key=prisons.funding.key,
                aggregated_dimensions=[
                    MetricAggregatedDimensionData(
                        dimension_to_contexts={
                            FundingType.OTHER: [
                                MetricContextData(
                                    key=ContextKey["ADDITIONAL_CONTEXT"],
                                    value="Other user entered text...",
                                )
                            ],
                            FundingType.UNKNOWN: [
                                MetricContextData(
                                    key=ContextKey["ADDITIONAL_CONTEXT"],
                                    value="Unknown user entered text...",
                                )
                            ],
                        }
                    ),
                ],
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 2)

            other_datapoint = datapoints[0]
            self.assertEqual(other_datapoint.context_key, "ADDITIONAL_CONTEXT")
            self.assertEqual(other_datapoint.value, "Other user entered text...")
            self.assertEqual(
                other_datapoint.dimension_identifier_to_member,
                {"metric/prisons/funding/type": "OTHER"},
            )

            unknown_datapoint = datapoints[1]
            self.assertEqual(unknown_datapoint.context_key, "ADDITIONAL_CONTEXT")
            self.assertEqual(unknown_datapoint.value, "Unknown user entered text...")
            self.assertEqual(
                unknown_datapoint.dimension_identifier_to_member,
                {"metric/prisons/funding/type": "UNKNOWN"},
            )

            # Test build_metric_key_to_datapoints() and get_aggregated_dimension_data()
            metric_key_to_data_points = (
                DatapointInterface.build_metric_key_to_datapoints(datapoints)
            )
            metric_datapoints = metric_key_to_data_points[
                other_datapoint.metric_definition_key
            ]
            self.assertEqual(
                metric_datapoints.dimension_to_context_key_to_datapoints,
                {
                    FundingType.OTHER: {other_datapoint.context_key: other_datapoint},
                    FundingType.UNKNOWN: {
                        unknown_datapoint.context_key: unknown_datapoint
                    },
                },
            )

            agg_dims = metric_datapoints.get_aggregated_dimension_data(
                agency_metric.metric_definition
            )
            self.assertEqual(
                agg_dims[0].dimension_to_contexts,
                {
                    FundingType.OTHER: [
                        MetricContextData(
                            key=ContextKey(other_datapoint.context_key),
                            value=get_value(datapoint=other_datapoint),
                        )
                    ],
                    FundingType.UNKNOWN: [
                        MetricContextData(
                            key=ContextKey(unknown_datapoint.context_key),
                            value=get_value(datapoint=unknown_datapoint),
                        )
                    ],
                },
            )

    def test_save_agency_datapoints_contexts(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            user = self.test_schema_objects.test_user_A
            session.add_all([user, agency])
            agency_metric = self.test_schema_objects.get_agency_metric_interface(
                include_contexts=True, include_disaggregation=True
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )
            datapoints = session.query(Datapoint).all()
            # 1 top level datapoint, 1 context datapoint, 3 disaggregation datapoints
            self.assertEqual(len(datapoints), 5)
            for datapoint in datapoints:
                if datapoint.context_key is None:
                    if datapoint.dimension_identifier_to_member is None:
                        self.assertEqual(datapoint.enabled, True)
                    else:
                        self.assertEqual(datapoint.enabled, False)
                else:
                    self.assertEqual(
                        datapoint.context_key,
                        ContextKey.INCLUDES_EXCLUDES_DESCRIPTION.value,
                    )
                    self.assertEqual(
                        datapoint.value, "our metrics are different because xyz"
                    )

    def test_save_includes_excludes_histories(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            user = self.test_schema_objects.test_user_A
            session.add_all([user, agency])
            agency_metric = MetricInterface(
                key=prisons.funding.key,
                includes_excludes_member_to_setting={
                    member: IncludesExcludesSetting.NO
                    for member in PrisonFundingTimeframeIncludesExcludes
                },
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )
            datapoints = session.query(Datapoint).all()
            # New includes/excludes datapoint for each member of PrisonFundingTimeframeIncludesExcludes
            self.assertEqual(
                len(datapoints), len(PrisonFundingTimeframeIncludesExcludes)
            )
            datapoint_histories = session.query(DatapointHistory).all()
            self.assertEqual(len(datapoint_histories), 0)
            agency_metric = MetricInterface(
                key=prisons.funding.key,
                includes_excludes_member_to_setting={
                    member: IncludesExcludesSetting.YES
                    for member in PrisonFundingTimeframeIncludesExcludes
                },
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )
            datapoints = session.query(Datapoint).all()
            # New includes/excludes datapoint for each member of PrisonFundingTimeframeIncludesExcludes
            self.assertEqual(
                len(datapoints), len(PrisonFundingTimeframeIncludesExcludes)
            )
            datapoint_histories = session.query(DatapointHistory).all()
            self.assertEqual(
                len(datapoint_histories), len(PrisonFundingTimeframeIncludesExcludes)
            )
            for datapoint in datapoint_histories:
                self.assertEqual(datapoint.old_value, "No")
                self.assertEqual(datapoint.new_value, "Yes")

    def test_save_custom_reporting_frequency(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            user = self.test_schema_objects.test_user_A
            session.add_all([user, agency])
            agency_metric = MetricInterface(
                key=law_enforcement.funding.key,
                custom_reporting_frequency=CustomReportingFrequency(
                    frequency=ReportingFrequency.ANNUAL, starting_month=3
                ),
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 1)
            # Annual frequency starting in March
            self.assertEqual(
                datapoints[0].value,
                '{"custom_frequency": "ANNUAL", "starting_month": 3}',
            )

    def test_save_disaggregated_by_supervision_subsystems_boolean(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_E
            user = self.test_schema_objects.test_user_A
            session.add_all([user, agency])
            agency_metric = MetricInterface(
                key=supervision.funding.key,
                disaggregated_by_supervision_subsystems=True,
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 6)
            context_datapoints = [dp for dp in datapoints if dp.context_key is not None]
            enabled_disabled_datapoints = [
                dp for dp in datapoints if dp.context_key is None
            ]
            for datapoint in context_datapoints:
                self.assertEqual(datapoint.value, "True")
                self.assertEqual(
                    datapoint.context_key, DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS
                )
            for datapoint in enabled_disabled_datapoints:
                if "SUPERVISION" in datapoint.metric_definition_key:
                    self.assertFalse(datapoint.enabled)
                else:
                    self.assertTrue(datapoint.enabled)

            agency_metric = MetricInterface(
                key=supervision.funding.key,
                disaggregated_by_supervision_subsystems=False,
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 6)
            context_datapoints = [dp for dp in datapoints if dp.context_key is not None]
            enabled_disabled_datapoints = [
                dp for dp in datapoints if dp.context_key is None
            ]
            for datapoint in context_datapoints:
                self.assertEqual(datapoint.value, "False")
                self.assertEqual(
                    datapoint.context_key, DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS
                )
            for datapoint in enabled_disabled_datapoints:
                if "SUPERVISION" in datapoint.metric_definition_key:
                    self.assertTrue(datapoint.enabled)
                else:
                    self.assertFalse(datapoint.enabled)

    def test_save_invalid_datapoint(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            monthly_report = self.test_schema_objects.test_report_monthly
            user = self.test_schema_objects.test_user_A
            session.add_all([monthly_report, user])
            current_time = datetime.datetime.now(tz=datetime.timezone.utc)

            try:
                # When a report is in Draft mode, no errors are raised when the value is invalid
                DatapointInterface.add_report_datapoint(
                    session=session,
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

    def test_get_disaggregated_by_supervision_subsystems_agency_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            supervision_agency = self.test_schema_objects.test_agency_C
            law_enforcement_agency = self.test_schema_objects.test_agency_A
            session.add_all([supervision_agency, law_enforcement_agency])
            session.flush()
            session.refresh(supervision_agency)
            session.refresh(law_enforcement_agency)
            supervision_agency_metrics = (
                DatapointInterface.get_metric_settings_by_agency(
                    session=session, agency=supervision_agency
                )
            )

            law_enforcement_agency_metrics = (
                DatapointInterface.get_metric_settings_by_agency(
                    session=session, agency=law_enforcement_agency
                )
            )

            for metric in supervision_agency_metrics:
                # Since the agency is a supervision agency, the disaggregated_by_supervision_subsystems
                # field will default to False
                self.assertEqual(metric.disaggregated_by_supervision_subsystems, False)

            for metric in law_enforcement_agency_metrics:
                # Since the agency is NOT a supervision agency, the disaggregated_by_supervision_subsystems
                # field will default to None
                self.assertEqual(metric.disaggregated_by_supervision_subsystems, None)

            # Add agency datapoint that makes the supervision funding metric be
            # disaggregated by subsystem
            session.add(
                Datapoint(
                    metric_definition_key=supervision.funding.key,
                    source=supervision_agency,
                    context_key=DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS,
                    dimension_identifier_to_member=None,
                    value=str(True),
                    is_report_datapoint=False,
                ),
            )
            session.flush()

            supervision_agency_metrics = (
                DatapointInterface.get_metric_settings_by_agency(
                    session=session, agency=supervision_agency
                )
            )
            for metric in supervision_agency_metrics:
                # All metrics except for funding should have a
                # disaggregated_by_supervision_subsystems field as False,
                # budget should be True.
                if metric.key != supervision.funding.key:
                    self.assertEqual(
                        metric.disaggregated_by_supervision_subsystems, False
                    )
                else:
                    self.assertEqual(
                        metric.disaggregated_by_supervision_subsystems, True
                    )

    def test_get_datapoints(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            monthly_report = self.test_schema_objects.test_report_monthly
            user = self.test_schema_objects.test_user_A
            session.add_all([monthly_report, user])
            session.flush()
            session.refresh(monthly_report)
            current_time = datetime.datetime(2022, 6, 1)

            DatapointInterface.add_report_datapoint(
                session=session,
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
            DatapointInterface.add_report_datapoint(
                session=session,
                report=report,
                existing_datapoints_dict=existing_datapoints_dict,
                value=100,
                metric_definition_key="SUPERVISION_POPULATION",
                current_time=current_time,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.add_report_datapoint(
                session=session,
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
                report=report,
                existing_datapoints_dict=existing_datapoints_dict,
                value=30,
                metric_definition_key="SUPERVISION_POPULATION",
                current_time=current_time,
                dimension=DailyPopulationType["ABSCONDED"],
                upload_method=UploadMethod.BULK_UPLOAD,
            )

            # Make sure datapoints are there
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 4)
            self.assertEqual(int(datapoints[0].value), 100)
            self.assertEqual(datapoints[0].created_at, current_time)
            self.assertEqual(datapoints[0].last_updated, current_time)
            self.assertEqual(int(datapoints[1].value), 50)
            self.assertEqual(datapoints[1].created_at, current_time)
            self.assertEqual(datapoints[1].last_updated, current_time)
            self.assertEqual(int(datapoints[2].value), 20)
            self.assertEqual(datapoints[2].created_at, current_time)
            self.assertEqual(datapoints[2].last_updated, current_time)
            self.assertEqual(int(datapoints[3].value), 30)
            self.assertEqual(datapoints[3].created_at, current_time)
            self.assertEqual(datapoints[3].last_updated, current_time)

            # Update datapoint values
            updated_time = datetime.datetime.now(tz=datetime.timezone.utc)
            existing_datapoints_dict = ReportInterface.get_existing_datapoints_dict(
                reports=[report]
            )
            DatapointInterface.add_report_datapoint(
                session=session,
                report=report,
                existing_datapoints_dict=existing_datapoints_dict,
                value=90,
                metric_definition_key="SUPERVISION_POPULATION",
                current_time=updated_time,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.add_report_datapoint(
                session=session,
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
                report=report,
                existing_datapoints_dict=existing_datapoints_dict,
                value=35,
                metric_definition_key="SUPERVISION_POPULATION",
                current_time=updated_time,
                dimension=DailyPopulationType["ABSCONDED"],
                upload_method=UploadMethod.BULK_UPLOAD,
            )

            # Make sure values and last_updated have changed
            # created_at should have remained the same
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 4)
            self.assertEqual(int(datapoints[0].value), 90)
            self.assertEqual(datapoints[0].created_at, current_time)
            self.assertEqual(datapoints[0].last_updated, updated_time)
            self.assertEqual(int(datapoints[1].value), 40)
            self.assertEqual(datapoints[1].created_at, current_time)
            self.assertEqual(datapoints[1].last_updated, updated_time)
            self.assertEqual(int(datapoints[2].value), 15)
            self.assertEqual(datapoints[2].created_at, current_time)
            self.assertEqual(datapoints[2].last_updated, updated_time)
            self.assertEqual(int(datapoints[3].value), 35)
            self.assertEqual(datapoints[3].created_at, current_time)
            self.assertEqual(datapoints[3].last_updated, updated_time)

            # If we update datapoints but they have the same values, ensure that
            # both created_at and last_updated do not change
            updated_time_2 = datetime.datetime.now(tz=datetime.timezone.utc)
            self.assertGreater(updated_time_2, updated_time)
            existing_datapoints_dict = ReportInterface.get_existing_datapoints_dict(
                reports=[report]
            )
            DatapointInterface.add_report_datapoint(
                session=session,
                report=report,
                existing_datapoints_dict=existing_datapoints_dict,
                value=90,
                metric_definition_key="SUPERVISION_POPULATION",
                current_time=updated_time_2,
                upload_method=UploadMethod.BULK_UPLOAD,
            )
            DatapointInterface.add_report_datapoint(
                session=session,
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
                report=report,
                existing_datapoints_dict=existing_datapoints_dict,
                value=35,
                metric_definition_key="SUPERVISION_POPULATION",
                current_time=updated_time_2,
                dimension=DailyPopulationType["ABSCONDED"],
                upload_method=UploadMethod.BULK_UPLOAD,
            )

            # Since new values are the same as old values, last_updated should not have
            # changed
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 4)
            self.assertEqual(int(datapoints[0].value), 90)
            self.assertEqual(datapoints[0].created_at, current_time)
            self.assertEqual(datapoints[0].last_updated, updated_time)
            self.assertEqual(int(datapoints[1].value), 40)
            self.assertEqual(datapoints[1].created_at, current_time)
            self.assertEqual(datapoints[1].last_updated, updated_time)
            self.assertEqual(int(datapoints[2].value), 15)
            self.assertEqual(datapoints[2].created_at, current_time)
            self.assertEqual(datapoints[2].last_updated, updated_time)
            self.assertEqual(int(datapoints[3].value), 35)
            self.assertEqual(datapoints[3].created_at, current_time)
            self.assertEqual(datapoints[3].last_updated, updated_time)

    def test_agency_datapoints_last_updated(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_C
            user = self.test_schema_objects.test_user_C
            session.add_all([user, agency])

            # First, test enabled/disabled metric
            agency_metric_enabled = (
                self.test_schema_objects.get_agency_metric_interface(
                    include_contexts=False,
                    include_disaggregation=False,
                    use_partially_disabled_disaggregation=False,
                    is_metric_enabled=True,
                )
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric_enabled,
                agency=agency,
                session=session,
                user_account=user,
            )

            # Make sure datapoint is there
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 1)
            self.assertEqual(datapoints[0].value, None)

            # sqlalchemy reads datetimes back using local timezone
            # need to convert these to utc
            first_created_at = (
                datapoints[0]
                .created_at.replace(tzinfo=None)
                .astimezone(tz=datetime.timezone.utc)
            )
            first_updated_at = (
                datapoints[0]
                .last_updated.replace(tzinfo=None)
                .astimezone(tz=datetime.timezone.utc)
            )
            self.assertEqual(first_created_at, first_updated_at)

            # Disabled the metric
            # created_at should stay the same
            # last_updated should change
            agency_metric_disabled = (
                self.test_schema_objects.get_agency_metric_interface(
                    include_contexts=False,
                    include_disaggregation=False,
                    use_partially_disabled_disaggregation=False,
                    is_metric_enabled=False,
                )
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric_disabled,
                agency=agency,
                session=session,
                user_account=user,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 4)  # now includes disaggregations as well
            self.assertEqual(datapoints[0].value, None)

            # sqlalchemy reads datetimes back using local timezone
            # need to convert these to utc
            second_created_at = (
                datapoints[0]
                .created_at.replace(tzinfo=None)
                .astimezone(tz=datetime.timezone.utc)
            )
            self.assertEqual(first_created_at, second_created_at)
            second_updated_at = (
                datapoints[0]
                .last_updated.replace(tzinfo=None)
                .astimezone(tz=datetime.timezone.utc)
            )
            self.assertGreater(second_updated_at, first_updated_at)

            # Second, test Context datapoint
            agency_metric_context = (
                self.test_schema_objects.get_agency_metric_interface(
                    include_contexts=True,
                    include_disaggregation=False,
                    use_partially_disabled_disaggregation=False,
                    is_metric_enabled=True,
                )
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric_context,
                agency=agency,
                session=session,
                user_account=user,
            )

            # Make sure datapoint is there
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 5)
            self.assertEqual(
                datapoints[4].value, "our metrics are different because xyz"
            )
            first_context_created_at = (
                datapoints[4]
                .created_at.replace(tzinfo=None)
                .astimezone(tz=datetime.timezone.utc)
            )
            first_context_updated_at = (
                datapoints[4]
                .last_updated.replace(tzinfo=None)
                .astimezone(tz=datetime.timezone.utc)
            )
            self.assertEqual(first_context_created_at, first_context_updated_at)

            # change the context value
            # created_at should stay the same
            # last_updated should change
            agency_metric_context.contexts[0].value = "new additional contexts."
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric_context,
                agency=agency,
                session=session,
                user_account=user,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 5)
            self.assertEqual(datapoints[4].value, "new additional contexts.")

            second_context_created_at = (
                datapoints[4]
                .created_at.replace(tzinfo=None)
                .astimezone(tz=datetime.timezone.utc)
            )
            second_context_updated_at = (
                datapoints[4]
                .last_updated.replace(tzinfo=None)
                .astimezone(tz=datetime.timezone.utc)
            )
            self.assertEqual(first_context_created_at, second_context_created_at)
            self.assertGreater(second_context_updated_at, first_context_updated_at)

    ## Datapoint History tests.

    def test_record_change_history_for_report_datapoints(self) -> None:
        """
        Change the value of a report datapoint. Test that this change is recorded in the
        datapoint history table.
        """
        with SessionFactory.using_database(self.database_key) as session:
            monthly_report = self.test_schema_objects.test_report_monthly
            created_datetime = datetime.datetime.now()

            DatapointInterface.add_report_datapoint(
                session=session,
                report=monthly_report,
                existing_datapoints_dict=ReportInterface.get_existing_datapoints_dict(
                    reports=[monthly_report]
                ),
                value="123abc",
                metric_definition_key=law_enforcement.funding.key,
                current_time=created_datetime,
                upload_method=UploadMethod.MANUAL_ENTRY,
            )
            session.commit()
            session.refresh(monthly_report)

            # No update is written for the first datapoint upload.
            datapoint_histories = session.query(DatapointHistory).all()
            self.assertEqual(len(datapoint_histories), 0)

            # Update datapoint with new value
            DatapointInterface.add_report_datapoint(
                session=session,
                report=monthly_report,
                existing_datapoints_dict=ReportInterface.get_existing_datapoints_dict(
                    reports=[monthly_report]
                ),
                value="456def",
                metric_definition_key=law_enforcement.funding.key,
                current_time=created_datetime + datetime.timedelta(days=2),
                upload_method=UploadMethod.BULK_UPLOAD,
            )

            datapoint_histories = session.query(DatapointHistory).all()
            self.assertEqual(len(datapoint_histories), 1)
            datapoint = datapoint_histories[0]
            self.assertEqual(datapoint.old_value, "123abc")
            self.assertEqual(datapoint.new_value, "456def")
            self.assertEqual(datapoint.old_upload_method, "MANUAL_ENTRY")
            self.assertEqual(datapoint.new_upload_method, "BULK_UPLOAD")

    def test_record_enable_disable_metric_changes(self) -> None:
        """
        Disable a previosly-enabled metric. Test that this change is recorded in the
        datapoint history table.
        """
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_C
            user = self.test_schema_objects.test_user_C
            session.add_all([user, agency])
            # Add enabled metric datapoint.
            agency_metric_enabled = (
                self.test_schema_objects.get_agency_metric_interface(
                    include_contexts=False,
                    include_disaggregation=False,
                    use_partially_disabled_disaggregation=False,
                    is_metric_enabled=True,
                )
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric_enabled,
                agency=agency,
                session=session,
                user_account=user,
            )

            # No update is written for the first datapoint upload.
            datapoint_histories = session.query(DatapointHistory).all()
            self.assertEqual(len(datapoint_histories), 0)

            # Disable the metric.
            agency_metric_disabled = (
                self.test_schema_objects.get_agency_metric_interface(
                    include_contexts=False,
                    include_disaggregation=False,
                    use_partially_disabled_disaggregation=False,
                    is_metric_enabled=False,
                )
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric_disabled,
                agency=agency,
                session=session,
                user_account=user,
            )

            histories = session.query(DatapointHistory).all()
            self.assertEqual(len(histories), 1)
            history = histories[0]
            self.assertEqual(history.old_enabled, True)
            self.assertEqual(history.new_enabled, False)
            ## Enable/disable metric histories will have these values as None.
            self.assertEqual(history.old_value, None)
            self.assertEqual(history.new_value, None)
            self.assertEqual(history.old_upload_method, None)
            self.assertEqual(history.new_upload_method, None)

    def test_record_metric_context_updates(self) -> None:
        """
        Modify a metric's context (note, this is the top-level metric's context and not
        the metric breakdown's context). Test that this change is recorded in the
        datapoint history table.
        """
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_C
            user = self.test_schema_objects.test_user_C
            session.add_all([user, agency])
            agency_metric_context = (
                self.test_schema_objects.get_agency_metric_interface(
                    include_contexts=True,
                    include_disaggregation=False,
                    use_partially_disabled_disaggregation=False,
                    is_metric_enabled=True,
                )
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric_context,
                agency=agency,
                session=session,
                user_account=user,
            )

            # No update is written for the first datapoint upload.
            datapoint_histories = session.query(DatapointHistory).all()
            self.assertEqual(len(datapoint_histories), 0)

            # Change the metric's context value.
            agency_metric_context.contexts[0].value = "new additional contexts."
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric_context,
                agency=agency,
                session=session,
                user_account=user,
            )
            histories = session.query(DatapointHistory).all()
            self.assertEqual(len(histories), 1)
            history = histories[0]
            ## Enable/disable metric histories will have these values as None.
            self.assertEqual(history.old_value, "our metrics are different because xyz")
            self.assertEqual(history.new_value, "new additional contexts.")
            self.assertEqual(history.old_upload_method, None)
            self.assertEqual(history.new_upload_method, None)
            self.assertEqual(history.old_enabled, None)
            self.assertEqual(history.new_enabled, None)

    def test_record_custom_reporting_frequency_updates(self) -> None:
        """
        Modify a metric's reporting frequency from ANNUAL to MONTHLY.
        Test that this change is recorded in datapoint history.
        """
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            user = self.test_schema_objects.test_user_A
            session.add_all([user, agency])
            # Set metric's frequency to annual.
            agency_metric = MetricInterface(
                key=law_enforcement.funding.key,
                custom_reporting_frequency=CustomReportingFrequency(
                    frequency=ReportingFrequency.ANNUAL, starting_month=3
                ),
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )

            # No update is written for the first datapoint upload.
            datapoint_histories = session.query(DatapointHistory).all()
            self.assertEqual(len(datapoint_histories), 0)

            # Update the metric's frequency to monthly.
            agency_metric = MetricInterface(
                key=law_enforcement.funding.key,
                custom_reporting_frequency=CustomReportingFrequency(
                    frequency=ReportingFrequency.MONTHLY
                ),
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )

            histories = session.query(DatapointHistory).all()
            self.assertEqual(len(histories), 1)
            history = histories[0]
            self.assertEqual(
                history.old_value, '{"custom_frequency": "ANNUAL", "starting_month": 3}'
            )
            self.assertEqual(
                history.new_value,
                '{"custom_frequency": "MONTHLY", "starting_month": null}',
            )
            self.assertEqual(history.old_upload_method, None)
            self.assertEqual(history.new_upload_method, None)
            self.assertEqual(history.old_enabled, None)
            self.assertEqual(history.new_enabled, None)

    def test_record_disaggregated_by_supervision_subsystems_updates(self) -> None:
        """
        Flip the DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS boolean from True to False.
        Test that this generates 6 datapoint history entries:
            * 3 entries (one for the supervision system and two for the subsystems)
                record that the disaggregated by supervision subsystems boolean was
                flipped from True to False.
            * 2 entries (one for each of the two subsystems the agency belongs to)
                record that the metric changed from enabled to disabled.
            * 1 entry for the supervision system records that the metric changed from
                disabled to enabled.
        """
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_E
            user = self.test_schema_objects.test_user_A
            session.add_all([user, agency])
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=MetricInterface(
                    key=supervision.funding.key,
                    disaggregated_by_supervision_subsystems=True,
                ),
                agency=agency,
                session=session,
                user_account=user,
            )

            # No update is written for the first datapoint upload.
            datapoint_histories = session.query(DatapointHistory).all()
            self.assertEqual(len(datapoint_histories), 0)

            # Change `disaggregated_by_supervision_subsystems` to false.
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=MetricInterface(
                    key=supervision.funding.key,
                    disaggregated_by_supervision_subsystems=False,
                ),
                agency=agency,
                session=session,
                user_account=user,
            )

            histories = session.query(DatapointHistory).all()
            self.assertEqual(len(histories), 6)

            # 3 entries (one for the supervision system and two for the subsystems)
            # recording that the disaggregated by supervision subsystems boolean was
            # flipped from True to False.
            disagg_histories = [dp for dp in histories if dp.new_value is not None]
            self.assertEqual(len(disagg_histories), 3)
            for history in disagg_histories:
                self.assertEqual(history.old_value, "True")
                self.assertEqual(history.new_value, "False")
                self.assertEqual(history.old_upload_method, None)
                self.assertEqual(history.new_upload_method, None)
                self.assertEqual(history.old_enabled, None)
                self.assertEqual(history.new_enabled, None)

            # 2 entries (one for each of the two subsystems the agency belongs to)
            # recording that the metric changed from enabled to disabled.
            subsystem_disabled_histories = [
                dp for dp in histories if dp.new_enabled is False
            ]
            self.assertEqual(len(subsystem_disabled_histories), 2)
            for history in subsystem_disabled_histories:
                self.assertEqual(history.old_enabled, True)
                self.assertEqual(history.new_enabled, False)
                self.assertEqual(history.old_value, None)
                self.assertEqual(history.new_value, None)
                self.assertEqual(history.old_upload_method, None)
                self.assertEqual(history.new_upload_method, None)

            # 1 entry for the supervision system recording that the metric changed from
            # disabled to enabled.
            supervision_enabled = [dp for dp in histories if dp.new_enabled is True]
            self.assertEqual(len(supervision_enabled), 1)
            for history in supervision_enabled:
                self.assertEqual(history.old_enabled, False)
                self.assertEqual(history.new_enabled, True)
                self.assertEqual(history.old_value, None)
                self.assertEqual(history.new_value, None)
                self.assertEqual(history.old_upload_method, None)
                self.assertEqual(history.new_upload_method, None)

    def test_record_breakdown_context_updates(self) -> None:
        """
        Modify a metric dimension's context. Test that this results in an addition to
        the datapoint history table.
        """
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_G
            user = self.test_schema_objects.test_user_A
            session.add_all([user, agency])
            agency_metric = MetricInterface(
                key=prisons.funding.key,
                aggregated_dimensions=[
                    MetricAggregatedDimensionData(
                        dimension_to_contexts={
                            FundingType.OTHER: [
                                MetricContextData(
                                    key=ContextKey["ADDITIONAL_CONTEXT"],
                                    value="Old and boring context description.",
                                )
                            ],
                        }
                    ),
                ],
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )

            # No update is written for the first datapoint upload.
            datapoint_histories = session.query(DatapointHistory).all()
            self.assertEqual(len(datapoint_histories), 0)

            agency_metric = MetricInterface(
                key=prisons.funding.key,
                aggregated_dimensions=[
                    MetricAggregatedDimensionData(
                        dimension_to_contexts={
                            FundingType.OTHER: [
                                MetricContextData(
                                    key=ContextKey["ADDITIONAL_CONTEXT"],
                                    value="New and fun context description.",
                                )
                            ],
                        }
                    ),
                ],
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )

            histories = session.query(DatapointHistory).all()
            self.assertEqual(len(histories), 1)
            history = histories[0]
            self.assertEqual(history.old_value, "Old and boring context description.")
            self.assertEqual(history.new_value, "New and fun context description.")
            self.assertEqual(history.old_enabled, None)
            self.assertEqual(history.new_enabled, None)
            self.assertEqual(history.old_upload_method, None)
            self.assertEqual(history.new_upload_method, None)

    def test_record_enable_disable_breakdown_updates(self) -> None:
        """
        Change one breakdown metric from enabled to disabled. Test that this results
        in an addition to the datapoint history table.
        """
        with SessionFactory.using_database(self.database_key) as session:
            # Enables EMERGENCY dimension, but leaves the NON_EMERGENCY and UNKNOWN
            # dimensions disabled.
            agency = self.test_schema_objects.test_agency_A
            user = self.test_schema_objects.test_user_A
            session.add_all([user, agency])
            agency_metric = self.test_schema_objects.get_agency_metric_interface(
                use_partially_disabled_disaggregation=True, include_disaggregation=True
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )

            # No update is written for the first datapoint upload.
            datapoint_histories = session.query(DatapointHistory).all()
            self.assertEqual(len(datapoint_histories), 0)

            # Disable EMERGENCY dimension as well.
            updated_metric = self.test_schema_objects.get_agency_metric_interface(
                use_partially_disabled_disaggregation=False, include_disaggregation=True
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=updated_metric,
                agency=agency,
                session=session,
                user_account=user,
            )
            # The change is stored in datapoint history.
            histories = session.query(DatapointHistory).all()
            self.assertEqual(len(histories), 1)
            history = histories[0]
            self.assertEqual(history.old_enabled, True)
            self.assertEqual(history.new_enabled, False)
            self.assertEqual(history.old_value, None)
            self.assertEqual(history.new_value, None)
            self.assertEqual(history.old_upload_method, None)
            self.assertEqual(history.new_upload_method, None)

    def test_join_for_aggregate_metric(self) -> None:
        """
        Tests that join_report_datapoints_to_metric_interfaces() correctly adds report
        datapoints that represent a metric's aggregate value to the metric interface.
        """
        metric_interfaces_by_key = {
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
            metric_interfaces_by_key=metric_interfaces_by_key,
            report_datapoints=report_datapoints,
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[prisons.funding.key].value, 10000)

    def test_join_with_disaggregated_metric_present(self) -> None:
        """
        Tests that join_report_datapoints_to_metric_interfaces() correctly adds report
        datapoints breakdown values to a metric's aggregate value to the metric interface.
        """
        metric_interfaces_by_key = {
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
            metric_interfaces_by_key=metric_interfaces_by_key,
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
        MetricInterface object if there is no metric interface in `metric_interfaces_by_key`
        which matches datapoints `metric_definition_key`
        """
        metric_interfaces_by_key: dict[str, MetricInterface] = {}
        report_datapoints = [
            Datapoint(
                report_id=11111,
                metric_definition_key=prisons.funding.key,
                is_report_datapoint=True,
                value=10000,
            )
        ]
        result = DatapointInterface.join_report_datapoints_to_metric_interfaces(
            metric_interfaces_by_key=metric_interfaces_by_key,
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
        metric_interfaces_by_key = {
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
            metric_interfaces_by_key=metric_interfaces_by_key,
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
        metric_interfaces_by_key = {
            prisons.funding.key: MetricInterface(
                key=prisons.funding.key,
            )
        }
        report_datapoints = [
            Datapoint(
                report_id=None,
                metric_definition_key=prisons.funding.key,
                is_report_datapoint=True,
                dimension_identifier_to_member={
                    "metric/prisons/funding/type": "GRANTS"
                },
                value=7000,
            )
        ]
        try:
            DatapointInterface.join_report_datapoints_to_metric_interfaces(
                metric_interfaces_by_key=metric_interfaces_by_key,
                report_datapoints=report_datapoints,
            )
            assert False
        except ValueError:
            assert True
