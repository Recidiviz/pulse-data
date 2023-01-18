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
from recidiviz.justice_counts.dimensions.jails_and_prisons import PrisonsFundingType
from recidiviz.justice_counts.dimensions.law_enforcement import CallType
from recidiviz.justice_counts.exceptions import JusticeCountsServerError
from recidiviz.justice_counts.includes_excludes.prisons import (
    PrisonFundingIncludesExcludes,
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
)
from recidiviz.persistence.database.schema.justice_counts.schema import (
    Datapoint,
    DatapointHistory,
    Report,
    ReportingFrequency,
    ReportStatus,
    UserAccount,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)


class TestDatapointInterface(JusticeCountsDatabaseTestCase):
    """Implements tests for the DatapointInterface."""

    def setUp(self) -> None:
        super().setUp()
        self.test_schema_objects = JusticeCountsSchemaTestObjects()

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
                            PrisonsFundingType.OTHER: [
                                MetricContextData(
                                    key=ContextKey["ADDITIONAL_CONTEXT"],
                                    value="User entered text...",
                                )
                            ]
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
            self.assertEqual(len(datapoints), 1)
            datapoint = datapoints[0]
            self.assertEqual(datapoint.context_key, "ADDITIONAL_CONTEXT")
            self.assertEqual(datapoint.value, "User entered text...")
            self.assertEqual(
                datapoint.dimension_identifier_to_member,
                {"metric/prisons/funding/type": "OTHER"},
            )

            # Test build_metric_key_to_datapoints() and get_aggregated_dimension_data()
            metric_key_to_data_points = (
                DatapointInterface.build_metric_key_to_datapoints(datapoints)
            )
            metric_datapoints = metric_key_to_data_points[
                datapoint.metric_definition_key
            ]
            self.assertEqual(
                metric_datapoints.dimension_to_context_key_to_datapoints,
                {PrisonsFundingType.OTHER: {datapoint.context_key: datapoint}},
            )

            agg_dims = metric_datapoints.get_aggregated_dimension_data(
                agency_metric.metric_definition
            )
            self.assertEqual(
                agg_dims[0].dimension_to_contexts,
                {
                    PrisonsFundingType.OTHER: [
                        MetricContextData(
                            key=ContextKey(datapoint.context_key),
                            value=datapoint.get_value(),
                        )
                    ]
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
                    for member in PrisonFundingIncludesExcludes
                },
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )
            datapoints = session.query(Datapoint).all()
            # New includes/excludes datapoint for each member of PrisonFundingIncludesExcludes
            self.assertEqual(len(datapoints), len(PrisonFundingIncludesExcludes))
            datapoint_histories = session.query(DatapointHistory).all()
            self.assertEqual(len(datapoint_histories), 0)
            agency_metric = MetricInterface(
                key=prisons.funding.key,
                includes_excludes_member_to_setting={
                    member: IncludesExcludesSetting.YES
                    for member in PrisonFundingIncludesExcludes
                },
            )
            DatapointInterface.add_or_update_agency_datapoints(
                agency_metric=agency_metric,
                agency=agency,
                session=session,
                user_account=user,
            )
            datapoints = session.query(Datapoint).all()
            # New includes/excludes datapoint for each member of PrisonFundingIncludesExcludes
            self.assertEqual(len(datapoints), len(PrisonFundingIncludesExcludes))
            datapoint_histories = session.query(DatapointHistory).all()
            self.assertEqual(
                len(datapoint_histories), len(PrisonFundingIncludesExcludes)
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
            self.assertEqual(len(datapoints), 3)
            for datapoint in datapoints:
                self.assertEqual(datapoint.value, "True")
                self.assertEqual(
                    datapoint.context_key, DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS
                )

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
            self.assertEqual(len(datapoints), 3)
            for datapoint in datapoints:
                self.assertEqual(datapoint.value, "False")
                self.assertEqual(
                    datapoint.context_key, DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS
                )

    def test_save_invalid_datapoint(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            monthly_report = self.test_schema_objects.test_report_monthly
            user = self.test_schema_objects.test_user_A
            session.add_all([monthly_report, user])
            current_time = datetime.datetime.now(tz=datetime.timezone.utc)

            try:
                # When a report is in Draft mode, no errors are raised when the value is invalid
                DatapointInterface.add_datapoint(
                    session=session,
                    existing_datapoints_dict=ReportInterface.get_existing_datapoints_dict(
                        reports=[monthly_report]
                    ),
                    report=monthly_report,
                    value="123abc",
                    user_account=user,
                    metric_definition_key=law_enforcement.funding.key,
                    current_time=current_time,
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
                DatapointInterface.add_datapoint(
                    session=session,
                    existing_datapoints_dict=ReportInterface.get_existing_datapoints_dict(
                        reports=[monthly_report]
                    ),
                    report=monthly_report,
                    value="123abc",
                    user_account=user,
                    metric_definition_key=law_enforcement.funding.key,
                    current_time=current_time,
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

            DatapointInterface.add_datapoint(
                session=session,
                existing_datapoints_dict=ReportInterface.get_existing_datapoints_dict(
                    reports=[monthly_report]
                ),
                report=monthly_report,
                value="$123.0",
                user_account=user,
                metric_definition_key=law_enforcement.calls_for_service.key,
                current_time=current_time,
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
            )

            self.assertDictEqual(
                datapoint_json,
                {
                    "id": monthly_report.id,
                    "report_id": 1,
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

            DatapointInterface.add_datapoint(
                session=session,
                existing_datapoints_dict=ReportInterface.get_existing_datapoints_dict(
                    reports=[monthly_report]
                ),
                report=monthly_report,
                value=456.3,
                user_account=user,
                metric_definition_key=law_enforcement.calls_for_service.key,
                current_time=current_time,
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
            )

            self.assertDictEqual(
                datapoint_json,
                {
                    "id": monthly_report.id,
                    "report_id": 1,
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
