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
from recidiviz.justice_counts.dimensions.law_enforcement import CallType, ForceType
from recidiviz.justice_counts.exceptions import JusticeCountsDataError
from recidiviz.justice_counts.metrics import law_enforcement
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.persistence.database.schema.justice_counts.schema import (
    Agency,
    Datapoint,
    Report,
    ReportStatus,
    UserAccount,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)


class TestDatapointInterface(JusticeCountsDatabaseTestCase):
    """Implements tests for the UserAccountInterface."""

    def setUp(self) -> None:
        super().setUp()
        self.test_schema_objects = JusticeCountsSchemaTestObjects()

    def test_save_agency_datapoints_turnoff_whole_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency = self.test_schema_objects.test_agency_A
            session.add(agency)
            metric_json = self.test_schema_objects.get_agency_datapoints(
                is_metric_enabled=False
            )
            DatapointInterface.update_agency_metric(
                metric_json=metric_json, agency=agency, session=session
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 1)
            self.assertEqual(datapoints[0].enabled, False)
            self.assertEqual(
                datapoints[0].metric_definition_key, metric_json.get("key")
            )
            self.assertEqual(datapoints[0].dimension_identifier_to_member, None)
            self.assertEqual(datapoints[0].context_key, None)

    def test_save_agency_datapoints_turnoff_disaggregation(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(self.test_schema_objects.test_agency_A)
            agency = session.query(Agency).one()
            metric_json = self.test_schema_objects.get_agency_datapoints(
                use_disabled_disaggregation=True
            )
            DatapointInterface.update_agency_metric(
                session=session,
                metric_json=metric_json,
                agency=agency,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), len(ForceType))
            datapoints.sort(
                key=lambda d: list(d.dimension_identifier_to_member.values())[0]
            )
            self.assertEqual(datapoints[0].enabled, False)
            self.assertEqual(
                datapoints[0].dimension_identifier_to_member,
                {ForceType.dimension_identifier(): "PHYSICAL"},
            )
            self.assertEqual(datapoints[1].enabled, False)
            self.assertEqual(
                datapoints[1].dimension_identifier_to_member,
                {ForceType.dimension_identifier(): "RESTRAINT"},
            )
            self.assertEqual(datapoints[2].enabled, False)
            self.assertEqual(
                datapoints[2].dimension_identifier_to_member,
                {ForceType.dimension_identifier(): "UNKNOWN"},
            )
            self.assertEqual(datapoints[3].enabled, False)
            self.assertEqual(
                datapoints[3].dimension_identifier_to_member,
                {ForceType.dimension_identifier(): "VERBAL"},
            )
            self.assertEqual(datapoints[4].enabled, False)
            self.assertEqual(
                datapoints[4].dimension_identifier_to_member,
                {ForceType.dimension_identifier(): "WEAPON"},
            )

    def test_save_agency_datapoints_disable_single_breakdown(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(self.test_schema_objects.test_agency_A)
            agency = session.query(Agency).one()
            metric_json = self.test_schema_objects.get_agency_datapoints(
                use_partially_disabled_disaggregation=True
            )
            DatapointInterface.update_agency_metric(
                session=session,
                metric_json=metric_json,
                agency=agency,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 2)
            datapoints.sort(
                key=lambda d: list(d.dimension_identifier_to_member.values())[0]
            )
            self.assertEqual(datapoints[0].enabled, False)
            self.assertEqual(
                datapoints[0].dimension_identifier_to_member,
                {CallType.dimension_identifier(): "EMERGENCY"},
            )
            self.assertEqual(datapoints[1].enabled, False)
            self.assertEqual(
                datapoints[1].dimension_identifier_to_member,
                {CallType.dimension_identifier(): "UNKNOWN"},
            )

    def test_save_agency_datapoints_reenable_breakdown(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(self.test_schema_objects.test_agency_A)
            agency = session.query(Agency).one()
            metric_json = self.test_schema_objects.get_agency_datapoints(
                use_partially_disabled_disaggregation=True
            )
            DatapointInterface.update_agency_metric(
                session=session,
                metric_json=metric_json,
                agency=agency,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 2)
            # Reenable Metric, this will delete the datapoint
            metric_json = self.test_schema_objects.get_agency_datapoints(
                use_reenabled_breakdown=True
            )
            DatapointInterface.update_agency_metric(
                session=session,
                metric_json=metric_json,
                agency=agency,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 1)

    def test_save_agency_datapoints_reenable_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(self.test_schema_objects.test_agency_A)
            agency = session.query(Agency).one()
            metric_json = self.test_schema_objects.get_agency_datapoints(
                is_metric_enabled=False
            )
            DatapointInterface.update_agency_metric(
                session=session,
                metric_json=metric_json,
                agency=agency,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 1)
            metric_json = self.test_schema_objects.get_agency_datapoints(
                is_metric_enabled=True
            )
            DatapointInterface.update_agency_metric(
                session=session,
                metric_json=metric_json,
                agency=agency,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 0)

    def test_save_agency_datapoints_contexts(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(self.test_schema_objects.test_agency_A)
            agency = session.query(Agency).one()
            metric_json = self.test_schema_objects.get_agency_datapoints(
                include_contexts=True, use_disabled_disaggregation=True
            )
            DatapointInterface.update_agency_metric(
                session=session,
                metric_json=metric_json,
                agency=agency,
            )
            datapoints = session.query(Datapoint).all()
            self.assertEqual(len(datapoints), 6)
            for datapoint in datapoints:
                if datapoint.context_key is None:
                    self.assertEqual(datapoint.enabled, False)
                else:
                    self.assertEqual(
                        datapoint.context_key, ContextKey.ADDITIONAL_CONTEXT.value
                    )
                    self.assertEqual(
                        datapoint.value, "this additional context provides contexts"
                    )

    def test_save_invalid_datapoint(self) -> None:
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
                    report=monthly_report,
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
