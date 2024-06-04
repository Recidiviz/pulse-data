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
"""This class implements tests for the Justice Counts MetricSettingInterface."""

from recidiviz.justice_counts.metric_setting import MetricSettingInterface
from recidiviz.justice_counts.metrics import law_enforcement, supervision
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.persistence.database.schema.justice_counts.schema import MetricSetting
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)


class TestMetricSettingInterface(JusticeCountsDatabaseTestCase):
    """Implements tests for the MetricSettingInterface."""

    def setUp(self) -> None:
        super().setUp()
        self.test_schema_objects = JusticeCountsSchemaTestObjects()

    def test_add_metric_setting(self) -> None:
        """Test that adding a metric setting to the database works as expected."""
        agency = self.test_schema_objects.test_agency_A
        funding_metric = self.test_schema_objects.funding_metric
        with SessionFactory.using_database(self.database_key) as session:
            # Add agency to the database.
            session.add(agency)
            session.commit()
            session.refresh(agency)

            # Write the metric setting to the database.
            MetricSettingInterface.new_add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric=funding_metric,
            )
            session.commit()

            metric_settings = session.query(MetricSetting).one()
            self.assertDictEqual(
                metric_settings.metric_interface, funding_metric.to_storage_json()
            )

    def test_disable_metric_setting(self) -> None:
        """Test that updating a metric setting works as expected."""
        with SessionFactory.using_database(self.database_key) as session:
            # Add agency to the database.
            agency = self.test_schema_objects.test_agency_A
            session.add(agency)
            session.commit()
            session.refresh(agency)

            # Write the initial metric setting to the database.
            MetricSettingInterface.new_add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric=MetricInterface(
                    key=law_enforcement.funding.key,
                    is_metric_enabled=True,
                ),
            )
            session.commit()

            # Check that the initial metric is enabled.
            metric_settings = session.query(MetricSetting).one()
            self.assertEqual(
                metric_settings.metric_interface["is_metric_enabled"], True
            )

            # Write the updated metric setting to the database. In this case, we are
            # disabling the metric.
            MetricSettingInterface.new_add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric=MetricInterface(
                    key=law_enforcement.funding.key,
                    is_metric_enabled=False,
                ),
            )
            session.commit()

            # Test that the existing metric is now disabled.
            metric_settings = session.query(MetricSetting).one()
            self.assertEqual(
                metric_settings.metric_interface["is_metric_enabled"], False
            )

    def test_set_disaggregated_by_supervision_subsystem_to_true(self) -> None:
        """
        Test that toggling a metric's disaggregated_by_supervision_subsystem to true
        will correctly set all of the disaggregated_by_supervision_subsystem fields for
        supervision systems and subsystems for that agency. And that is_metric_enabled
        will be set True for the subsystem metrics and False for the supervision metric.
        """
        with SessionFactory.using_database(self.database_key) as session:
            # Add agency to the database.
            agency = self.test_schema_objects.test_agency_E  # Parole and Probation.
            session.add(agency)
            session.commit()
            session.refresh(agency)

            # Write the initial metric setting to the database. The intial metrics will
            # be set as a Parole and Probation supervision agency which has its funding
            # metric reporting as combined (not disaggregated) across the supervision
            # systems.
            parole_funding_key = supervision.funding.key.replace(
                "SUPERVISION", "PAROLE"
            )
            probation_funding_key = supervision.funding.key.replace(
                "SUPERVISION", "PROBATION"
            )

            MetricSettingInterface.new_add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric=MetricInterface(
                    key=supervision.funding.key,
                    disaggregated_by_supervision_subsystems=False,
                    is_metric_enabled=True,
                ),
            )
            MetricSettingInterface.new_add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric=MetricInterface(
                    key=parole_funding_key,
                    is_metric_enabled=False,
                ),
            )
            MetricSettingInterface.new_add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric=MetricInterface(
                    key=probation_funding_key,
                    is_metric_enabled=False,
                ),
            )
            session.commit()

            # Send a Metric Interface update which sets disaggregated_by_supervision_subsystems
            # to True.
            MetricSettingInterface.new_add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric=MetricInterface(
                    key=parole_funding_key,
                    disaggregated_by_supervision_subsystems=True,
                ),
            )

            # Test that now the supervision metric is disabled, and the subsystem
            # metrics are enabled.
            metric_settings = session.query(MetricSetting).all()
            self.assertEqual(len(metric_settings), 3)
            is_metric_enabled_values = {
                metric_setting.metric_definition_key: metric_setting.metric_interface[
                    "is_metric_enabled"
                ]
                for metric_setting in metric_settings
            }
            self.assertDictEqual(
                is_metric_enabled_values,
                {
                    supervision.funding.key: False,
                    parole_funding_key: True,
                    probation_funding_key: True,
                },
            )

            # Check that disaggregated_by_supervision_subsystems_values is now set to
            # True for all supervision and subsystem metrics.
            disaggregated_by_supervision_subsystems_values = {
                metric_setting.metric_definition_key: metric_setting.metric_interface[
                    "disaggregated_by_supervision_subsystems"
                ]
                for metric_setting in metric_settings
            }
            self.assertDictEqual(
                disaggregated_by_supervision_subsystems_values,
                {
                    supervision.funding.key: True,
                    parole_funding_key: True,
                    probation_funding_key: True,
                },
            )

    def test_set_disaggregated_by_supervision_subsystem_to_false(self) -> None:
        """
        Test that toggling a metric's disaggregated_by_supervision_subsystem to false
        will correctly set all of the disaggregated_by_supervision_subsystem fields for
        supervision systems and subsystems for that agency. And that is_metric_enabled
        will be set False for the subsystem metrics and True for the supervision metric.
        """
        with SessionFactory.using_database(self.database_key) as session:
            # Add agency to the database.
            agency = self.test_schema_objects.test_agency_E  # Parole and Probation.
            session.add(agency)
            session.commit()
            session.refresh(agency)

            # Write the initial metric setting to the database. The intial metrics will
            # be set as a Parole and Probation supervision agency which has its funding
            # metric reporting as combined (not disaggregated) across the supervision
            # systems.
            parole_funding_key = supervision.funding.key.replace(
                "SUPERVISION", "PAROLE"
            )
            probation_funding_key = supervision.funding.key.replace(
                "SUPERVISION", "PROBATION"
            )

            MetricSettingInterface.new_add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric=MetricInterface(
                    key=supervision.funding.key,
                    disaggregated_by_supervision_subsystems=True,
                    is_metric_enabled=False,
                ),
            )
            MetricSettingInterface.new_add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric=MetricInterface(
                    key=parole_funding_key,
                    is_metric_enabled=True,
                ),
            )
            MetricSettingInterface.new_add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric=MetricInterface(
                    key=probation_funding_key,
                    is_metric_enabled=True,
                ),
            )
            session.commit()

            # Send a Metric Interface update which sets disaggregated_by_supervision_subsystems
            # to False.
            MetricSettingInterface.new_add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric=MetricInterface(
                    key=parole_funding_key,
                    disaggregated_by_supervision_subsystems=False,
                ),
            )

            # Test that now the supervision metric is enabled, and the subsystem
            # metrics are disabled.
            metric_settings = session.query(MetricSetting).all()
            self.assertEqual(len(metric_settings), 3)
            is_metric_enabled_values = {
                metric_setting.metric_definition_key: metric_setting.metric_interface[
                    "is_metric_enabled"
                ]
                for metric_setting in metric_settings
            }
            self.assertDictEqual(
                is_metric_enabled_values,
                {
                    supervision.funding.key: True,
                    parole_funding_key: False,
                    probation_funding_key: False,
                },
            )

            # Check that disaggregated_by_supervision_subsystems_values is now set to
            # False for all supervision and subsystem metrics.
            disaggregated_by_supervision_subsystems_values = {
                metric_setting.metric_definition_key: metric_setting.metric_interface[
                    "disaggregated_by_supervision_subsystems"
                ]
                for metric_setting in metric_settings
            }
            self.assertDictEqual(
                disaggregated_by_supervision_subsystems_values,
                {
                    supervision.funding.key: False,
                    parole_funding_key: False,
                    probation_funding_key: False,
                },
            )
