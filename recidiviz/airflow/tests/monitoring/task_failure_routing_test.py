# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for task_failure_routing.py"""
import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from recidiviz.airflow.dags.monitoring.airflow_alerting_incident import (
    AirflowAlertingIncident,
)
from recidiviz.airflow.dags.monitoring.dag_registry import (
    get_calculation_dag_id,
    get_sftp_dag_id,
)
from recidiviz.airflow.dags.monitoring.incident_alert_routing import (
    get_alerting_service_for_incident,
)
from recidiviz.airflow.dags.utils.recidiviz_pagerduty_service import (
    RecidivizPagerDutyService,
)
from recidiviz.common.constants.states import StateCode

_PROJECT_ID = "recidiviz-456"


@patch.dict(os.environ, {"GCP_PROJECT": _PROJECT_ID})
class TestGetAlertingServiceFromTask(unittest.TestCase):
    """Tests for task_failure_routing.py"""

    @staticmethod
    def _make_incident(dag_id: str, task_id: str) -> AirflowAlertingIncident:
        return AirflowAlertingIncident(
            dag_id=dag_id,
            conf="{}",
            task_id=task_id,
            failed_execution_dates=[datetime.now(tz=timezone.utc)],
        )

    def test_get_alerting_service_for_incident(self) -> None:
        self.assertEqual(
            RecidivizPagerDutyService.data_platform_airflow_service(
                project_id=_PROJECT_ID
            ),
            get_alerting_service_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    task_id="initialize_dag.verify_parameters",
                )
            ),
        )
        self.assertEqual(
            RecidivizPagerDutyService.data_platform_airflow_service(
                project_id=_PROJECT_ID
            ),
            get_alerting_service_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    task_id="update_state",
                )
            ),
        )

        # State-specific metric export failure
        self.assertEqual(
            RecidivizPagerDutyService.airflow_service_for_state_code(
                project_id=_PROJECT_ID, state_code=StateCode.US_PA
            ),
            get_alerting_service_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    task_id=(
                        "metric_exports.state_specific_metric_exports.US_PA_metric_exports."
                        "export_product_user_import_us_pa_metric_view_data"
                    ),
                )
            ),
        )

        # Metric export group start / end
        self.assertEqual(
            RecidivizPagerDutyService.data_platform_airflow_service(
                project_id=_PROJECT_ID
            ),
            get_alerting_service_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    task_id=("metric_exports.state_specific_metric_exports"),
                )
            ),
        )
        self.assertEqual(
            RecidivizPagerDutyService.data_platform_airflow_service(
                project_id=_PROJECT_ID
            ),
            get_alerting_service_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    task_id=(
                        "metric_exports.state_specific_metric_exports.branch_start"
                    ),
                )
            ),
        )
        self.assertEqual(
            RecidivizPagerDutyService.data_platform_airflow_service(
                project_id=_PROJECT_ID
            ),
            get_alerting_service_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    task_id="metric_exports.state_specific_metric_exports.branch_end",
                )
            ),
        )

        # State-specific Dataflow pipeline failure
        self.assertEqual(
            RecidivizPagerDutyService.airflow_service_for_state_code(
                project_id=_PROJECT_ID, state_code=StateCode.US_ND
            ),
            get_alerting_service_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    task_id=(
                        "post_normalization_pipelines.US_ND_dataflow_pipelines."
                        "full-us-nd-supervision-metrics.run_pipeline"
                    ),
                )
            ),
        )

        # Failures in this task indicate general data platform issues
        self.assertEqual(
            RecidivizPagerDutyService.data_platform_airflow_service(
                project_id=_PROJECT_ID
            ),
            get_alerting_service_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    task_id=(
                        "post_normalization_pipelines.US_ND_dataflow_pipelines."
                        "full-us-nd-supervision-metrics.create_flex_template"
                    ),
                )
            ),
        )

        # We don't route validation task failures to state-specific services because
        # crashes here are indicative of general infra issues.
        self.assertEqual(
            RecidivizPagerDutyService.data_platform_airflow_service(
                project_id=_PROJECT_ID
            ),
            get_alerting_service_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    task_id="validations.execute_validations_US_AR",
                )
            ),
        )

        # Generic tasks in SFTP route to platform
        self.assertEqual(
            RecidivizPagerDutyService.data_platform_airflow_service(
                project_id=_PROJECT_ID
            ),
            get_alerting_service_for_incident(
                self._make_incident(
                    dag_id=get_sftp_dag_id(_PROJECT_ID),
                    task_id="start_sftp",
                )
            ),
        )

        # All other state-specific SFTP tasks route to that state
        self.assertEqual(
            RecidivizPagerDutyService.airflow_service_for_state_code(
                project_id=_PROJECT_ID, state_code=StateCode.US_AR
            ),
            get_alerting_service_for_incident(
                self._make_incident(
                    dag_id=get_sftp_dag_id(_PROJECT_ID),
                    task_id="US_AR.check_config",
                )
            ),
        )

    def test_get_alerting_service_for_incident_two_different_state_codes(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found task_id \[.*\] referencing more than one state code. "
            r"References \[US_CA\] and \[US_ND\]",
        ):
            get_alerting_service_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    task_id=(
                        "post_normalization_pipelines.US_CA_dataflow_pipelines."
                        "full-us-nd-supervision-metrics.run_pipeline"
                    ),
                )
            )

    def test_get_alerting_service_for_incident_two_different_state_codes_same_part(
        self,
    ) -> None:
        bad_task_ids = [
            "post_normalization_pipelines.US_CA_US_ND_dataflow_pipelines.run_pipeline",
            "post_normalization_pipelines.US_CA_US_ND.run_pipeline",
            "post_normalization_pipelines.US_CA-us-nd_asdf.run_pipeline",
            "post_normalization_pipelines.full-US_CA-asdf_us-nd.run_pipeline",
        ]
        for bad_task_id in bad_task_ids:
            with self.assertRaisesRegex(
                ValueError,
                r"Task id part \[.*\] references more than one "
                r"state code: \['US_CA', 'US_ND'\].",
            ):
                get_alerting_service_for_incident(
                    self._make_incident(
                        dag_id=get_calculation_dag_id(_PROJECT_ID),
                        task_id=bad_task_id,
                    )
                )
