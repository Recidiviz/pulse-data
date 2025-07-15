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
"""Tests for incident_alert_routing.py"""
import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from recidiviz.airflow.dags.monitoring.airflow_alerting_incident import (
    AirflowAlertingIncident,
)
from recidiviz.airflow.dags.monitoring.dag_registry import (
    get_calculation_dag_id,
    get_raw_data_import_dag_id,
    get_sftp_dag_id,
)
from recidiviz.airflow.dags.monitoring.incident_alert_routing import (
    get_alerting_services_for_incident,
)
from recidiviz.airflow.dags.monitoring.job_run import JobRunType
from recidiviz.airflow.dags.monitoring.recidiviz_github_alerting_service import (
    RecidivizGitHubService,
)
from recidiviz.airflow.dags.utils.recidiviz_pagerduty_service import (
    RecidivizPagerDutyService,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCPEnvironment

_PROJECT_ID = "recidiviz-456"


@patch.dict(os.environ, {"GCP_PROJECT": _PROJECT_ID})
class TestGetAlertingServiceForIncident(unittest.TestCase):
    """Tests for incident_alert_routing.py"""

    def setUp(self) -> None:
        self.github_hook_patcher = patch(
            "recidiviz.airflow.dags.monitoring.recidiviz_github_alerting_service.GithubHook",
        )
        self.github_hook_patcher.start()
        self.env_for_project_patch = patch(
            "recidiviz.airflow.dags.monitoring.recidiviz_github_alerting_service.get_environment_for_project",
            return_value=GCPEnvironment.STAGING,
        )
        self.env_for_project_patch.start()

    def tearDown(self) -> None:
        self.github_hook_patcher.stop()
        self.env_for_project_patch.stop()

    @staticmethod
    def _make_incident(
        dag_id: str, job_id: str, incident_type: str | None = None
    ) -> AirflowAlertingIncident:
        return AirflowAlertingIncident(
            dag_id=dag_id,
            dag_run_config="{}",
            job_id=job_id,
            failed_execution_dates=[datetime.now(tz=timezone.utc)],
            incident_type=incident_type or JobRunType.AIRFLOW_TASK_RUN.value,
        )

    def test_get_alerting_service_for_incident(self) -> None:
        self.assertEqual(
            [
                RecidivizPagerDutyService.data_platform_airflow_service(
                    project_id=_PROJECT_ID
                )
            ],
            get_alerting_services_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    job_id="initialize_dag.verify_parameters",
                )
            ),
        )
        self.assertEqual(
            [
                RecidivizPagerDutyService.data_platform_airflow_service(
                    project_id=_PROJECT_ID
                )
            ],
            get_alerting_services_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    job_id="update_state",
                )
            ),
        )

        # State-specific metric export failure
        self.assertEqual(
            [
                RecidivizPagerDutyService.polaris_airflow_service(
                    project_id=_PROJECT_ID
                ),
            ],
            get_alerting_services_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    job_id=(
                        "metric_exports.state_specific_metric_exports.US_PA_metric_exports."
                        "export_product_user_import_us_pa_metric_view_data"
                    ),
                )
            ),
        )

        # Metric export group start / end
        self.assertEqual(
            [
                RecidivizPagerDutyService.data_platform_airflow_service(
                    project_id=_PROJECT_ID
                )
            ],
            get_alerting_services_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    job_id=("metric_exports.state_specific_metric_exports"),
                )
            ),
        )
        self.assertEqual(
            [
                RecidivizPagerDutyService.data_platform_airflow_service(
                    project_id=_PROJECT_ID
                )
            ],
            get_alerting_services_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    job_id=(
                        "metric_exports.state_specific_metric_exports.branch_start"
                    ),
                )
            ),
        )
        self.assertEqual(
            [
                RecidivizPagerDutyService.data_platform_airflow_service(
                    project_id=_PROJECT_ID
                )
            ],
            get_alerting_services_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    job_id="metric_exports.state_specific_metric_exports.branch_end",
                )
            ),
        )

        # State-specific Dataflow pipeline failure
        self.assertEqual(
            [
                RecidivizPagerDutyService.airflow_service_for_state_code(
                    project_id=_PROJECT_ID, state_code=StateCode.US_ND
                ),
                RecidivizGitHubService.dataflow_service_for_state_code(
                    project_id=_PROJECT_ID, state_code=StateCode.US_ND
                ),
            ],
            get_alerting_services_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    job_id=(
                        "post_ingest_pipelines.US_ND_dataflow_pipelines."
                        "full-us-nd-supervision-metrics.run_pipeline"
                    ),
                )
            ),
        )
        self.assertEqual(
            [
                RecidivizPagerDutyService.data_platform_airflow_service(
                    project_id=_PROJECT_ID
                )
            ],
            get_alerting_services_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    job_id=(
                        "post_ingest_pipelines.US_ND_dataflow_pipelines."
                        "full-us-nd-supervision-metrics.run_pipeline"
                    ),
                    incident_type=JobRunType.RUNTIME_MONITORING.value,
                )
            ),
        )

        # Failures in this task indicate that raw data has been removed or operations
        # tables haven't been properly managed, so route the failure to state-specific
        # on-calls.
        self.assertEqual(
            [
                RecidivizPagerDutyService.airflow_service_for_state_code(
                    project_id=_PROJECT_ID, state_code=StateCode.US_ND
                ),
                RecidivizGitHubService.dataflow_service_for_state_code(
                    project_id=_PROJECT_ID, state_code=StateCode.US_ND
                ),
            ],
            get_alerting_services_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    job_id=(
                        "ingest.us_nd_dataflow.initialize_ingest_pipeline."
                        "check_for_valid_watermarks"
                    ),
                )
            ),
        )

        # Failures in this task indicate general data platform issues
        self.assertEqual(
            [
                RecidivizPagerDutyService.data_platform_airflow_service(
                    project_id=_PROJECT_ID
                )
            ],
            get_alerting_services_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    job_id=(
                        "post_ingest_pipelines.US_ND_dataflow_pipelines."
                        "full-us-nd-supervision-metrics.create_flex_template"
                    ),
                )
            ),
        )
        self.assertEqual(
            [
                RecidivizPagerDutyService.data_platform_airflow_service(
                    project_id=_PROJECT_ID
                )
            ],
            get_alerting_services_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    job_id=(
                        "post_ingest_pipelines.US_ND_dataflow_pipelines."
                        "full-us-nd-supervision-metrics.create_flex_template"
                    ),
                    incident_type=JobRunType.RUNTIME_MONITORING.value,
                )
            ),
        )

        # We don't route validation task failures to state-specific services because
        # crashes here are indicative of general infra issues.
        self.assertEqual(
            [
                RecidivizPagerDutyService.data_platform_airflow_service(
                    project_id=_PROJECT_ID
                )
            ],
            get_alerting_services_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    job_id="validations.execute_validations_US_AR",
                )
            ),
        )

        # Generic tasks in SFTP route to platform
        self.assertEqual(
            [
                RecidivizPagerDutyService.data_platform_airflow_service(
                    project_id=_PROJECT_ID
                )
            ],
            get_alerting_services_for_incident(
                self._make_incident(
                    dag_id=get_sftp_dag_id(_PROJECT_ID),
                    job_id="start_sftp",
                )
            ),
        )

        # All other state-specific SFTP tasks route to that state
        self.assertEqual(
            [
                RecidivizPagerDutyService.airflow_service_for_state_code(
                    project_id=_PROJECT_ID, state_code=StateCode.US_AR
                ),
            ],
            get_alerting_services_for_incident(
                self._make_incident(
                    dag_id=get_sftp_dag_id(_PROJECT_ID),
                    job_id="US_AR.check_config",
                )
            ),
        )

        self.assertEqual(
            [
                RecidivizPagerDutyService.raw_data_service_for_state_code(
                    project_id=_PROJECT_ID, state_code=StateCode.US_OZ
                ),
                RecidivizGitHubService.raw_data_service_for_state_code(
                    project_id=_PROJECT_ID, state_code=StateCode.US_OZ
                ),
            ],
            get_alerting_services_for_incident(
                self._make_incident(
                    dag_id=get_raw_data_import_dag_id(_PROJECT_ID),
                    job_id="US_OZ.hunger_games_person",
                )
            ),
        )

        # but data platform for runtime!
        self.assertEqual(
            [
                RecidivizPagerDutyService.data_platform_airflow_service(
                    project_id=_PROJECT_ID
                )
            ],
            get_alerting_services_for_incident(
                self._make_incident(
                    dag_id=get_raw_data_import_dag_id(_PROJECT_ID),
                    job_id="US_OZ.hunger_games_person",
                    incident_type=JobRunType.RUNTIME_MONITORING.value,
                ),
            ),
        )

        self.assertEqual(
            [
                RecidivizPagerDutyService.raw_data_service_for_state_code(
                    project_id=_PROJECT_ID, state_code=StateCode.US_OZ
                ),
                RecidivizGitHubService.raw_data_service_for_state_code(
                    project_id=_PROJECT_ID, state_code=StateCode.US_OZ
                ),
            ],
            get_alerting_services_for_incident(
                self._make_incident(
                    dag_id=get_raw_data_import_dag_id(_PROJECT_ID),
                    job_id="raw_data_branching.us_oz_primary_import_branch.raise_operations_registration_errors",
                )
            ),
        )

        self.assertEqual(
            [
                RecidivizPagerDutyService.data_platform_airflow_service(
                    project_id=_PROJECT_ID
                )
            ],
            get_alerting_services_for_incident(
                self._make_incident(
                    dag_id=get_raw_data_import_dag_id(_PROJECT_ID),
                    job_id="raw_data_branching.us_oz_primary_import_branch.pre_import_normalization.raise_chunk_normalization_errors",
                )
            ),
        )

    def test_get_alerting_service_for_incident_two_different_state_codes(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found job_id \[.*\] referencing more than one state code. "
            r"References \[US_CA\] and \[US_ND\]",
        ):
            get_alerting_services_for_incident(
                self._make_incident(
                    dag_id=get_calculation_dag_id(_PROJECT_ID),
                    job_id=(
                        "post_ingest_pipelines.US_CA_dataflow_pipelines."
                        "full-us-nd-supervision-metrics.run_pipeline"
                    ),
                )
            )

    def test_get_alerting_service_for_incident_two_different_state_codes_same_part(
        self,
    ) -> None:
        bad_task_ids = [
            "post_ingest_pipelines.US_CA_US_ND_dataflow_pipelines.run_pipeline",
            "post_ingest_pipelines.US_CA_US_ND.run_pipeline",
            "post_ingest_pipelines.US_CA-us-nd_asdf.run_pipeline",
            "post_ingest_pipelines.full-US_CA-asdf_us-nd.run_pipeline",
        ]
        for bad_task_id in bad_task_ids:
            with self.assertRaisesRegex(
                ValueError,
                r"Job id part \[.*\] references more than one "
                r"state code: \['US_CA', 'US_ND'\].",
            ):
                get_alerting_services_for_incident(
                    self._make_incident(
                        dag_id=get_calculation_dag_id(_PROJECT_ID),
                        job_id=bad_task_id,
                    )
                )
