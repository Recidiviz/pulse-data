# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for RecidivizDataflowFlexTemplateOperator."""
from datetime import datetime
from unittest.mock import MagicMock, create_autospec, patch

from airflow import DAG
from airflow.providers.google.cloud.hooks.dataflow import (
    DataflowHook,
    DataflowJobStatus,
)
from google.cloud.logging import Client as LoggingClient

from recidiviz.airflow.dags.operators.recidiviz_dataflow_operator import (
    RecidivizDataflowFlexTemplateOperator,
)
from recidiviz.airflow.tests.test_utils import AirflowIntegrationTest, execute_task


class TestRecidivizDataflowFlexTemplateOperator(AirflowIntegrationTest):
    """Tests for RecidivizDataflowFlexTemplateOperator."""

    def setUp(self) -> None:
        self.mock_hook = create_autospec(DataflowHook)
        self.mock_hook_patcher = patch(
            "recidiviz.airflow.dags.operators.recidiviz_dataflow_operator.DataflowHook",
        )
        self.mock_hook_patcher.start().return_value = self.mock_hook

        self.mock_logs_client = create_autospec(LoggingClient)
        self.mock_logs_client_patcher = patch(
            "recidiviz.airflow.dags.operators.recidiviz_dataflow_operator.LoggingClient",
        )
        self.mock_logs_client_patcher.start().return_value = self.mock_logs_client

        # Mocking instance method rather than setting up deeply nested mock of the dataflow service
        self.mock_get_job = patch(
            "recidiviz.airflow.dags.operators.recidiviz_dataflow_operator.RecidivizDataflowFlexTemplateOperator.get_job"
        ).start()

        self.dag = DAG(dag_id="test_dag", start_date=datetime.now())
        self.dataflow_task = RecidivizDataflowFlexTemplateOperator(
            task_id="test_task",
            project_id="test-project",
            body={"launchParameter": {"jobName": "test-job"}},
            location="us-central1",
        )

    def tearDown(self) -> None:
        self.mock_hook_patcher.stop()
        self.mock_logs_client_patcher.stop()
        self.mock_get_job.stop()

    def test_execute_basic(self) -> None:
        # Arrange
        self.mock_hook.is_job_dataflow_running.return_value = False

        # Act
        _ = execute_task(self.dag, self.dataflow_task)

        self.mock_hook.start_flex_template.assert_called()

    def test_execute_on_retry(self) -> None:
        # Arrange
        self.mock_hook.is_job_dataflow_running.return_value = True

        # Act
        _ = execute_task(self.dag, self.dataflow_task)

        self.mock_hook.wait_for_done.assert_called()

    def test_execute_job_failure(self) -> None:
        self.mock_hook.is_job_dataflow_running.return_value = False
        self.mock_hook.start_flex_template.side_effect = Exception("Job has failed!")
        self.mock_get_job.return_value = {
            "currentState": DataflowJobStatus.JOB_STATE_FAILED,
            "id": "2023-09-18_07_09_47-16912541725945987225",
            "createTime": "2023-09-18T14:09:47.864426Z",
        }

        with self.assertRaises(Exception):
            _ = execute_task(self.dag, self.dataflow_task)

        self.mock_logs_client.list_entries.assert_called_with(
            resource_names=["projects/test-project"],
            filter_="\n"
            '(log_id("dataflow.googleapis.com/job-message") OR log_id("dataflow.googleapis.com/launcher"))\n'
            'resource.type="dataflow_step"\n'
            'resource.labels.job_id="2023-09-18_07_09_47-16912541725945987225"\n'
            'timestamp >= "2023-09-18T14:09:47.864426Z"\n'
            '(severity >= ERROR OR "Error:")\n',
        )

    @patch(
        "recidiviz.airflow.dags.operators.recidiviz_dataflow_operator.time.sleep",
        return_value=1,
    )
    def test_execute_retry(self, _sleep_patch: MagicMock) -> None:
        self.mock_hook.is_job_dataflow_running.return_value = False
        self.mock_hook.start_flex_template.side_effect = Exception("Job has failed!")
        self.mock_get_job.return_value = {
            "currentState": DataflowJobStatus.JOB_STATE_FAILED,
            "id": "2023-09-18_07_09_47-16912541725945987225",
            "createTime": "2023-09-18T14:09:47.864426Z",
        }

        self.mock_logs_client.list_entries.return_value = [
            MagicMock(payload="ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS")
        ]

        with self.assertLogs() as logs, self.assertRaises(Exception):
            _ = execute_task(self.dag, self.dataflow_task)

        self.assertIn(
            "Retrying once more in 5 minutes due to zonal resource exhaustion",
            "\n".join(logs.output),
        )
        # With no fallback_regions configured, the retry stays in the original region.
        self.assertEqual(
            "us-central1",
            self.mock_hook.start_flex_template.call_args.kwargs["location"],
        )

    @patch(
        "recidiviz.airflow.dags.operators.recidiviz_dataflow_operator.time.sleep",
        return_value=1,
    )
    def test_execute_retry_samples_fallback_region(
        self, _sleep_patch: MagicMock
    ) -> None:
        original_subnetwork = (
            "https://www.googleapis.com/compute/v1/projects/test-project/"
            "regions/us-central1/subnetworks/default"
        )
        task = RecidivizDataflowFlexTemplateOperator(
            task_id="fallback_task",
            project_id="test-project",
            body={
                "launchParameter": {
                    "jobName": "test-job",
                    "environment": {"subnetwork": original_subnetwork},
                }
            },
            location="us-central1",
            fallback_regions=["us-central1", "us-east1", "us-west2"],
        )
        self.mock_hook.is_job_dataflow_running.return_value = False
        self.mock_hook.start_flex_template.side_effect = Exception("Job has failed!")
        self.mock_get_job.return_value = {
            "currentState": DataflowJobStatus.JOB_STATE_FAILED,
            "id": "2023-09-18_07_09_47-16912541725945987225",
            "createTime": "2023-09-18T14:09:47.864426Z",
        }
        self.mock_logs_client.list_entries.return_value = [
            MagicMock(payload="ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS")
        ]

        with self.assertRaises(Exception):
            _ = execute_task(self.dag, task)

        # Initial launch + one retry in a sampled fallback region.
        self.assertEqual(2, self.mock_hook.start_flex_template.call_count)
        retry_kwargs = self.mock_hook.start_flex_template.call_args.kwargs
        self.assertNotEqual("us-central1", retry_kwargs["location"])
        self.assertIn(retry_kwargs["location"], {"us-east1", "us-west2"})
        self.assertEqual(task.location, retry_kwargs["location"])
        # The region-specific subnetwork is rewritten to the new region.
        self.assertEqual(
            f"/regions/{task.location}/subnetworks/default",
            retry_kwargs["body"]["launchParameter"]["environment"]["subnetwork"].split(
                "test-project"
            )[1],
        )
