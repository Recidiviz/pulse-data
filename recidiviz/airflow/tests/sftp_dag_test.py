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
"""Unit test to test the SFTP DAG."""
import os
import re
import unittest
from unittest.mock import patch

from airflow.models import DagBag
from google.cloud.tasks_v2.types.queue import Queue

from recidiviz.airflow.dags.sftp_dag import get_running_queue_instances
from recidiviz.airflow.tests.test_utils import AIRFLOW_WORKING_DIRECTORY, DAG_FOLDER
from recidiviz.calculator import pipeline

_PROJECT_ID = "recidiviz-staging"
CALC_PIPELINE_CONFIG_FILE_RELATIVE_PATH = os.path.join(
    os.path.relpath(
        os.path.dirname(pipeline.__file__),
        start=AIRFLOW_WORKING_DIRECTORY,
    ),
    "calculation_pipeline_templates.yaml",
)

_START_SFTP_TASK_ID = "start_sftp"
_END_SFTP_TASK_ID = "end_sftp"


@patch(
    "os.environ",
    {
        "GCP_PROJECT": _PROJECT_ID,
        "CONFIG_FILE": CALC_PIPELINE_CONFIG_FILE_RELATIVE_PATH,
    },
)
class TestSftpPipelineDag(unittest.TestCase):
    """Tests the sftp pipeline DAG."""

    SFTP_DAG_ID = f"{_PROJECT_ID}_sftp_dag"

    def test_start_sftp_upstream_of_state_specific_tasks(self) -> None:
        """Tests that the `start_sftp` check happens before the state task group
        starts executing."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.SFTP_DAG_ID]
        state_specific_tasks_dag = dag.partial_subset(
            task_ids_or_regex=r"US_[A-Z][A-Z]",
            include_downstream=False,
            include_upstream=True,
        )
        self.assertNotEqual(0, len(state_specific_tasks_dag.task_ids))

        upstream_tasks = set()
        for task in state_specific_tasks_dag.tasks:
            upstream_tasks.update(task.upstream_task_ids)

        self.assertIn(_START_SFTP_TASK_ID, upstream_tasks)

    def test_check_config_upstream_of_remote_file_discovery_tasks(self) -> None:
        """Tests that the `check_config` check happens before we discover remote file
        discovery tasks."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.SFTP_DAG_ID]
        state_specific_tasks_dag = dag.partial_subset(
            task_ids_or_regex=r"US_[A-Z][A-Z]\.remote_file_discovery.find_sftp_files_to_download",
            include_downstream=False,
            include_upstream=False,
            include_direct_upstream=True,
        )
        self.assertNotEqual(0, len(state_specific_tasks_dag.task_ids))

        upstream_tasks = set()
        for task in state_specific_tasks_dag.tasks:
            upstream_tasks.update(task.upstream_task_ids)

        for task_id in upstream_tasks:
            self.assertTrue("check_config" in task_id)

    def test_mark_files_discovered_upstream_of_gather_discovered_files(self) -> None:
        """Tests that gathering all discovered files occurs after marking new files discovered."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.SFTP_DAG_ID]
        state_specific_tasks_dag = dag.partial_subset(
            task_ids_or_regex=r"US_[A-Z][A-Z].*gather_discovered.*files",
            include_downstream=False,
            include_upstream=False,
            include_direct_upstream=True,
        )
        self.assertNotEqual(0, len(state_specific_tasks_dag.task_ids))

        upstream_tasks = set()
        for task in state_specific_tasks_dag.tasks:
            upstream_tasks.update(task.upstream_task_ids)

        for task_id in upstream_tasks:
            self.assertRegex(task_id, r"US_[A-Z][A-Z].*mark_.*files_discovered")

    def test_mark_files_discovered_upstream_of_mark_files_loaded(self) -> None:
        """Tests that marking files uploaded or downloaded occurs after marking them discovered."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.SFTP_DAG_ID]
        state_specific_tasks_dag = dag.partial_subset(
            task_ids_or_regex=r"US_[A-Z][A-Z].*mark_.*files_.*loaded",
            include_downstream=False,
            include_upstream=True,
        )
        self.assertNotEqual(0, len(state_specific_tasks_dag.task_ids))

        upstream_tasks = set()
        for task in state_specific_tasks_dag.tasks:
            for task_id in task.upstream_task_ids:
                if re.match(r"US_[A-Z][A-Z].*mark_.*files_.*discovered", task_id):
                    upstream_tasks.add(task_id)

        self.assertNotEqual(0, len(upstream_tasks))

    def test_sftp_and_gcs_operators_have_retries(self) -> None:
        task_types_with_retries = [
            "remote_file_download.download_sftp_files",
            "remote_file_download.post_process_downloaded_files",
            "ingest_ready_file_upload.upload_files_to_ingest_bucket",
        ]
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.SFTP_DAG_ID]
        for task in dag.tasks:
            for task_type in task_types_with_retries:
                if task_type in task.task_id:
                    self.assertEqual(3, task.retries)

    def test_get_running_queue_instances(self) -> None:
        primary_queue_status = {
            "name": "projects/recidiviz-staging/locations/us-east1/queues/direct-ingest-state-us-me-scheduler",
            "rate_limits": {
                "max_dispatches_per_second": 100.0,
                "max_burst_size": 20,
                "max_concurrent_dispatches": 1,
            },
            "retry_config": {
                "max_attempts": 5,
                "min_backoff": "0.100s",
                "max_backoff": "3600s",
                "max_doublings": 16,
            },
            "state": Queue.State.RUNNING,
            "purge_time": "2022-01-24T18:39:30.777006Z",
            "stackdriver_logging_config": {"sampling_ratio": 1.0},
        }

        secondary_queue_status = {
            "name": "projects/recidiviz-staging/locations/us-east1/queues/direct-ingest-state-us-me-scheduler-secondary",
            "rate_limits": {
                "max_dispatches_per_second": 100.0,
                "max_burst_size": 20,
                "max_concurrent_dispatches": 1,
            },
            "retry_config": {
                "max_attempts": 5,
                "min_backoff": "0.100s",
                "max_backoff": "3600s",
                "max_doublings": 16,
            },
            "state": Queue.State.RUNNING,
            "purge_time": "2022-01-26T01:07:46.518478Z",
            "stackdriver_logging_config": {"sampling_ratio": 1.0},
        }

        instances = get_running_queue_instances(
            primary_queue_status, secondary_queue_status
        )
        self.assertEqual(["primary", "secondary"], instances)

        # Now start with primary paused
        primary_queue_status["state"] = Queue.State.PAUSED

        instances = get_running_queue_instances(
            primary_queue_status, secondary_queue_status
        )
        self.assertEqual(["secondary"], instances)

        # Now start with secondary paused as well
        secondary_queue_status["state"] = Queue.State.PAUSED
        instances = get_running_queue_instances(
            primary_queue_status, secondary_queue_status
        )
        self.assertEqual([], instances)
