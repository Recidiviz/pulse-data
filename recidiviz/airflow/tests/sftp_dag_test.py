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
import unittest
from unittest.mock import patch

from airflow.models import DagBag

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
            task_ids_or_regex=r"US_[A-Z][A-Z]\.remote_file_discovery.find_sftp_files_to_download",
            include_downstream=False,
            include_upstream=True,
        )
        self.assertNotEqual(0, len(state_specific_tasks_dag.task_ids))

        upstream_tasks = set()
        for task in state_specific_tasks_dag.tasks:
            upstream_tasks.update(task.upstream_task_ids)

        self.assertIn(_START_SFTP_TASK_ID, upstream_tasks)

    def test_set_locks_upstream_of_rest_of_state_specific_tasks(self) -> None:
        """Tests that the state-specific locks get set before the rest of the files
        start executing."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.SFTP_DAG_ID]
        state_specific_lock_tasks_dag = dag.partial_subset(
            task_ids_or_regex=r"US_[A-Z][A-Z]\.remote_file_discovery.find_sftp_files_to_download",
            include_downstream=False,
            include_upstream=False,
            include_direct_upstream=True,
        )
        self.assertNotEqual(0, len(state_specific_lock_tasks_dag.task_ids))

        upstream_tasks = set()
        for task in state_specific_lock_tasks_dag.tasks:
            upstream_tasks.update(task.upstream_task_ids)

        for task in upstream_tasks:
            self.assertRegex(task, r"US_[A-Z][A-Z]\..*_lock")

    def test_release_locks_downstream_of_rest_of_state_specific_tasks(self) -> None:
        """Tests that the state-specific locks get released after the rest of the state
        tasks get executed and right before the end."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.SFTP_DAG_ID]
        end_dag = dag.partial_subset(
            task_ids_or_regex=[_END_SFTP_TASK_ID],
            include_direct_upstream=True,
            include_upstream=False,
            include_downstream=False,
        )
        self.assertNotEqual(0, len(end_dag.task_ids))

        upstream_tasks = set()
        for task in end_dag.tasks:
            upstream_tasks.update(task.upstream_task_ids)
        for task in upstream_tasks:
            self.assertRegex(task, r"US_[A-Z][A-Z]\.release\_.*\_lock")

        state_specific_dag = dag.partial_subset(
            task_ids_or_regex=r"US_[A-Z][A-Z]\.release\_.*\_lock",
            include_upstream=True,
            include_downstream=False,
        )
        self.assertNotEqual(0, state_specific_dag.leaves)
        for task in state_specific_dag.leaves:
            if task != "start_sftp":
                self.assertRegex(task.task_id, r"US_[A-Z][A-Z]\..*")
