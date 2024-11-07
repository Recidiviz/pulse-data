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
from unittest.mock import patch

from airflow.models import DagBag
from airflow.utils.trigger_rule import TriggerRule

import recidiviz
from recidiviz.airflow.dags.sftp.metadata import END_SFTP, START_SFTP, TASK_RETRIES
from recidiviz.airflow.tests.test_utils import (
    AIRFLOW_WORKING_DIRECTORY,
    DAG_FOLDER,
    AirflowIntegrationTest,
)

_PROJECT_ID = "recidiviz-staging"
CALC_PIPELINE_CONFIG_FILE_RELATIVE_PATH = os.path.join(
    os.path.relpath(
        os.path.dirname(recidiviz.__file__),
        start=AIRFLOW_WORKING_DIRECTORY,
    ),
    "pipelines/calculation_pipeline_templates.yaml",
)


@patch(
    "os.environ",
    {
        "GCP_PROJECT": _PROJECT_ID,
        "CONFIG_FILE": CALC_PIPELINE_CONFIG_FILE_RELATIVE_PATH,
    },
)
class TestSftpPipelineDag(AirflowIntegrationTest):
    """Tests the sftp pipeline DAG."""

    def setUp(self) -> None:
        self.raw_data_dag_enabled_patcher = patch(
            "recidiviz.airflow.dags.utils.dag_orchestration_utils.is_raw_data_import_dag_enabled"
        )
        self.raw_data_dag_enabled_mock = self.raw_data_dag_enabled_patcher.start()
        self.raw_data_dag_enabled_mock.return_value = False
        super().setUp()

    def tearDown(self) -> None:
        self.raw_data_dag_enabled_patcher.stop()
        super().tearDown()

    SFTP_DAG_ID = f"{_PROJECT_ID}_sftp_dag"

    def test_import(self) -> None:
        """Just tests that the sftp_dag file can be imported"""
        # Need to import calculation_dag inside test suite so environment variables are
        # set before importing, otherwise sftp_dag will raise an Error and not
        # import.

        # pylint: disable=C0415 import-outside-toplevel
        from recidiviz.airflow.dags.sftp_dag import dag  # pylint: disable=unused-import

        # If nothing fails, this test passes

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

        self.assertIn(START_SFTP, upstream_tasks)

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
                    self.assertEqual(TASK_RETRIES, task.retries)

    def test_acquire_permission_happens_at_start_of_ingest_file_upload(self) -> None:
        """Tests that releasing permission happens at before ingest file upload"""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.SFTP_DAG_ID]
        state_specific_tasks_dag = dag.partial_subset(
            task_ids_or_regex=r"US_[A-Z][A-Z].ingest_ready_file_upload",
            include_downstream=True,
            include_upstream=False,
            include_direct_upstream=False,
        )
        self.assertNotEqual(0, len(state_specific_tasks_dag.task_ids))

        # makes sure that acquire_permission_for_ingest_file_upload happens before we upload
        # to the ingest bucket
        for task in state_specific_tasks_dag.roots:
            assert any(
                "acquire_permission_for_ingest_file_upload" in task_id
                for task_id in task.downstream_task_ids
            )

    def test_release_permission_happens_if_we_uploaded(self) -> None:
        """Tests that the last thing in each branch is release_permission_for_ingest_file_upload,
        if we uploaded files.
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.SFTP_DAG_ID]
        state_specific_tasks_dag = dag.partial_subset(
            task_ids_or_regex=END_SFTP,
            include_downstream=False,
            include_upstream=False,
            include_direct_upstream=True,
        )
        self.assertNotEqual(0, len(state_specific_tasks_dag.task_ids))

        upstream_tasks = set()
        for task in state_specific_tasks_dag.tasks:
            upstream_tasks.update(task.upstream_task_ids)

        for task_id in upstream_tasks:
            self.assertTrue(
                "release_permission_for_ingest_file_upload" in task_id
                or "do_not_upload_ingest_ready_files" in task_id
            )

    def test_all_done_operators_downstream_of_check_if_ingest_ready_files_have_stabilized_are_directly_downstream(
        self,
    ) -> None:
        """Test that makes sure that all ALL_DONE operators downstream of the
        `check_if_ingest_ready_files_have_stabilized` task are directly downstream
        (and therefore can be skipped if their branch is not chosen)
        """

        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.SFTP_DAG_ID]
        downstream_of_stabilized = dag.partial_subset(
            task_ids_or_regex="check_if_ingest_ready_files_have_stabilized",
            include_downstream=True,
            include_upstream=False,
            include_direct_upstream=False,
        )
        self.assertNotEqual(0, len(downstream_of_stabilized.task_ids))

        downstream_all_done_tasks = set()
        directly_downstream_of_check_if_ingest_ready_files_have_stabilized_tasks = set()
        for task in downstream_of_stabilized.tasks:
            if task.trigger_rule == TriggerRule.ALL_DONE:
                if "check_if_ingest_ready_files_have_stabilized" in task.task_id:
                    directly_downstream_of_check_if_ingest_ready_files_have_stabilized_tasks.update(
                        task.downstream_task_ids
                    )
                elif END_SFTP not in task.task_id:
                    downstream_all_done_tasks.add(task.task_id)

        if (
            missing := downstream_all_done_tasks
            - directly_downstream_of_check_if_ingest_ready_files_have_stabilized_tasks
        ):
            raise ValueError(
                f"Found tasks downstream of `check_if_ingest_ready_files_have_stabilized` "
                f"that have an ALL_DONE trigger rule but are not directly downstream of "
                f"check_if_ingest_ready_files_have_stabilized: [{missing}]"
            )
