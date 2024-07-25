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
"""Tests for the raw data import DAG"""
from typing import Dict
from unittest.mock import patch

from airflow.models import DAG, BaseOperator, DagBag
from airflow.utils.state import DagRunState
from sqlalchemy.orm import Session

from recidiviz.airflow.dags.monitoring.dag_registry import get_raw_data_import_dag_id
from recidiviz.airflow.tests.test_utils import DAG_FOLDER, AirflowIntegrationTest
from recidiviz.airflow.tests.utils.dag_helper_functions import (
    FakeFailureOperator,
    fake_operator_with_return_value,
    fake_task_function_with_return_value,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

_PROJECT_ID = "recidiviz-testing"


class RawDataImportDagSequencingTest(AirflowIntegrationTest):
    """Tests for task sequencing for the raw data import dag"""

    def setUp(self) -> None:
        self.dag_id = get_raw_data_import_dag_id(_PROJECT_ID)
        self.environment_patcher = patch(
            "os.environ",
            {
                "GCP_PROJECT": _PROJECT_ID,
            },
        )
        self.environment_patcher.start()

    def tearDown(self) -> None:
        self.environment_patcher.stop()

    def test_import(self) -> None:
        """Just tests that raw data import dag file can be imported; needs to be done
        here to have env overrides active
        """

        # pylint: disable=C0415 import-outside-toplevel
        # pylint: disable=unused-import
        from recidiviz.airflow.dags.raw_data_import_dag import raw_data_import_dag

    def test_lock_before_anything_else(self) -> None:
        """Tests that we acquire the resource locks before we do anything else in
        state-specific import branches
        """

        dag = DagBag(dag_folder=DAG_FOLDER, include_examples=False).dags[self.dag_id]
        state_specific_tasks_dag = dag.partial_subset(
            task_ids_or_regex=r"us_[a-z][a-z]_(?:primary|secondary)_import_branch\.",
            include_upstream=False,
        )

        for root_task in state_specific_tasks_dag.roots:
            assert "acquire_raw_data_resource_locks" in root_task.task_id

    def test_lock_after_everything_else(self) -> None:
        """Tests that we release the resource locks as the last thing we do in
        state-specific import branches
        """

        dag = DagBag(dag_folder=DAG_FOLDER, include_examples=False).dags[self.dag_id]
        branch_end_and_one_before = dag.partial_subset(
            task_ids_or_regex=r"branch_end",
            include_upstream=False,
            include_direct_upstream=True,
        )

        for root_task in branch_end_and_one_before.roots:
            assert "release_raw_data_resource_locks" in root_task.task_id

    def test_step_2_sequencing(self) -> None:
        dag = DagBag(dag_folder=DAG_FOLDER, include_examples=False).dags[self.dag_id]
        step_2_root = dag.partial_subset(
            task_ids_or_regex=r"list_normalized_unprocessed_gcs_file_paths",
            include_upstream=False,
            include_downstream=True,
        )
        step_2_task_ids = [
            ["get_all_unprocessed_gcs_file_metadata"],
            ["get_all_unprocessed_bq_file_metadata"],
            ["coalesce_results_and_errors", "split_by_pre_import_normalization_type"],
        ]

        for root in step_2_root.roots:
            assert "list_normalized_unprocessed_gcs_file_paths" in root.task_id
            curr_task = root
            for step_2_task_id in step_2_task_ids:
                next_task = None
                assert len(curr_task.downstream_list) == len(step_2_task_id)
                for downstream_task in curr_task.downstream_list:
                    assert any(
                        task_id in downstream_task.task_id for task_id in step_2_task_id
                    )
                    if step_2_task_id[-1] in downstream_task.task_id:
                        next_task = downstream_task
                curr_task = next_task

    def test_step_3_sequencing(self) -> None:
        dag = DagBag(dag_folder=DAG_FOLDER, include_examples=False).dags[self.dag_id]
        step_3_root = dag.partial_subset(
            task_ids_or_regex=r"split_by_pre_import_normalization_type",
            include_upstream=False,
            include_downstream=True,
        )
        ordered_step_3_task_ids = [
            [
                "coalesce_import_ready_files",
                "regroup_and_verify_file_chunks",
                "generate_file_chunking_pod_arguments",
            ],
            "raw_data_file_chunking",
            [
                "coalesce_results_and_errors",
                "raise_file_chunking_errors",
                "generate_chunk_processing_pod_arguments",
            ],
            "raw_data_chunk_normalization",
            "regroup_and_verify_file_chunks",
            [
                "coalesce_results_and_errors",
                "coalesce_import_ready_files",
                "raise_chunk_normalization_errors",
            ],
        ]
        for root in step_3_root.roots:
            assert "split_by_pre_import_normalization_type" in root.task_id
            curr_task = root
            for step_3_task_id in ordered_step_3_task_ids:
                if isinstance(step_3_task_id, list):
                    assert len(curr_task.downstream_list) == len(step_3_task_id)
                    for downstream_task in curr_task.downstream_list:
                        # The downstream list is unordered so assert any of the expected task ids are found in the downstream task
                        assert any(
                            task_id in downstream_task.task_id
                            for task_id in step_3_task_id
                        )
                        if step_3_task_id[-1] in downstream_task.task_id:
                            # Assign next task to be the one corresponding to the last element in the expected task id list
                            # It doesn't matter that it actually executes last but the downstream tasks depend on this task's output
                            next_task = downstream_task
                else:
                    assert len(curr_task.downstream_list) == 1
                    next_task = curr_task.downstream_list[0]
                    assert step_3_task_id in next_task.task_id
                curr_task = next_task

    def test_step_4_sequencing(self) -> None:
        dag = DagBag(dag_folder=DAG_FOLDER, include_examples=False).dags[self.dag_id]
        step_4_root = dag.partial_subset(
            task_ids_or_regex=r"raise_chunk_normalization_errors",
            include_upstream=False,
            include_downstream=True,
        )
        ordered_step_4_task_ids = [
            ["coalesce_import_ready_files"],
            ["load_and_prep_paths_for_batch"],
            [
                "coalesce_results_and_errors",
                "raise_load_prep_errors",
                "generate_append_batches",
            ],
            [
                "coalesce_results_and_errors",
                "raise_load_prep_errors",
                "append_ready_file_batches_from_generate_append_batches",
            ],
            ["append_to_raw_data_table_for_batch"],
            ["coalesce_results_and_errors", "raise_append_errors"],
        ]

        for root in step_4_root.roots:
            assert "raise_chunk_normalization_errors" in root.task_id
            curr_task = root
            for step_4_task_id in ordered_step_4_task_ids:
                next_task = None
                assert len(curr_task.downstream_list) == len(step_4_task_id)
                for downstream_task in curr_task.downstream_list:
                    assert any(
                        task_id in downstream_task.task_id for task_id in step_4_task_id
                    )
                    if step_4_task_id[-1] in downstream_task.task_id:
                        next_task = downstream_task
                curr_task = next_task

    def test_step_5_sequencing(self) -> None:
        dag = DagBag(dag_folder=DAG_FOLDER, include_examples=False).dags[self.dag_id]
        step_5_root = dag.partial_subset(
            task_ids_or_regex=r"coalesce_results_and_errors",
            include_upstream=False,
            include_downstream=True,
        )
        ordered_step_5_task_ids_paths = [
            [
                [
                    "move_successfully_imported_paths_to_storage",
                    "clean_up_temporary_files",
                    "clean_up_temporary_tables",
                    "write_import_sessions",
                ],
                ["write_file_processed_time"],
                ["ensure_release_resource_locks_release_if_acquired"],
                ["release_raw_data_resource_locks"],
            ],
            [
                [
                    "move_successfully_imported_paths_to_storage",
                    "write_import_sessions",
                    "clean_up_temporary_tables",
                    "clean_up_temporary_files",
                ],
                ["ensure_release_resource_locks_release_if_acquired"],
                ["release_raw_data_resource_locks"],
            ],
            [
                [
                    "move_successfully_imported_paths_to_storage",
                    "write_import_sessions",
                    "clean_up_temporary_files",
                    "clean_up_temporary_tables",
                ],
                ["ensure_release_resource_locks_release_if_acquired"],
                ["release_raw_data_resource_locks"],
            ],
        ]

        for root in step_5_root.roots:
            assert "coalesce_results_and_errors" in root.task_id
            for step_5_path in ordered_step_5_task_ids_paths:
                curr_task = root
                for step_5_task_ids in step_5_path:
                    next_task = None
                    assert len(curr_task.downstream_list) == len(step_5_task_ids)
                    for downstream_task in curr_task.downstream_list:
                        assert any(
                            task_id in downstream_task.task_id
                            for task_id in step_5_task_ids
                        )
                        if step_5_task_ids[-1] in downstream_task.task_id:
                            next_task = downstream_task
                    curr_task = next_task


class RawDataImportDagIntegrationTest(AirflowIntegrationTest):
    """Integration tests for the raw data import dag"""

    def setUp(self) -> None:
        super().setUp()
        self.environment_patcher = patch(
            "os.environ",
            {
                "GCP_PROJECT": _PROJECT_ID,
            },
        )
        self.environment_patcher.start()
        test_state_codes = [StateCode.US_XX, StateCode.US_YY]
        test_state_code_and_instance_pairs = [
            (StateCode.US_XX, DirectIngestInstance.PRIMARY),
            (StateCode.US_XX, DirectIngestInstance.SECONDARY),
            (StateCode.US_YY, DirectIngestInstance.PRIMARY),
            (StateCode.US_YY, DirectIngestInstance.SECONDARY),
        ]
        self.raw_data_enabled_pairs = patch(
            "recidiviz.airflow.dags.raw_data.raw_data_branching.get_raw_data_dag_enabled_state_and_instance_pairs",
            return_value=test_state_code_and_instance_pairs,
        )
        self.raw_data_enabled_pairs.start()
        self.raw_data_enabled_pairs_two = patch(
            "recidiviz.airflow.dags.raw_data.initialize_raw_data_dag_group.get_raw_data_dag_enabled_state_and_instance_pairs",
            return_value=test_state_code_and_instance_pairs,
        )
        self.raw_data_enabled_pairs_two.start()
        self.raw_data_enabled_states = patch(
            "recidiviz.airflow.dags.raw_data.raw_data_branching.get_raw_data_dag_enabled_states",
            return_value=test_state_codes,
        )
        self.raw_data_enabled_states.start()
        self.cloud_sql_query_operator_patcher = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.CloudSqlQueryOperator",
            side_effect=fake_operator_with_return_value({}),
        )
        self.mock_cloud_sql_patcher = self.cloud_sql_query_operator_patcher.start()
        self.gcs_operator_patcher = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.DirectIngestListNormalizedUnprocessedFilesOperator",
            side_effect=fake_operator_with_return_value([]),
        )
        self.gcs_operator_patcher.start()
        self.kpo_operator_patcher = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.RecidivizKubernetesPodOperator",
            side_effect=fake_operator_with_return_value([]),
        )
        self.kpo_operator_patcher.start()

        self.file_chunking_args_patcher = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.generate_file_chunking_pod_arguments.function",
            side_effect=fake_task_function_with_return_value([]),
        )
        self.file_chunking_args_patcher.start()

        self.chunk_processing_args_patcher = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.generate_chunk_processing_pod_arguments.function",
            side_effect=fake_task_function_with_return_value([]),
        )
        self.chunk_processing_args_patcher.start()

        self.verify_file_chunks_patcher = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.regroup_and_verify_file_chunks.function",
            side_effect=fake_task_function_with_return_value([]),
        )
        self.verify_file_chunks_patcher.start()

        self.raise_errors_patcher = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.raise_chunk_normalization_errors.function",
            side_effect=fake_task_function_with_return_value(None),
        )
        self.raise_errors_patcher.start()

        self.split_by_norm = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.split_by_pre_import_normalization_type.function",
            side_effect=fake_task_function_with_return_value({}),
        )
        self.split_by_norm.start()

        self.coalesce_files = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.coalesce_import_ready_files.function",
            side_effect=fake_task_function_with_return_value([]),
        )
        self.coalesce_files.start()
        self.clean_temp_files = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.clean_up_temporary_files.function",
            side_effect=fake_task_function_with_return_value(None),
        )
        self.clean_temp_files.start()
        self.clean_temp_tables = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.clean_up_temporary_tables.function",
            side_effect=fake_task_function_with_return_value(None),
        )
        self.clean_temp_tables.start()
        self.rename_paths = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.move_successfully_imported_paths_to_storage.function",
            side_effect=fake_task_function_with_return_value(None),
        )
        self.rename_paths.start()

    def tearDown(self) -> None:
        self.environment_patcher.stop()
        self.raw_data_enabled_pairs.stop()
        self.raw_data_enabled_pairs_two.stop()
        self.raw_data_enabled_states.stop()
        self.cloud_sql_query_operator_patcher.stop()
        self.gcs_operator_patcher.stop()
        self.kpo_operator_patcher.stop()
        self.file_chunking_args_patcher.stop()
        self.chunk_processing_args_patcher.stop()
        self.verify_file_chunks_patcher.stop()
        self.raise_errors_patcher.stop()
        self.split_by_norm.stop()
        self.coalesce_files.stop()
        self.clean_temp_files.stop()
        self.clean_temp_tables.stop()
        self.rename_paths.stop()
        super().tearDown()

    def _create_dag(self) -> DAG:
        # pylint: disable=import-outside-toplevel
        from recidiviz.airflow.dags.raw_data_import_dag import (
            create_raw_data_import_dag,
        )

        return create_raw_data_import_dag()

    def test_branching_primary(self) -> None:
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf={
                    "ingest_instance": "PRIMARY",
                },
                expected_skipped_ids=[
                    "_secondary_import_branch_start",
                    r".*_secondary_import_branch\.(?!ensure_release_resource_locks_release_if_acquired)",
                    # these steps are skipped as mapped tasks (and downstream tasks)
                    # are skipped when the input is an empty list
                    r"raw_data_branching.*_primary_import_branch.biq_query_load.*",
                    r"raw_data_branching.*_primary_import_branch.cleanup_and_storage.*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

    def test_branching_secondary(self) -> None:
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf={
                    "ingest_instance": "SECONDARY",
                    "state_code_filter": StateCode.US_XX.value,
                },
                expected_skipped_ids=[
                    r".*_primary_import_branch_start",
                    r".*_primary_import_branch\.(?!ensure_release_resource_locks_release)",
                    "us_yy_secondary_import_branch_start",
                    r".*us_yy_secondary_import_branch\.(?!ensure_release_resource_locks_release_if_acquired)",
                    # these steps are skipped as mapped tasks (and downstream tasks)
                    # are skipped when the input is an empty list
                    r"raw_data_branching.us_xx_secondary_import_branch.biq_query_load.*",
                    r"raw_data_branching.us_xx_secondary_import_branch.cleanup_and_storage.*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

    def test_branching_resource_lock_fails(self) -> None:
        def _resource_lock_fails(*args: Dict, **kwargs: Dict) -> BaseOperator:
            if "acquire_raw_data_resource_locks" in kwargs["task_id"]:
                return FakeFailureOperator(*args, **kwargs)
            return fake_operator_with_return_value([])(*args, **kwargs)

        self.mock_cloud_sql_patcher.side_effect = _resource_lock_fails
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf={
                    "ingest_instance": "PRIMARY",
                },
                expected_failure_ids=[
                    r".*_primary_import_branch\.acquire_raw_data_resource_locks",
                    r".*_primary_import_branch\.list_normalized_unprocessed_gcs_file_paths",
                    r".*_primary_import_branch\.get_all_unprocessed_gcs_file_metadata",
                    r".*_primary_import_branch\.get_all_unprocessed_bq_file_metadata",
                    r".*_primary_import_branch\.split_by_pre_import_normalization_type",
                    r".*_primary_import_branch\.coalesce_import_ready_files",
                    r".*_primary_import_branch\.pre_import_normalization\.*",
                    r".*_primary_import_branch\.biq_query_load\.*",
                    r".*_primary_import_branch\.cleanup_and_storage.*",
                ],
                expected_skipped_ids=[
                    "_secondary_import_branch_start",
                    r".*_secondary_import_branch\.(?!ensure_release_resource_locks_release_if_acquired)",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)
