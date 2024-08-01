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
import datetime
import json
from typing import Any, Callable, Dict, List, Tuple
from unittest.mock import patch

import attr
from airflow.models import DAG, DagBag
from airflow.models.baseoperator import partial
from airflow.models.mappedoperator import OperatorPartial
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.state import DagRunState
from sqlalchemy import text
from sqlalchemy.orm import Session

from recidiviz.airflow.dags.monitoring.dag_registry import get_raw_data_import_dag_id
from recidiviz.airflow.dags.raw_data.metadata import (
    IMPORT_READY_FILES,
    REQUIRES_PRE_IMPORT_NORMALIZATION_FILES,
    REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_METADATA,
)
from recidiviz.airflow.tests.test_utils import DAG_FOLDER, AirflowIntegrationTest
from recidiviz.airflow.tests.utils.dag_helper_functions import (
    fake_failing_operator_constructor,
    fake_k8s_operator_with_return_value,
    fake_operator_with_return_value,
    fake_task_function_with_return_value,
)
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.cloud_storage.gcsfs_csv_chunk_boundary_finder import CsvChunkBoundary
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import (
    AppendReadyFile,
    AppendSummary,
    ImportReadyFile,
    PreImportNormalizationType,
    PreImportNormalizedCsvChunkResult,
    RawBigQueryFileMetadata,
    RawDataAppendImportError,
    RawFileLoadAndPrepError,
    RawFileProcessingError,
    RawGCSFileMetadata,
    RequiresPreImportNormalizationFile,
)
from recidiviz.persistence.database.schema.operations.schema import OperationsBase
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.utils.airflow_types import (
    BatchedTaskInstanceOutput,
    MappedBatchedTaskOutput,
)
from recidiviz.utils.types import assert_type

_PROJECT_ID = "recidiviz-testing"


def _comparable(files: List[ImportReadyFile]) -> List[Dict]:
    comparable_files = []
    for file in files:
        comparable_files.append(
            {
                k: v if not isinstance(v, list) else set(v)
                for k, v in attr.asdict(file, recurse=False).items()
            }
        )
    return list(sorted(comparable_files, key=lambda x: x["file_id"]))


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

    metas = [OperationsBase]
    conn_id = "operations_postgres_conn_id"

    def setUp(self) -> None:
        super().setUp()
        self.fs = FakeGCSFileSystem()
        self.dag_id = get_raw_data_import_dag_id(_PROJECT_ID)
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
        self.cloud_sql_db_hook_patcher = patch(
            "recidiviz.airflow.dags.operators.cloud_sql_query_operator.CloudSQLDatabaseHook"
        )
        self.mock_cloud_sql_db_hook = self.cloud_sql_db_hook_patcher.start()
        self.mock_cloud_sql_db_hook().get_database_hook.return_value = PostgresHook(
            self.conn_id
        )
        self.gcs_operator_patcher = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.DirectIngestListNormalizedUnprocessedFilesOperator",
            side_effect=fake_operator_with_return_value([]),
        )
        self.list_normalized_unprocessed_files_mock = self.gcs_operator_patcher.start()
        self.kpo_operator_patcher = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.RecidivizKubernetesPodOperator",
            side_effect=fake_operator_with_return_value([]),
        )
        self.kpo_operator_mock = self.kpo_operator_patcher.start()

        self.file_chunking_args_patcher = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.generate_file_chunking_pod_arguments.function",
            side_effect=fake_task_function_with_return_value([]),
        )
        self.file_chunking_args_mock = self.file_chunking_args_patcher.start()

        self.chunk_processing_args_patcher = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.generate_chunk_processing_pod_arguments.function",
            side_effect=fake_task_function_with_return_value([]),
        )
        self.chunk_processing_args_mock = self.chunk_processing_args_patcher.start()

        self.verify_file_chunks_patcher = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.regroup_and_verify_file_chunks.function",
            side_effect=fake_task_function_with_return_value([]),
        )
        self.verify_file_chunks_mock = self.verify_file_chunks_patcher.start()

        self.raise_errors_patcher = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.raise_chunk_normalization_errors.function",
            side_effect=fake_task_function_with_return_value(None),
        )
        self.raise_errors_mock = self.raise_errors_patcher.start()

        self.split_by_norm = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.split_by_pre_import_normalization_type.function",
            side_effect=fake_task_function_with_return_value({}),
        )
        self.split_by_norm_mock = self.split_by_norm.start()

        self.coalesce_files = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.coalesce_import_ready_files.function",
            side_effect=fake_task_function_with_return_value([]),
        )
        self.coalesce_files_mock = self.coalesce_files.start()
        self.clean_up_gcsfs_patcher = patch(
            "recidiviz.airflow.dags.raw_data.clean_up_tasks.GcsfsFactory.build",
            return_value=self.fs,
        )
        self.clean_up_gcsfs_patcher.start()
        self.clean_up_bq_patcher = patch(
            "recidiviz.airflow.dags.raw_data.clean_up_tasks.BigQueryClientImpl",
        )
        self.clean_up_bq_mock = self.clean_up_bq_patcher.start()

        self.region_module_patch = patch(
            "recidiviz.airflow.dags.raw_data.utils.direct_ingest_regions_module",
            fake_regions,
        )
        self.region_module_patch.start()

    def tearDown(self) -> None:
        self.raw_data_enabled_pairs.stop()
        self.raw_data_enabled_pairs_two.stop()
        self.raw_data_enabled_states.stop()
        self.cloud_sql_db_hook_patcher.stop()
        self.gcs_operator_patcher.stop()
        self.kpo_operator_patcher.stop()
        self.file_chunking_args_patcher.stop()
        self.chunk_processing_args_patcher.stop()
        self.verify_file_chunks_patcher.stop()
        self.raise_errors_patcher.stop()
        self.split_by_norm.stop()
        self.coalesce_files.stop()
        self.clean_up_gcsfs_patcher.stop()
        self.clean_up_bq_patcher.stop()
        self.region_module_patch.stop()
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
                    # we expect all SECONDARY branch tasks to be skipped except
                    # ensure_release_resource_locks_release_if_acquired
                    "_secondary_import_branch_start",
                    r".*_secondary_import_branch\.(?!ensure_release_resource_locks_release_if_acquired)",
                    # these steps are skipped as mapped tasks (and downstream tasks)
                    # are skipped when the input is an empty list
                    r"raw_data_branching.*_primary_import_branch.biq_query_load.*",
                    r"raw_data_branching.*_primary_import_branch.cleanup_and_storage.*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            for lock_released in session.execute(
                text("select released from direct_ingest_raw_data_resource_lock")
            ):
                assert lock_released[0]

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
                    # we expect all PRIMARY and US_YY SECONDARY branch tasks to be
                    # skipped except ensure_release_resource_locks_release_if_acquired
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

            for lock_released in session.execute(
                text("select released from direct_ingest_raw_data_resource_lock")
            ):
                assert lock_released[0]

    def test_branching_resource_lock_acquisition_fails(self) -> None:
        def _resource_lock_fails(*_args: Dict, **_kwargs: Any) -> None:
            raise ValueError("We failed! Oops!")

        self.mock_cloud_sql_db_hook().get_database_hook.side_effect = (
            _resource_lock_fails
        )
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf={
                    "ingest_instance": "PRIMARY",
                },
                expected_failure_ids=[
                    # intended tasks to fail
                    r".*_primary_import_branch\.acquire_raw_data_resource_locks",
                    # since the task above failed, we had no lock_ids to pull from xcom
                    r".*_primary_import_branch\.release_raw_data_resource_locks",
                    # downstream failures
                    r".*_primary_import_branch\.list_normalized_unprocessed_gcs_file_paths",
                    r".*_primary_import_branch\.get_all_unprocessed_gcs_file_metadata",
                    r".*_primary_import_branch\.get_all_unprocessed_bq_file_metadata",
                    r".*_primary_import_branch\.split_by_pre_import_normalization_type",
                    r".*_primary_import_branch\.coalesce_import_ready_files",
                    r".*_primary_import_branch\.pre_import_normalization\.*",
                    r".*_primary_import_branch\.biq_query_load\.*",
                    r".*_primary_import_branch\.cleanup_and_storage.*",
                    r"raw_data_branching\.branch_end",
                ],
                expected_skipped_ids=[
                    "_secondary_import_branch_start",
                    r".*_secondary_import_branch\.(?!ensure_release_resource_locks_release_if_acquired)",
                ],
            )
            # failed state will only happen when resource releasing failed
            self.assertEqual(DagRunState.FAILED, result.dag_run_state)

            assert not list(
                session.execute(
                    text("select released from direct_ingest_raw_data_resource_lock")
                )
            )

    def test_branching_resource_lock_acquisition_succeeds_but_we_fail_after(
        self,
    ) -> None:

        self.list_normalized_unprocessed_files_mock.side_effect = (
            fake_failing_operator_constructor
        )

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf={
                    "ingest_instance": "PRIMARY",
                },
                expected_failure_ids=[
                    # intended failures
                    r".*_primary_import_branch\.list_normalized_unprocessed_gcs_file_paths",
                    # downstream failures
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
            # failed state will only happen when resource releasing failed
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            for lock_released in session.execute(
                text("select released from direct_ingest_raw_data_resource_lock")
            ):
                assert lock_released[0]


class RawDataImportOperationsRegistrationIntegrationTest(AirflowIntegrationTest):
    """integration tests for step 2: operations registration"""

    metas = [OperationsBase]
    conn_id = "operations_postgres_conn_id"

    def setUp(self) -> None:
        super().setUp()
        self.dag_id = get_raw_data_import_dag_id(_PROJECT_ID)
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
        self.region_module_patch = patch(
            "recidiviz.airflow.dags.raw_data.utils.direct_ingest_regions_module",
            fake_regions,
        )
        self.region_module_patch.start()
        self.cloud_sql_db_hook_patcher = patch(
            "recidiviz.airflow.dags.operators.cloud_sql_query_operator.CloudSQLDatabaseHook"
        )
        self.mock_cloud_sql_db_hook = self.cloud_sql_db_hook_patcher.start()
        self.mock_cloud_sql_db_hook().get_database_hook.return_value = PostgresHook(
            self.conn_id
        )
        self.gcs_operator_patcher = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.DirectIngestListNormalizedUnprocessedFilesOperator",
            side_effect=fake_operator_with_return_value([]),
        )
        self.list_normalized_unprocessed_files_mock = self.gcs_operator_patcher.start()

    def tearDown(self) -> None:
        self.raw_data_enabled_pairs.stop()
        self.raw_data_enabled_pairs_two.stop()
        self.raw_data_enabled_states.stop()
        self.cloud_sql_db_hook_patcher.stop()
        self.gcs_operator_patcher.stop()
        self.region_module_patch.stop()
        super().tearDown()

    def _create_dag(self) -> DAG:
        # pylint: disable=import-outside-toplevel
        from recidiviz.airflow.dags.raw_data_import_dag import (
            create_raw_data_import_dag,
        )

        return create_raw_data_import_dag()

    def test_no_chunked_files(self) -> None:
        self.list_normalized_unprocessed_files_mock.side_effect = fake_operator_with_return_value(
            [
                "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagBasicData.csv",
                "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagFileConfigHeaders.csv",
                "testing/unprocessed_2024-01-27T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv",
            ]
        )

        step_2_only_dag = self._create_dag().partial_subset(
            task_ids_or_regex=[
                "raw_data_branching.us_xx_primary_import_branch.list_normalized_unprocessed_gcs_file_paths",
                "raw_data_branching.us_xx_primary_import_branch.get_all_unprocessed_gcs_file_metadata",
                "raw_data_branching.us_xx_primary_import_branch.get_all_unprocessed_bq_file_metadata",
                "raw_data_branching.us_xx_primary_import_branch.split_by_pre_import_normalization_type",
            ],
            include_upstream=False,
            include_downstream=False,
        )

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                step_2_only_dag,
                session=session,
                expected_failure_ids=[],  # none!
                expected_skipped_ids=[],  # none!
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # --- validate xcom output

            import_ready_files_jsonb = self.get_xcom_for_task_id(
                "raw_data_branching.us_xx_primary_import_branch.split_by_pre_import_normalization_type",
                session=session,
                key=IMPORT_READY_FILES,
            )
            requires_norm_bq_metadata_jsonb = self.get_xcom_for_task_id(
                "raw_data_branching.us_xx_primary_import_branch.split_by_pre_import_normalization_type",
                session=session,
                key=REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_METADATA,
            )
            requires_norm_paths_jsonb = self.get_xcom_for_task_id(
                "raw_data_branching.us_xx_primary_import_branch.split_by_pre_import_normalization_type",
                session=session,
                key=REQUIRES_PRE_IMPORT_NORMALIZATION_FILES,
            )

            requires_norm_paths = [
                GcsfsFilePath.from_absolute_path(path)
                for path in json.loads(assert_type(requires_norm_paths_jsonb, bytes))
            ]

            assert requires_norm_paths == [
                GcsfsFilePath.from_absolute_path(
                    "testing/unprocessed_2024-01-27T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv",
                )
            ]

            requires_norm_bq_metadata = [
                RawBigQueryFileMetadata.deserialize(requires_norm_bq_metadata_str)
                for requires_norm_bq_metadata_str in json.loads(
                    assert_type(requires_norm_bq_metadata_jsonb, bytes)
                )
            ]

            assert requires_norm_bq_metadata == [
                RawBigQueryFileMetadata(
                    gcs_files=[
                        RawGCSFileMetadata(
                            gcs_file_id=3,
                            file_id=None,
                            path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-27T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv",
                            ),
                        )
                    ],
                    file_id=3,
                    file_tag="tagCustomLineTerminatorNonUTF8",
                    update_datetime=datetime.datetime.fromisoformat(
                        "2024-01-27T16:35:33:617135Z"
                    ),
                )
            ]

            import_ready_files = [
                ImportReadyFile.deserialize(import_ready_file_str)
                for import_ready_file_str in json.loads(
                    assert_type(import_ready_files_jsonb, bytes)
                )
            ]

            assert import_ready_files == [
                ImportReadyFile(
                    file_id=1,
                    file_tag="tagBasicData",
                    update_datetime=datetime.datetime.fromisoformat(
                        "2024-01-25T16:35:33:617135Z"
                    ),
                    pre_import_normalized_file_paths=None,
                    original_file_paths=[
                        GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagBasicData.csv",
                        )
                    ],
                ),
                ImportReadyFile(
                    file_id=2,
                    file_tag="tagFileConfigHeaders",
                    update_datetime=datetime.datetime.fromisoformat(
                        "2024-01-26T16:35:33:617135Z"
                    ),
                    pre_import_normalized_file_paths=None,
                    original_file_paths=[
                        GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagFileConfigHeaders.csv",
                        )
                    ],
                ),
            ]

            # --- validate persisted rows in operations db

            bq_metadata = session.execute(
                text(
                    "select file_id, update_datetime, file_processed_time from direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert list(bq_metadata) == [
                (
                    1,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    None,
                ),
                (
                    2,
                    datetime.datetime.fromisoformat("2024-01-26T16:35:33:617135Z"),
                    None,
                ),
                (
                    3,
                    datetime.datetime.fromisoformat("2024-01-27T16:35:33:617135Z"),
                    None,
                ),
            ]

            gcs_metadata = session.execute(
                text(
                    "select file_id, gcs_file_id, update_datetime from direct_ingest_raw_gcs_file_metadata"
                )
            )

            assert list(gcs_metadata) == [
                (
                    1,
                    1,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                ),
                (
                    2,
                    2,
                    datetime.datetime.fromisoformat("2024-01-26T16:35:33:617135Z"),
                ),
                (
                    3,
                    3,
                    datetime.datetime.fromisoformat("2024-01-27T16:35:33:617135Z"),
                ),
            ]

    def test_chunked_files(self) -> None:
        # step 2 input
        self.list_normalized_unprocessed_files_mock.side_effect = fake_operator_with_return_value(
            [
                # complete set of chunks
                "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-1.csv",
                "testing/unprocessed_2024-01-25T17:35:33:617135_raw_tagChunkedFile-2.csv",
                "testing/unprocessed_2024-01-25T18:35:33:617135_raw_tagChunkedFile-3.csv",
                # missing 1 chunk
                "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFile-1.csv",
                "testing/unprocessed_2024-01-26T17:35:33:617135_raw_tagChunkedFile-3.csv",
                # has 1 too many chunks
                "testing/unprocessed_2024-01-27T16:35:33:617135_raw_tagChunkedFile-1.csv",
                "testing/unprocessed_2024-01-27T17:35:33:617135_raw_tagChunkedFile-2.csv",
                "testing/unprocessed_2024-01-27T18:35:33:617135_raw_tagChunkedFile-3.csv",
                "testing/unprocessed_2024-01-27T19:35:33:617135_raw_tagChunkedFile-3.csv",
            ]
        )

        step_2_only_dag = self._create_dag().partial_subset(
            task_ids_or_regex=[
                "raw_data_branching.us_xx_primary_import_branch.list_normalized_unprocessed_gcs_file_paths",
                "raw_data_branching.us_xx_primary_import_branch.get_all_unprocessed_gcs_file_metadata",
                "raw_data_branching.us_xx_primary_import_branch.get_all_unprocessed_bq_file_metadata",
                "raw_data_branching.us_xx_primary_import_branch.split_by_pre_import_normalization_type",
            ],
            include_upstream=False,
            include_downstream=False,
        )

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                step_2_only_dag,
                session=session,
                expected_failure_ids=[],  # none!
                expected_skipped_ids=[],  # none!
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            import_ready_files_jsonb = self.get_xcom_for_task_id(
                "raw_data_branching.us_xx_primary_import_branch.split_by_pre_import_normalization_type",
                session=session,
                key=IMPORT_READY_FILES,
            )
            requires_norm_bq_metadata_jsonb = self.get_xcom_for_task_id(
                "raw_data_branching.us_xx_primary_import_branch.split_by_pre_import_normalization_type",
                session=session,
                key=REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_METADATA,
            )
            requires_norm_paths_jsonb = self.get_xcom_for_task_id(
                "raw_data_branching.us_xx_primary_import_branch.split_by_pre_import_normalization_type",
                session=session,
                key=REQUIRES_PRE_IMPORT_NORMALIZATION_FILES,
            )

            requires_norm_paths = [
                GcsfsFilePath.from_absolute_path(path)
                for path in json.loads(assert_type(requires_norm_paths_jsonb, bytes))
            ]

            assert requires_norm_paths == [
                GcsfsFilePath.from_absolute_path(
                    "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-1.csv",
                ),
                GcsfsFilePath.from_absolute_path(
                    "testing/unprocessed_2024-01-25T17:35:33:617135_raw_tagChunkedFile-2.csv",
                ),
                GcsfsFilePath.from_absolute_path(
                    "testing/unprocessed_2024-01-25T18:35:33:617135_raw_tagChunkedFile-3.csv",
                ),
            ]

            requires_norm_bq_metadata = [
                RawBigQueryFileMetadata.deserialize(requires_norm_bq_metadata_str)
                for requires_norm_bq_metadata_str in json.loads(
                    assert_type(requires_norm_bq_metadata_jsonb, bytes)
                )
            ]

            assert requires_norm_bq_metadata == [
                RawBigQueryFileMetadata(
                    gcs_files=[
                        RawGCSFileMetadata(
                            gcs_file_id=1,
                            file_id=None,
                            path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-1.csv",
                            ),
                        ),
                        RawGCSFileMetadata(
                            gcs_file_id=2,
                            file_id=None,
                            path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-25T17:35:33:617135_raw_tagChunkedFile-2.csv",
                            ),
                        ),
                        RawGCSFileMetadata(
                            gcs_file_id=3,
                            file_id=None,
                            path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-25T18:35:33:617135_raw_tagChunkedFile-3.csv",
                            ),
                        ),
                    ],
                    file_id=1,
                    file_tag="tagChunkedFile",
                    update_datetime=datetime.datetime.fromisoformat(
                        "2024-01-25T18:35:33:617135Z"
                    ),
                )
            ]

            assert not json.loads(assert_type(import_ready_files_jsonb, bytes))

            # --- validate persisted rows in operations db

            bq_metadata = session.execute(
                text(
                    "select file_id, update_datetime, file_processed_time from direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    1,
                    datetime.datetime.fromisoformat("2024-01-25T18:35:33:617135Z"),
                    None,
                ),
            }

            gcs_metadata = session.execute(
                text(
                    "select file_id, gcs_file_id, update_datetime from direct_ingest_raw_gcs_file_metadata"
                )
            )

            assert set(gcs_metadata) == {
                (
                    1,
                    1,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                ),
                (
                    1,
                    2,
                    datetime.datetime.fromisoformat("2024-01-25T17:35:33:617135Z"),
                ),
                (
                    1,
                    3,
                    datetime.datetime.fromisoformat("2024-01-25T18:35:33:617135Z"),
                ),
                (
                    None,
                    4,
                    datetime.datetime.fromisoformat("2024-01-26T16:35:33:617135Z"),
                ),
                (
                    None,
                    5,
                    datetime.datetime.fromisoformat("2024-01-26T17:35:33:617135Z"),
                ),
                (
                    None,
                    6,
                    datetime.datetime.fromisoformat("2024-01-27T16:35:33:617135Z"),
                ),
                (
                    None,
                    7,
                    datetime.datetime.fromisoformat("2024-01-27T17:35:33:617135Z"),
                ),
                (
                    None,
                    8,
                    datetime.datetime.fromisoformat("2024-01-27T18:35:33:617135Z"),
                ),
                (
                    None,
                    9,
                    datetime.datetime.fromisoformat("2024-01-27T19:35:33:617135Z"),
                ),
            }


class RawDataImportDagPreImportNormalizationIntegrationTest(AirflowIntegrationTest):
    """integration tests for step 3: pre-import normalization"""

    def setUp(self) -> None:
        super().setUp()

        # env mocks ---

        self.dag_id = get_raw_data_import_dag_id(_PROJECT_ID)
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

        # operator mocks ---

        self.kpo_operator_patcher = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.RecidivizKubernetesPodOperator.partial",
            side_effect=self._fake_k8s_operator_wrapper,
        )
        self.kpo_operator_mock = self.kpo_operator_patcher.start()

        self.file_chunking_args_patcher = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.generate_file_chunking_pod_arguments.function",
        )
        original, _ = self.file_chunking_args_patcher.get_original()
        self.file_chunking_args_patcher.start().side_effect = self._file_chunking(
            original
        )

        self.verify_file_chunks_patcher = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.regroup_and_verify_file_chunks.function",
        )
        original, _ = self.verify_file_chunks_patcher.get_original()
        self.verify_file_chunks_patcher.start().side_effect = self._regroup(original)

        # task interaction mocks ---

        self.fake_gcs_patch = patch(
            "recidiviz.airflow.dags.raw_data.gcs_file_processing_tasks.GcsfsFactory.build"
        )
        self.fake_gcs_mock = self.fake_gcs_patch.start()
        self.fake_gcs_mock().get_file_size.return_value = 10000
        self.fake_gcs_mock().get_crc32c.return_value = "E6KYdQ=="

        # instance vars for mocked return vals ---

        self.input_requires_normalization: List[str] = []
        self.input_bq_metadata: List[str] = []
        self.chunking_return_value: List[str] = []
        self.normalization_return_value: List[str] = []

    def tearDown(self) -> None:
        self.environment_patcher.stop()
        self.raw_data_enabled_pairs.stop()
        self.raw_data_enabled_pairs_two.stop()
        self.raw_data_enabled_states.stop()
        self.kpo_operator_patcher.stop()
        self.file_chunking_args_patcher.stop()
        self.verify_file_chunks_patcher.stop()
        self.fake_gcs_patch.stop()
        super().tearDown()

    def _fake_k8s_operator_wrapper(self, *args: Any, **kwargs: Any) -> OperatorPartial:
        """because the raw data import dag calls Operator.partial, we need to mock out
        the return value of that function in order to properly mock out the k8s operator
        """

        return_value: List[str] = []

        if kwargs["task_id"] == "raw_data_file_chunking":
            return_value = self.chunking_return_value
        elif kwargs["task_id"] == "raw_data_chunk_normalization":
            return_value = self.normalization_return_value

        return partial(
            fake_k8s_operator_with_return_value(
                return_value,
                is_mapped=True,
            ),
            *args,
            **kwargs
        )

    def _file_chunking(self, func: Callable) -> Callable:
        # small wrapper to pass self.input_requires_normalization as the
        # serialized_requires_pre_import_normalization_file_paths parameter of
        # generate_file_chunking_pod_arguments
        def _inner(region_code: str, _irrelevant: Any, **kwargs: Any) -> Any:
            return func(
                region_code, self.input_requires_normalization, kwargs["num_batches"]
            )

        return _inner

    def _regroup(self, func: Callable) -> Callable:
        # small wrapper to pass self.input_bq_metadata to as the
        # serialized_requires_pre_import_normalization_files_bq_metadata parameter of
        # regroup_and_verify_file_chunks
        def _inner(
            normalized_chunks_result: List[str], _irrelevant: Any, **_kwargs: Any
        ) -> Any:
            return func(normalized_chunks_result, self.input_bq_metadata)

        return _inner

    def _create_dag(self) -> DAG:
        # pylint: disable=import-outside-toplevel
        from recidiviz.airflow.dags.raw_data_import_dag import (
            create_raw_data_import_dag,
        )

        return create_raw_data_import_dag()

    def test_empty(self) -> None:

        self.input_requires_normalization = []
        self.chunking_return_value = []
        self.normalization_return_value = []

        self.input_bq_metadata = []

        step_3_only_dag = self._create_dag().partial_subset(
            task_ids_or_regex=[
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.generate_file_chunking_pod_arguments",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_file_chunking",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_file_chunking_errors",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.generate_chunk_processing_pod_arguments",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_chunk_normalization",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.regroup_and_verify_file_chunks",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_chunk_normalization_errors",
            ],
            include_upstream=False,
            include_downstream=False,
        )

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                step_3_only_dag,
                session=session,
                expected_failure_ids=[],
                expected_skipped_ids=[
                    # we expect this since airflow will skipped mapped tasks w/ an empty input
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_file_chunking",
                    # these all have ALL_SUCCESS so ^^ being skipped will cause these to be skipped
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_file_chunking_errors",
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.generate_chunk_processing_pod_arguments",
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_chunk_normalization",
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.regroup_and_verify_file_chunks",
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_chunk_normalization_errors",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            regrouped_and_verified_jsonb = self.get_xcom_for_task_id(
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.regroup_and_verify_file_chunks",
                session=session,
                key="return_value",
            )
            assert regrouped_and_verified_jsonb is None

    def test_all_paths_success(self) -> None:

        self.input_requires_normalization = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
        ]

        self.chunking_return_value = [
            BatchedTaskInstanceOutput(results=[result], errors=[]).serialize()
            for result in [
                RequiresPreImportNormalizationFile(
                    path=GcsfsFilePath.from_absolute_path(
                        "testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
                    ),
                    pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
                    chunk_boundaries=[
                        CsvChunkBoundary(
                            chunk_num=0, start_inclusive=0, end_exclusive=2
                        ),
                        CsvChunkBoundary(
                            chunk_num=1, start_inclusive=2, end_exclusive=4
                        ),
                        CsvChunkBoundary(
                            chunk_num=2, start_inclusive=4, end_exclusive=6
                        ),
                    ],
                    headers=["aaaaaa"],
                ),
                RequiresPreImportNormalizationFile(
                    path=GcsfsFilePath.from_absolute_path(
                        "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                    ),
                    pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
                    chunk_boundaries=[
                        CsvChunkBoundary(
                            chunk_num=0, start_inclusive=0, end_exclusive=2
                        ),
                        CsvChunkBoundary(
                            chunk_num=1, start_inclusive=2, end_exclusive=4
                        ),
                        CsvChunkBoundary(
                            chunk_num=2, start_inclusive=4, end_exclusive=6
                        ),
                    ],
                    headers=["aaaaaa"],
                ),
                RequiresPreImportNormalizationFile(
                    path=GcsfsFilePath.from_absolute_path(
                        "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
                    ),
                    pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
                    chunk_boundaries=[
                        CsvChunkBoundary(
                            chunk_num=0, start_inclusive=0, end_exclusive=2
                        ),
                        CsvChunkBoundary(
                            chunk_num=1, start_inclusive=2, end_exclusive=4
                        ),
                        CsvChunkBoundary(
                            chunk_num=2, start_inclusive=4, end_exclusive=6
                        ),
                    ],
                    headers=["aaaaaa"],
                ),
            ]
        ]

        self.normalization_return_value = [
            BatchedTaskInstanceOutput(results=result, errors=[]).serialize()
            for result in [
                [
                    PreImportNormalizedCsvChunkResult(
                        input_file_path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
                        ),
                        output_file_path=GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                        ),
                        chunk_boundary=CsvChunkBoundary(
                            chunk_num=0, start_inclusive=0, end_exclusive=2
                        ),
                        crc32c=0,
                    ),
                    PreImportNormalizedCsvChunkResult(
                        input_file_path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
                        ),
                        output_file_path=GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                        ),
                        chunk_boundary=CsvChunkBoundary(
                            chunk_num=2, start_inclusive=4, end_exclusive=6
                        ),
                        crc32c=2,
                    ),
                ],
                [
                    PreImportNormalizedCsvChunkResult(
                        input_file_path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
                        ),
                        output_file_path=GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey-2.csv",
                        ),
                        chunk_boundary=CsvChunkBoundary(
                            chunk_num=1, start_inclusive=2, end_exclusive=4
                        ),
                        crc32c=1,
                    ),
                    PreImportNormalizedCsvChunkResult(
                        input_file_path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                        ),
                        output_file_path=GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                        ),
                        chunk_boundary=CsvChunkBoundary(
                            chunk_num=0, start_inclusive=0, end_exclusive=2
                        ),
                        crc32c=0,
                    ),
                    PreImportNormalizedCsvChunkResult(
                        input_file_path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                        ),
                        output_file_path=GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                        ),
                        chunk_boundary=CsvChunkBoundary(
                            chunk_num=1, start_inclusive=4, end_exclusive=6
                        ),
                        crc32c=1,
                    ),
                ],
                [
                    PreImportNormalizedCsvChunkResult(
                        input_file_path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                        ),
                        output_file_path=GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-2.csv",
                        ),
                        chunk_boundary=CsvChunkBoundary(
                            chunk_num=2, start_inclusive=2, end_exclusive=4
                        ),
                        crc32c=2,
                    ),
                    PreImportNormalizedCsvChunkResult(
                        input_file_path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
                        ),
                        output_file_path=GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                        ),
                        chunk_boundary=CsvChunkBoundary(
                            chunk_num=0, start_inclusive=0, end_exclusive=2
                        ),
                        crc32c=0,
                    ),
                    PreImportNormalizedCsvChunkResult(
                        input_file_path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
                        ),
                        output_file_path=GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                        ),
                        chunk_boundary=CsvChunkBoundary(
                            chunk_num=1, start_inclusive=4, end_exclusive=6
                        ),
                        crc32c=1,
                    ),
                    PreImportNormalizedCsvChunkResult(
                        input_file_path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
                        ),
                        output_file_path=GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-2.csv",
                        ),
                        chunk_boundary=CsvChunkBoundary(
                            chunk_num=2, start_inclusive=2, end_exclusive=4
                        ),
                        crc32c=2,
                    ),
                ],
                [],
                [],
                [],
                [],
            ]
        ]

        self.input_bq_metadata = [
            RawBigQueryFileMetadata(
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
                        ),
                    )
                ],
                file_tag="singlePrimaryKey",
                update_datetime=datetime.datetime.fromisoformat(
                    "2024-01-25T16:35:33:617135Z"
                ),
                file_id=1,
            ).serialize(),
            RawBigQueryFileMetadata(
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                        ),
                    )
                ],
                file_tag="singlePrimaryKey",
                update_datetime=datetime.datetime.fromisoformat(
                    "2024-01-26T16:35:33:617135Z"
                ),
                file_id=2,
            ).serialize(),
            RawBigQueryFileMetadata(
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
                        ),
                    )
                ],
                file_tag="singlePrimaryKey",
                update_datetime=datetime.datetime.fromisoformat(
                    "2024-01-27T16:35:33:617135Z"
                ),
                file_id=3,
            ).serialize(),
        ]

        step_3_only_dag = self._create_dag().partial_subset(
            task_ids_or_regex=[
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.generate_file_chunking_pod_arguments",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_file_chunking",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_file_chunking_errors",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.generate_chunk_processing_pod_arguments",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_chunk_normalization",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.regroup_and_verify_file_chunks",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_chunk_normalization_errors",
            ],
            include_upstream=False,
            include_downstream=False,
        )

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                step_3_only_dag,
                session=session,
                expected_failure_ids=[],  # none!
                expected_skipped_ids=[],  # none!
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            regrouped_and_verified_jsonb = self.get_xcom_for_task_id(
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.regroup_and_verify_file_chunks",
                session=session,
                key="return_value",
            )

            regrouped_and_verified = BatchedTaskInstanceOutput.deserialize(
                json.loads(assert_type(regrouped_and_verified_jsonb, bytes)),
                result_cls=ImportReadyFile,
                error_cls=RawFileProcessingError,
            )

            # validate that it matches what we expect
            assert not regrouped_and_verified.errors
            expected_results = [
                ImportReadyFile(
                    file_id=1,
                    file_tag="singlePrimaryKey",
                    update_datetime=datetime.datetime.fromisoformat(
                        "2024-01-25T16:35:33:617135Z"
                    ),
                    pre_import_normalized_file_paths=[
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                        ),
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                        ),
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey-2.csv",
                        ),
                    ],
                    original_file_paths=[
                        GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
                        )
                    ],
                ),
                ImportReadyFile(
                    file_id=2,
                    file_tag="singlePrimaryKey",
                    update_datetime=datetime.datetime.fromisoformat(
                        "2024-01-26T16:35:33:617135Z"
                    ),
                    pre_import_normalized_file_paths=[
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                        ),
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                        ),
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-2.csv",
                        ),
                    ],
                    original_file_paths=[
                        GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                        )
                    ],
                ),
                ImportReadyFile(
                    file_id=3,
                    file_tag="singlePrimaryKey",
                    update_datetime=datetime.datetime.fromisoformat(
                        "2024-01-27T16:35:33:617135Z"
                    ),
                    pre_import_normalized_file_paths=[
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                        ),
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                        ),
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-2.csv",
                        ),
                    ],
                    original_file_paths=[
                        GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
                        )
                    ],
                ),
            ]
        assert _comparable(regrouped_and_verified.results) == _comparable(
            expected_results
        )

    def test_failure_during_normalization(self) -> None:

        self.input_requires_normalization = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
        ]

        # chunking returns fine
        self.chunking_return_value = [
            BatchedTaskInstanceOutput(results=[result], errors=[]).serialize()
            for result in [
                RequiresPreImportNormalizationFile(
                    path=GcsfsFilePath.from_absolute_path(
                        "testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
                    ),
                    pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
                    chunk_boundaries=[
                        CsvChunkBoundary(
                            chunk_num=0, start_inclusive=0, end_exclusive=2
                        ),
                        CsvChunkBoundary(
                            chunk_num=1, start_inclusive=2, end_exclusive=4
                        ),
                        CsvChunkBoundary(
                            chunk_num=2, start_inclusive=4, end_exclusive=6
                        ),
                    ],
                    headers=["aaaaaa"],
                ),
                RequiresPreImportNormalizationFile(
                    path=GcsfsFilePath.from_absolute_path(
                        "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                    ),
                    pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
                    chunk_boundaries=[
                        CsvChunkBoundary(
                            chunk_num=0, start_inclusive=0, end_exclusive=2
                        ),
                        CsvChunkBoundary(
                            chunk_num=1, start_inclusive=2, end_exclusive=4
                        ),
                        CsvChunkBoundary(
                            chunk_num=2, start_inclusive=4, end_exclusive=6
                        ),
                    ],
                    headers=["aaaaaa"],
                ),
                RequiresPreImportNormalizationFile(
                    path=GcsfsFilePath.from_absolute_path(
                        "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
                    ),
                    pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
                    chunk_boundaries=[
                        CsvChunkBoundary(
                            chunk_num=0, start_inclusive=0, end_exclusive=2
                        ),
                        CsvChunkBoundary(
                            chunk_num=1, start_inclusive=2, end_exclusive=4
                        ),
                        CsvChunkBoundary(
                            chunk_num=2, start_inclusive=4, end_exclusive=6
                        ),
                    ],
                    headers=["aaaaaa"],
                ),
            ]
        ]

        self.normalization_return_value = [
            BatchedTaskInstanceOutput(results=results, errors=errors).serialize()
            for results, errors in zip(
                [
                    [
                        PreImportNormalizedCsvChunkResult(
                            input_file_path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            output_file_path=GcsfsFilePath.from_absolute_path(
                                "temp/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                            ),
                            chunk_boundary=CsvChunkBoundary(
                                chunk_num=0, start_inclusive=0, end_exclusive=2
                            ),
                            crc32c=0,
                        ),
                        # this path fails!
                        # PreImportNormalizedCsvChunkResult(
                        #     input_file_path=GcsfsFilePath.from_absolute_path(
                        #         "testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
                        #     ),
                        #     output_file_path=GcsfsFilePath.from_absolute_path(
                        #         "temp/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                        #     ),
                        #     chunk_boundary=CsvChunkBoundary(
                        #         chunk_num=2, start_inclusive=4, end_exclusive=6
                        #     ),
                        #     crc32c=2,
                        # ),
                    ],
                    [
                        PreImportNormalizedCsvChunkResult(
                            input_file_path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            output_file_path=GcsfsFilePath.from_absolute_path(
                                "temp/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey-2.csv",
                            ),
                            chunk_boundary=CsvChunkBoundary(
                                chunk_num=1, start_inclusive=2, end_exclusive=4
                            ),
                            crc32c=1,
                        ),
                        PreImportNormalizedCsvChunkResult(
                            input_file_path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            output_file_path=GcsfsFilePath.from_absolute_path(
                                "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                            ),
                            chunk_boundary=CsvChunkBoundary(
                                chunk_num=0, start_inclusive=0, end_exclusive=2
                            ),
                            crc32c=0,
                        ),
                        PreImportNormalizedCsvChunkResult(
                            input_file_path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            output_file_path=GcsfsFilePath.from_absolute_path(
                                "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                            ),
                            chunk_boundary=CsvChunkBoundary(
                                chunk_num=1, start_inclusive=4, end_exclusive=6
                            ),
                            crc32c=1,
                        ),
                    ],
                    [
                        PreImportNormalizedCsvChunkResult(
                            input_file_path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            output_file_path=GcsfsFilePath.from_absolute_path(
                                "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-2.csv",
                            ),
                            chunk_boundary=CsvChunkBoundary(
                                chunk_num=2, start_inclusive=2, end_exclusive=4
                            ),
                            crc32c=2,
                        ),
                        PreImportNormalizedCsvChunkResult(
                            input_file_path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            output_file_path=GcsfsFilePath.from_absolute_path(
                                "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                            ),
                            chunk_boundary=CsvChunkBoundary(
                                chunk_num=0, start_inclusive=0, end_exclusive=2
                            ),
                            crc32c=0,
                        ),
                        PreImportNormalizedCsvChunkResult(
                            input_file_path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            output_file_path=GcsfsFilePath.from_absolute_path(
                                "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                            ),
                            chunk_boundary=CsvChunkBoundary(
                                chunk_num=1, start_inclusive=4, end_exclusive=6
                            ),
                            crc32c=1,
                        ),
                        PreImportNormalizedCsvChunkResult(
                            input_file_path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            output_file_path=GcsfsFilePath.from_absolute_path(
                                "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-2.csv",
                            ),
                            chunk_boundary=CsvChunkBoundary(
                                chunk_num=2, start_inclusive=2, end_exclusive=4
                            ),
                            crc32c=2,
                        ),
                    ],
                    [],
                    [],
                    [],
                ],
                [
                    [
                        RawFileProcessingError(
                            original_file_path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            temporary_file_paths=[
                                GcsfsFilePath.from_absolute_path(
                                    "temp/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                                )
                            ],
                            error_msg="Oops!",
                        )
                    ],
                    [],
                    [],
                    [],
                    [],
                    [],
                ],
            )
        ]

        self.input_bq_metadata = [
            RawBigQueryFileMetadata(
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
                        ),
                    )
                ],
                file_tag="singlePrimaryKey",
                update_datetime=datetime.datetime.fromisoformat(
                    "2024-01-25T16:35:33:617135Z"
                ),
                file_id=1,
            ).serialize(),
            RawBigQueryFileMetadata(
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                        ),
                    )
                ],
                file_tag="singlePrimaryKey",
                update_datetime=datetime.datetime.fromisoformat(
                    "2024-01-26T16:35:33:617135Z"
                ),
                file_id=2,
            ).serialize(),
            RawBigQueryFileMetadata(
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
                        ),
                    )
                ],
                file_tag="singlePrimaryKey",
                update_datetime=datetime.datetime.fromisoformat(
                    "2024-01-27T16:35:33:617135Z"
                ),
                file_id=3,
            ).serialize(),
        ]

        step_3_only_dag = self._create_dag().partial_subset(
            task_ids_or_regex=[
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.generate_file_chunking_pod_arguments",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_file_chunking",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_file_chunking_errors",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.generate_chunk_processing_pod_arguments",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_chunk_normalization",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.regroup_and_verify_file_chunks",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_chunk_normalization_errors",
            ],
            include_upstream=False,
            include_downstream=False,
        )

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                step_3_only_dag,
                session=session,
                expected_failure_ids=[
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_chunk_normalization_errors"
                ],
                expected_skipped_ids=[],  # none!
            )
            self.assertEqual(DagRunState.FAILED, result.dag_run_state)

            regrouped_and_verified_jsonb = self.get_xcom_for_task_id(
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.regroup_and_verify_file_chunks",
                session=session,
                key="return_value",
            )

            regrouped_and_verified = BatchedTaskInstanceOutput.deserialize(
                json.loads(assert_type(regrouped_and_verified_jsonb, bytes)),
                result_cls=ImportReadyFile,
                error_cls=RawFileProcessingError,
            )

            # validate that it matches what we expect
            assert regrouped_and_verified.errors == [
                RawFileProcessingError(
                    original_file_path=GcsfsFilePath.from_absolute_path(
                        "testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
                    ),
                    temporary_file_paths=[
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                        )
                    ],
                    error_msg="Oops!",
                ),
                RawFileProcessingError(
                    error_msg="Chunk [0] of [testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv] skipped due to error encountered with a different chunk with the same input path",
                    original_file_path=GcsfsFilePath(
                        bucket_name="testing",
                        blob_name="unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
                    ),
                    temporary_file_paths=[
                        GcsfsFilePath(
                            bucket_name="temp",
                            blob_name="unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                        )
                    ],
                ),
                RawFileProcessingError(
                    error_msg="Chunk [1] of [testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv] skipped due to error encountered with a different chunk with the same input path",
                    original_file_path=GcsfsFilePath(
                        bucket_name="testing",
                        blob_name="unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
                    ),
                    temporary_file_paths=[
                        GcsfsFilePath(
                            bucket_name="temp",
                            blob_name="unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey-2.csv",
                        )
                    ],
                ),
            ]

            expected_results = [
                ImportReadyFile(
                    file_id=2,
                    file_tag="singlePrimaryKey",
                    update_datetime=datetime.datetime.fromisoformat(
                        "2024-01-26T16:35:33:617135Z"
                    ),
                    pre_import_normalized_file_paths=[
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                        ),
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                        ),
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-2.csv",
                        ),
                    ],
                    original_file_paths=[
                        GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                        )
                    ],
                ),
                ImportReadyFile(
                    file_id=3,
                    file_tag="singlePrimaryKey",
                    update_datetime=datetime.datetime.fromisoformat(
                        "2024-01-27T16:35:33:617135Z"
                    ),
                    pre_import_normalized_file_paths=[
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                        ),
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                        ),
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-2.csv",
                        ),
                    ],
                    original_file_paths=[
                        GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
                        )
                    ],
                ),
            ]
        assert _comparable(regrouped_and_verified.results) == _comparable(
            expected_results
        )

    def test_failure_during_chunking(self) -> None:

        self.input_requires_normalization = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
        ]

        # chunking returns fine
        self.chunking_return_value = [
            BatchedTaskInstanceOutput(results=results, errors=errors).serialize()
            for results, errors in zip(
                [
                    [
                        RequiresPreImportNormalizationFile(
                            path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
                            chunk_boundaries=[
                                CsvChunkBoundary(
                                    chunk_num=0, start_inclusive=0, end_exclusive=2
                                ),
                                CsvChunkBoundary(
                                    chunk_num=1, start_inclusive=2, end_exclusive=4
                                ),
                                CsvChunkBoundary(
                                    chunk_num=2, start_inclusive=4, end_exclusive=6
                                ),
                            ],
                            headers=["aaaaaa"],
                        ),
                    ],
                    [
                        RequiresPreImportNormalizationFile(
                            path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
                            chunk_boundaries=[
                                CsvChunkBoundary(
                                    chunk_num=0, start_inclusive=0, end_exclusive=2
                                ),
                                CsvChunkBoundary(
                                    chunk_num=1, start_inclusive=2, end_exclusive=4
                                ),
                                CsvChunkBoundary(
                                    chunk_num=2, start_inclusive=4, end_exclusive=6
                                ),
                            ],
                            headers=["aaaaaa"],
                        ),
                    ],
                    [],
                ],
                [
                    [],
                    [],
                    [
                        RawFileProcessingError(
                            original_file_path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            temporary_file_paths=None,
                            error_msg="Ooops!",
                        )
                    ],
                ],
            )
        ]

        self.normalization_return_value = [
            BatchedTaskInstanceOutput(results=results, errors=errors).serialize()
            # wants us to annotate errors and i dont really want to pull this out of
            # comprehension
            for results, errors in zip(  # type: ignore
                [
                    [
                        PreImportNormalizedCsvChunkResult(
                            input_file_path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            output_file_path=GcsfsFilePath.from_absolute_path(
                                "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                            ),
                            chunk_boundary=CsvChunkBoundary(
                                chunk_num=0, start_inclusive=0, end_exclusive=2
                            ),
                            crc32c=0,
                        ),
                        PreImportNormalizedCsvChunkResult(
                            input_file_path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            output_file_path=GcsfsFilePath.from_absolute_path(
                                "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                            ),
                            chunk_boundary=CsvChunkBoundary(
                                chunk_num=1, start_inclusive=4, end_exclusive=6
                            ),
                            crc32c=1,
                        ),
                    ],
                    [
                        PreImportNormalizedCsvChunkResult(
                            input_file_path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            output_file_path=GcsfsFilePath.from_absolute_path(
                                "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-2.csv",
                            ),
                            chunk_boundary=CsvChunkBoundary(
                                chunk_num=2, start_inclusive=2, end_exclusive=4
                            ),
                            crc32c=2,
                        ),
                        PreImportNormalizedCsvChunkResult(
                            input_file_path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            output_file_path=GcsfsFilePath.from_absolute_path(
                                "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                            ),
                            chunk_boundary=CsvChunkBoundary(
                                chunk_num=0, start_inclusive=0, end_exclusive=2
                            ),
                            crc32c=0,
                        ),
                        PreImportNormalizedCsvChunkResult(
                            input_file_path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            output_file_path=GcsfsFilePath.from_absolute_path(
                                "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                            ),
                            chunk_boundary=CsvChunkBoundary(
                                chunk_num=1, start_inclusive=4, end_exclusive=6
                            ),
                            crc32c=1,
                        ),
                        PreImportNormalizedCsvChunkResult(
                            input_file_path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            output_file_path=GcsfsFilePath.from_absolute_path(
                                "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-2.csv",
                            ),
                            chunk_boundary=CsvChunkBoundary(
                                chunk_num=2, start_inclusive=2, end_exclusive=4
                            ),
                            crc32c=2,
                        ),
                    ],
                    [],
                    [],
                    [],
                ],
                [
                    [],
                    [],
                    [],
                    [],
                    [],
                    [],
                ],
            )
        ]

        self.input_bq_metadata = [
            RawBigQueryFileMetadata(
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
                        ),
                    )
                ],
                file_tag="singlePrimaryKey",
                update_datetime=datetime.datetime.fromisoformat(
                    "2024-01-25T16:35:33:617135Z"
                ),
                file_id=1,
            ).serialize(),
            RawBigQueryFileMetadata(
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                        ),
                    )
                ],
                file_tag="singlePrimaryKey",
                update_datetime=datetime.datetime.fromisoformat(
                    "2024-01-26T16:35:33:617135Z"
                ),
                file_id=2,
            ).serialize(),
            RawBigQueryFileMetadata(
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
                        ),
                    )
                ],
                file_tag="singlePrimaryKey",
                update_datetime=datetime.datetime.fromisoformat(
                    "2024-01-27T16:35:33:617135Z"
                ),
                file_id=3,
            ).serialize(),
        ]

        step_3_only_dag = self._create_dag().partial_subset(
            task_ids_or_regex=[
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.generate_file_chunking_pod_arguments",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_file_chunking",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_file_chunking_errors",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.generate_chunk_processing_pod_arguments",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_chunk_normalization",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.regroup_and_verify_file_chunks",
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_chunk_normalization_errors",
            ],
            include_upstream=False,
            include_downstream=False,
        )

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                step_3_only_dag,
                session=session,
                expected_failure_ids=[
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_file_chunking_errors"
                ],
                expected_skipped_ids=[],  # none!
            )
            self.assertEqual(DagRunState.FAILED, result.dag_run_state)

            regrouped_and_verified_jsonb = self.get_xcom_for_task_id(
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.regroup_and_verify_file_chunks",
                session=session,
                key="return_value",
            )

            raw_data_file_chunking_jsonb = self.get_xcom_for_task_id(
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_file_chunking",
                session=session,
                key="return_value",
                is_mapped=True,
            )

            assert isinstance(raw_data_file_chunking_jsonb, list)

            chunking_output = MappedBatchedTaskOutput.deserialize(
                [
                    json.loads(raw_data_file_chunking_jsonb_result)
                    for raw_data_file_chunking_jsonb_result in raw_data_file_chunking_jsonb
                ],
                result_cls=RequiresPreImportNormalizationFile,
                error_cls=RawFileProcessingError,
            )

            assert chunking_output.flatten_errors() == [
                RawFileProcessingError(
                    original_file_path=GcsfsFilePath.from_absolute_path(
                        "testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
                    ),
                    temporary_file_paths=None,
                    error_msg="Ooops!",
                )
            ]

            assert isinstance(regrouped_and_verified_jsonb, bytes)

            regrouped_and_verified = BatchedTaskInstanceOutput.deserialize(
                json.loads(regrouped_and_verified_jsonb),
                result_cls=ImportReadyFile,
                error_cls=RawFileProcessingError,
            )

            # validate that it matches what we expect
            assert regrouped_and_verified.errors == []

            expected_results = [
                ImportReadyFile(
                    file_id=2,
                    file_tag="singlePrimaryKey",
                    update_datetime=datetime.datetime.fromisoformat(
                        "2024-01-26T16:35:33:617135Z"
                    ),
                    pre_import_normalized_file_paths=[
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                        ),
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                        ),
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-2.csv",
                        ),
                    ],
                    original_file_paths=[
                        GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                        )
                    ],
                ),
                ImportReadyFile(
                    file_id=3,
                    file_tag="singlePrimaryKey",
                    update_datetime=datetime.datetime.fromisoformat(
                        "2024-01-27T16:35:33:617135Z"
                    ),
                    pre_import_normalized_file_paths=[
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                        ),
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                        ),
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-2.csv",
                        ),
                    ],
                    original_file_paths=[
                        GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
                        )
                    ],
                ),
            ]
        assert _comparable(regrouped_and_verified.results) == _comparable(
            expected_results
        )


# TODO(apache/airflow#41160) write tests here once we can construct a subdag of just
# the tasks for the big query load step
class RawDataImportDagBigQueryLoadIntegrationTest(AirflowIntegrationTest):
    """integration tests for step 4: big query load step"""

    def setUp(self) -> None:
        super().setUp()

        # env mocks ---

        self.dag_id = get_raw_data_import_dag_id(_PROJECT_ID)
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
        self.region_module_patch = patch(
            "recidiviz.airflow.dags.raw_data.utils.direct_ingest_regions_module",
            fake_regions,
        )
        self.region_module_patch.start()
        self.metadata_patcher = patch(
            "recidiviz.utils.metadata.project_id", return_value="recidiviz-fake"
        )
        self.metadata_patcher.start()

        # operator mocks ---

        self.coalesce_patcher = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.coalesce_import_ready_files.function",
        )

        self.coalesce_mock = self.coalesce_patcher.start()

        # task interaction mocks ---

        self.fs = FakeGCSFileSystem()
        self.gcsfs_patcher = patch(
            "recidiviz.airflow.dags.raw_data.bq_load_tasks.GcsfsFactory.build",
            return_value=self.fs,
        )
        self.gcsfs_patcher.start()

        self.bq_patcher = patch(
            "recidiviz.airflow.dags.raw_data.bq_load_tasks.BigQueryClientImpl",
        )
        self.bq_mock = self.bq_patcher.start()

        # instance vars for mocked return vals ---

        self.input_requires_normalization: List[str] = []
        self.input_bq_metadata: List[str] = []
        self.chunking_return_value: List[str] = []
        self.normalization_return_value: List[str] = []

    def _create_dag(self) -> DAG:
        # pylint: disable=import-outside-toplevel
        from recidiviz.airflow.dags.raw_data_import_dag import (
            create_raw_data_import_dag,
        )

        return create_raw_data_import_dag()

    def tearDown(self) -> None:
        self.raw_data_enabled_pairs.stop()
        self.raw_data_enabled_pairs_two.stop()
        self.raw_data_enabled_states.stop()
        self.metadata_patcher.stop()
        self.region_module_patch.stop()
        self.coalesce_patcher.stop()
        self.gcsfs_patcher.stop()
        self.bq_patcher.stop()
        super().tearDown()

    def _create_sub_dag(self) -> DAG:
        return self._create_dag().partial_subset(
            task_ids_or_regex=[
                "raw_data_branching.us_xx_primary_import_branch.coalesce_import_ready_files",
                "raw_data_branching.us_xx_primary_import_branch.biq_query_load.load_and_prep_paths_for_batch",
                "raw_data_branching.us_xx_primary_import_branch.biq_query_load.raise_load_prep_errors",
                "raw_data_branching.us_xx_primary_import_branch.biq_query_load.generate_append_batches",
                "raw_data_branching.us_xx_primary_import_branch.biq_query_load.append_ready_file_batches_from_generate_append_batches",
                "raw_data_branching.us_xx_primary_import_branch.biq_query_load.append_to_raw_data_table_for_batch",
                "raw_data_branching.us_xx_primary_import_branch.biq_query_load.raise_append_errors",
            ],
            include_upstream=False,
            include_downstream=False,
        )

    def _register_files(
        self, files: List[ImportReadyFile]
    ) -> Tuple[List[GcsfsFilePath], List[GcsfsFilePath]]:
        temp, original = [], []
        for file in files:
            if file.pre_import_normalized_file_paths:
                for pre_import_path in file.pre_import_normalized_file_paths:
                    self.fs.test_add_path(pre_import_path, local_path=None)
                    temp.append(pre_import_path)
            for path in file.original_file_paths:
                self.fs.test_add_path(path, local_path=None)
                original.append(path)

        return original, temp

    # TODO(#31955): remove the underscore from this test when we upgrade to the cloud
    # composer version associated with the bug fix for the partial_subset bug
    # TODO(apache/airflow#41160) ^^
    def _test_all_success(self) -> None:
        """test for all files successfully completing"""

        import_ready_files = [
            ImportReadyFile(
                file_id=2,
                file_tag="singlePrimaryKey",
                update_datetime=datetime.datetime.fromisoformat(
                    "2024-01-26T16:35:33:617135Z"
                ),
                pre_import_normalized_file_paths=[
                    GcsfsFilePath.from_absolute_path(
                        "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                    ),
                    GcsfsFilePath.from_absolute_path(
                        "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                    ),
                    GcsfsFilePath.from_absolute_path(
                        "temp/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-2.csv",
                    ),
                ],
                original_file_paths=[
                    GcsfsFilePath.from_absolute_path(
                        "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                    )
                ],
            ),
            ImportReadyFile(
                file_id=3,
                file_tag="singlePrimaryKey",
                update_datetime=datetime.datetime.fromisoformat(
                    "2024-01-27T16:35:33:617135Z"
                ),
                pre_import_normalized_file_paths=[
                    GcsfsFilePath.from_absolute_path(
                        "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                    ),
                    GcsfsFilePath.from_absolute_path(
                        "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                    ),
                    GcsfsFilePath.from_absolute_path(
                        "temp/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey-2.csv",
                    ),
                ],
                original_file_paths=[
                    GcsfsFilePath.from_absolute_path(
                        "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
                    )
                ],
            ),
        ]

        self.coalesce_mock.side_effect = fake_task_function_with_return_value(
            [[file.serialize()] for file in import_ready_files]
        )

        self.bq_mock().load_table_from_cloud_storage_async().output_rows = 100
        self.bq_mock().create_table_from_query_async().total_rows = 90

        original_paths, _ = self._register_files(import_ready_files)
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_sub_dag(),
                session=session,
                expected_failure_ids=[],  # none!
                expected_skipped_ids=[],  # none!
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # validate xcom ---

            load_and_prep_jsonb = self.get_xcom_for_task_id(
                "raw_data_branching.us_xx_primary_import_branch.biq_query_load.load_and_prep_paths_for_batch",
                session=session,
                key="return_value",
                is_mapped=True,
            )

            load_result = MappedBatchedTaskOutput.deserialize(
                [
                    json.loads(load_result_str)
                    for load_result_str in assert_type(load_and_prep_jsonb, list)
                ],
                result_cls=AppendReadyFile,
                error_cls=RawFileLoadAndPrepError,
            )

            assert not load_result.flatten_errors()
            assert sorted(
                load_result.flatten_results(),
                key=lambda x: x.import_ready_file.file_id,
            ) == [
                AppendReadyFile(
                    import_ready_file=import_ready_files[0],
                    append_ready_table_address=BigQueryAddress(
                        dataset_id="us_xx_primary_raw_data_temp_load",
                        table_id="singlePrimaryKey__2__transformed",
                    ),
                    raw_rows_count=100,
                ),
                AppendReadyFile(
                    import_ready_file=import_ready_files[1],
                    append_ready_table_address=BigQueryAddress(
                        dataset_id="us_xx_primary_raw_data_temp_load",
                        table_id="singlePrimaryKey__3__transformed",
                    ),
                    raw_rows_count=100,
                ),
            ]

            append_result_jsonb = self.get_xcom_for_task_id(
                "raw_data_branching.us_xx_primary_import_branch.biq_query_load.append_to_raw_data_table_for_batch",
                session=session,
                key="return_value",
                is_mapped=True,
            )

            append_output = MappedBatchedTaskOutput.deserialize(
                [
                    json.loads(append_result)
                    for append_result in assert_type(append_result_jsonb, list)
                ],
                result_cls=AppendSummary,
                error_cls=RawDataAppendImportError,
            )

            assert not append_output.flatten_errors()
            assert append_output.flatten_results() == [
                AppendSummary(file_id=2, historical_diffs_active=False),
                AppendSummary(file_id=3, historical_diffs_active=False),
            ]

            # validate filesystem

            assert set(self.fs.all_paths) == set(original_paths)
