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
import os
from typing import Any, Callable, Dict, Iterator, List, Tuple
from unittest.mock import ANY, MagicMock, call, create_autospec, patch

import attr
from airflow.models import DAG, DagBag
from airflow.models.baseoperator import partial
from airflow.models.mappedoperator import OperatorPartial
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from airflow.utils.state import DagRunState
from airflow.utils.task_group import TaskGroup
from google.cloud.bigquery import Row
from sqlalchemy import text
from sqlalchemy.orm import Session

from recidiviz.airflow.dags.monitoring.dag_registry import get_raw_data_import_dag_id
from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.raw_data.metadata import (
    CHUNKING_ERRORS,
    IMPORT_READY_FILES,
    RAW_DATA_BRANCHING,
    REQUIRES_PRE_IMPORT_NORMALIZATION_FILES,
    REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_METADATA,
)
from recidiviz.airflow.tests.fixtures import raw_data as raw_data_fixtures
from recidiviz.airflow.tests.raw_data.raw_data_test_utils import (
    FakeRawDataImportDelegateFactory,
)
from recidiviz.airflow.tests.test_utils import DAG_FOLDER, AirflowIntegrationTest
from recidiviz.airflow.tests.utils.dag_helper_functions import (
    fake_failing_operator_constructor,
    fake_operator_from_callable,
    fake_operator_with_return_value,
    fake_task_function_with_return_value,
)
from recidiviz.airflow.tests.utils.kubernetes_helper_functions import (
    fake_k8s_operator_for_entrypoint,
    fake_k8s_operator_with_return_value,
)
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.cloud_storage.types import CsvChunkBoundary
from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.entrypoints.raw_data.divide_raw_file_into_chunks import (
    RawDataFileChunkingEntrypoint,
)
from recidiviz.entrypoints.raw_data.normalize_raw_file_chunks import (
    RawDataChunkNormalizationEntrypoint,
)
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_state,
    gcsfs_direct_ingest_storage_directory_path_for_state,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import (
    AppendReadyFile,
    AppendSummary,
    ImportReadyFile,
    PreImportNormalizationType,
    PreImportNormalizedCsvChunkResult,
    RawBigQueryFileMetadata,
    RawDataAppendImportError,
    RawFileBigQueryLoadConfig,
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

    def test_branch_sorting(self) -> None:
        """Tests that branches are sorted in alphabetical order in topological_sort
        which determines ui visual sorting.
        """
        dag = DagBag(dag_folder=DAG_FOLDER, include_examples=False).dags[self.dag_id]
        branching_topological_sorted_groups = [
            maybe_group.group_id
            for maybe_group in dag.task_group.get_task_group_dict()[
                RAW_DATA_BRANCHING
            ].topological_sort()
            if isinstance(maybe_group, TaskGroup)
        ]

        assert branching_topological_sorted_groups == list(
            sorted(branching_topological_sorted_groups)
        )

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
            task_ids_or_regex=r"us_[a-z][a-z]_(?:primary|secondary)_import_branch\.",
            include_upstream=False,
        )

        for leaf in branch_end_and_one_before.leaves:
            assert "maybe_trigger_dag_rerun" in leaf.task_id
            assert len(leaf.upstream_task_ids) == 3
            assert any(
                "release_raw_data_resource_locks" in tid
                for tid in leaf.upstream_task_ids
            )

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
            [
                "has_files_to_import",
                "raise_operations_registration_errors",
                "get_files_to_import_this_run",
            ],
            [
                "split_by_pre_import_normalization_type",
                "read_and_verify_column_headers",
                "coalesce_results_and_errors",
                "maybe_trigger_dag_rerun",
                "write_import_start",
            ],
            [
                "write_import_completions",
                "load_and_prep_paths_for_batch",
                "read_and_verify_column_headers",
            ],
            [
                "coalesce_results_and_errors",
                "raise_header_verification_errors",
                "split_by_pre_import_normalization_type",
            ],
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
            "push_kpo_mapped_task_output_from_gcs_to_xcom",
            "filter_chunking_results_by_errors",
            [
                "raise_file_chunking_errors",
                "coalesce_results_and_errors",
                "generate_chunk_processing_pod_arguments",
            ],
            "raw_data_chunk_normalization",
            "push_kpo_mapped_task_output_from_gcs_to_xcom",
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
            ["branch_end", "load_and_prep_paths_for_batch"],
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
                    "branch_end",
                    "move_successfully_imported_paths_to_storage",
                    "maybe_trigger_dag_rerun",
                    "write_import_completions",
                ],
                ["write_file_processed_time"],
                ["ensure_release_resource_locks_release_if_acquired"],
                ["branch_end", "release_raw_data_resource_locks"],
                ["maybe_trigger_dag_rerun"],
                ["branch_end"],
            ],
            [
                [
                    "branch_end",
                    "write_import_completions",
                    "maybe_trigger_dag_rerun",
                    "move_successfully_imported_paths_to_storage",
                ],
                ["ensure_release_resource_locks_release_if_acquired"],
                ["branch_end", "release_raw_data_resource_locks"],
                ["maybe_trigger_dag_rerun"],
                ["branch_end"],
            ],
        ]

        for root in step_5_root.roots:
            assert "coalesce_results_and_errors" in root.task_id
            for step_5_path in ordered_step_5_task_ids_paths:
                curr_task = root
                for step_5_task_ids in step_5_path:
                    next_task = None
                    assert len(curr_task.downstream_task_ids) == len(step_5_task_ids)
                    for downstream_task in curr_task.downstream_list:
                        assert any(
                            task_id in downstream_task.task_id
                            for task_id in step_5_task_ids
                        )
                        if step_5_task_ids[-1] in downstream_task.task_id:
                            next_task = downstream_task
                    curr_task = next_task


class RawDataImportOperationsRegistrationIntegrationTest(AirflowIntegrationTest):
    """integration tests for step 2: operations registration"""

    metas = [OperationsBase]
    conn_id = "operations_postgres_conn_id"

    operations_registrations_tasks_ids = [
        "raw_data_branching.us_xx_primary_import_branch.list_normalized_unprocessed_gcs_file_paths",
        "raw_data_branching.us_xx_primary_import_branch.get_all_unprocessed_gcs_file_metadata",
        "raw_data_branching.us_xx_primary_import_branch.get_all_unprocessed_bq_file_metadata",
        "raw_data_branching.us_xx_primary_import_branch.read_and_verify_column_headers",
        "raw_data_branching.us_xx_primary_import_branch.split_by_pre_import_normalization_type",
        "raw_data_branching.us_xx_primary_import_branch.write_import_start",
        "raw_data_branching.us_xx_primary_import_branch.has_files_to_import",
        "raw_data_branching.us_xx_primary_import_branch.get_files_to_import_this_run",
        "raw_data_branching.us_xx_primary_import_branch.raise_operations_registration_errors",
    ]

    def setUp(self) -> None:
        super().setUp()
        self.dag_id = get_raw_data_import_dag_id(_PROJECT_ID)

        self.ingest_states_patcher = patch(
            "recidiviz.airflow.dags.raw_data.raw_data_branching.get_direct_ingest_states_launched_in_env",
            MagicMock(return_value=[StateCode.US_XX, StateCode.US_YY]),
        )
        self.ingest_states_patcher.start()
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

        # This must be patched prior to the import of the raw_data_import_dag in `gcs_operator_patcher`
        # as `DirectIngestListNormalizedUnprocessedFilesOperator` imports and holds a reference to the delegate
        self.delegate_patcher = patch(
            "recidiviz.airflow.dags.raw_data.get_all_unprocessed_bq_file_metadata_sql_query_generator.RawDataImportDelegateFactory",
            FakeRawDataImportDelegateFactory,
        )
        self.delegate_patcher.start()

        self.gcs_operator_patcher = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.DirectIngestListNormalizedUnprocessedFilesOperator",
            side_effect=fake_operator_with_return_value([]),
        )
        self.list_normalized_unprocessed_files_mock = self.gcs_operator_patcher.start()

        self.fs = FakeGCSFileSystem()
        self.gcsfs_patcher = patch(
            "recidiviz.airflow.dags.raw_data.gcs_file_processing_tasks.GcsfsFactory.build",
            return_value=self.fs,
        )
        self.gcsfs_patcher.start()

        self.region_module_patch = patch(
            "recidiviz.ingest.direct.raw_data.raw_file_configs.direct_ingest_regions_module",
            fake_regions,
        )
        self.region_module_patch.start()

        self.header_reader_patcher = patch(
            "recidiviz.airflow.dags.raw_data.gcs_file_processing_tasks.DirectIngestRawFileHeaderReader.read_and_validate_column_headers",
        )
        self.header_reader_mock = self.header_reader_patcher.start()

    def tearDown(self) -> None:
        self.ingest_states_patcher.stop()
        self.cloud_sql_db_hook_patcher.stop()
        self.gcs_operator_patcher.stop()
        self.region_module_patch.stop()
        self.delegate_patcher.stop()
        self.gcsfs_patcher.stop()
        self.region_module_patch.stop()
        self.header_reader_patcher.stop()
        super().tearDown()

    def _create_operations_registration_only_dag(self) -> DAG:
        # pylint: disable=import-outside-toplevel
        from recidiviz.airflow.dags.raw_data_import_dag import (
            create_raw_data_import_dag,
        )

        return create_raw_data_import_dag().partial_subset(
            task_ids_or_regex=self.operations_registrations_tasks_ids,
            include_upstream=False,
            include_downstream=False,
        )

    def test_no_chunked_files(self) -> None:
        self.list_normalized_unprocessed_files_mock.side_effect = fake_operator_with_return_value(
            [
                "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagBasicData.csv",
                "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagFileConfigHeaders.csv",
                "testing/unprocessed_2024-01-27T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv",
            ]
        )
        self.header_reader_mock.return_value = ["col1", "col2", "col3"]
        region_raw_file_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx", region_module=fake_regions
        )

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_operations_registration_only_dag(),
                session=session,
                expected_failure_task_id_regexes=[],  # none!
                expected_skipped_task_id_regexes=[],  # none!
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

            assert sorted(import_ready_files, key=lambda x: x.file_id) == sorted(
                [
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
                        bq_load_config=RawFileBigQueryLoadConfig.from_headers_and_raw_file_config(
                            file_headers=["col1", "col2", "col3"],
                            raw_file_config=region_raw_file_config.raw_file_configs[
                                "tagBasicData"
                            ],
                        ),
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
                        bq_load_config=RawFileBigQueryLoadConfig.from_headers_and_raw_file_config(
                            file_headers=["col1", "col2", "col3"],
                            raw_file_config=region_raw_file_config.raw_file_configs[
                                "tagFileConfigHeaders"
                            ],
                        ),
                    ),
                ],
                key=lambda x: x.file_id,
            )

            # --- validate persisted rows in operations db

            bq_metadata = session.execute(
                text(
                    "SELECT file_id, update_datetime, file_processed_time FROM direct_ingest_raw_big_query_file_metadata"
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
                    "SELECT file_id, gcs_file_id, update_datetime FROM direct_ingest_raw_gcs_file_metadata"
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

            file_imports = session.execute(
                text("SELECT file_id, import_status FROM direct_ingest_raw_file_import")
            )

            assert set(file_imports) == {(1, "STARTED"), (2, "STARTED"), (3, "STARTED")}

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
        self.header_reader_mock.return_value = ["col1", "col2", "col3"]

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_operations_registration_only_dag(),
                session=session,
                expected_failure_task_id_regexes=[
                    "raw_data_branching.us_xx_primary_import_branch.raise_operations_registration_errors"
                ],
                expected_skipped_task_id_regexes=[],  # none!
            )
            self.assertEqual(DagRunState.FAILED, result.dag_run_state)

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
                    "SELECT file_id, update_datetime, file_processed_time FROM direct_ingest_raw_big_query_file_metadata"
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
                    "SELECT file_id, gcs_file_id, update_datetime FROM direct_ingest_raw_gcs_file_metadata"
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

            file_imports = session.execute(
                text("SELECT file_id, import_status FROM direct_ingest_raw_file_import")
            )

            assert set(file_imports) == {(1, "STARTED")}


class RawDataImportDagPreImportNormalizationIntegrationTest(AirflowIntegrationTest):
    """integration tests for step 3: pre-import normalization"""

    step_3_tasks: List[str] = [
        "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.generate_file_chunking_pod_arguments",
        "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_file_chunking_group.raw_data_file_chunking",
        "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_file_chunking_group.push_kpo_mapped_task_output_from_gcs_to_xcom",
        "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.filter_chunking_results_by_errors",
        "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_file_chunking_errors",
        "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.generate_chunk_processing_pod_arguments",
        "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_chunk_normalization_group.raw_data_chunk_normalization",
        "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_chunk_normalization_group.push_kpo_mapped_task_output_from_gcs_to_xcom",
        "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.regroup_and_verify_file_chunks",
        "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_chunk_normalization_errors",
    ]

    def setUp(self) -> None:
        super().setUp()

        # env mocks ---

        self.dag_id = get_raw_data_import_dag_id(_PROJECT_ID)
        self.project_patcher = patch(
            "recidiviz.utils.metadata.project_id", return_value=_PROJECT_ID
        )
        self.project_patcher.start()
        self.environment_patcher = patch(
            "os.environ",
            {
                "GCP_PROJECT": _PROJECT_ID,
            },
        )
        self.environment_patcher.start()

        self.ingest_states_patcher = patch(
            "recidiviz.airflow.dags.raw_data.raw_data_branching.get_direct_ingest_states_launched_in_env",
            MagicMock(return_value=[StateCode.US_XX, StateCode.US_YY]),
        )
        self.ingest_states_patcher.start()

        # instance vars for mocked return vals ---

        self.input_requires_normalization: List[str] = []
        self.input_bq_metadata: List[str] = []
        self.chunking_return_value: List[str] = []
        self.normalization_return_value: List[str] = []
        self.input_bq_load_config: Dict[str, str] = {}

        # operator mocks ---
        self.fs = FakeGCSFileSystem()

        self.gcs_patchers = [
            patch(target, return_value=self.fs)
            for target in [
                "recidiviz.airflow.dags.utils.kubernetes_pod_operator_task_groups.get_gcsfs_from_hook",
                "recidiviz.entrypoints.raw_data.divide_raw_file_into_chunks.GcsfsFactory.build",
                "recidiviz.entrypoints.raw_data.normalize_raw_file_chunks.GcsfsFactory.build",
            ]
        ]
        for patcher in self.gcs_patchers:  # type: ignore
            patcher.start()

        self.kpo_operator_patcher = patch(
            "recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator.RecidivizKubernetesPodOperator.partial",
            side_effect=self._fake_k8s_operator_wrapper,
        )
        self.kpo_operator_mock = self.kpo_operator_patcher.start()

        # This must be patched prior to file_chunking_args_patcher as `generate_file_chunking_pod_arguments` imports
        # and holds a reference to the delegate
        self.delegate_patcher = patch(
            "recidiviz.airflow.dags.raw_data.get_all_unprocessed_bq_file_metadata_sql_query_generator.RawDataImportDelegateFactory",
            FakeRawDataImportDelegateFactory,
        )
        self.delegate_patcher.start()

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

    def tearDown(self) -> None:
        self.environment_patcher.stop()
        self.ingest_states_patcher.stop()
        self.kpo_operator_patcher.stop()
        self.file_chunking_args_patcher.stop()
        self.verify_file_chunks_patcher.stop()
        self.delegate_patcher.stop()
        self.fake_gcs_patch.stop()
        for patcher in self.gcs_patchers:  # type: ignore
            patcher.stop()
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
                self.fs,
                return_value,
                is_mapped=True,
            ),
            *args,
            **kwargs,
        )

    def _file_chunking(self, func: Callable) -> Callable:
        # small wrapper to pass self.input_requires_normalization as the
        # serialized_requires_pre_import_normalization_file_paths parameter of
        # generate_file_chunking_pod_arguments
        def _inner(region_code: str, **kwargs: Any) -> Any:
            return func(
                region_code=region_code,
                serialized_requires_pre_import_normalization_file_paths=self.input_requires_normalization,
                target_num_chunking_airflow_tasks=kwargs[
                    "target_num_chunking_airflow_tasks"
                ],
                max_chunks_per_airflow_task=kwargs["max_chunks_per_airflow_task"],
            )

        return _inner

    def _regroup(self, func: Callable) -> Callable:
        # small wrapper to pass self.input_bq_metadata as the
        # serialized_requires_pre_import_normalization_files_bq_metadata parameter
        # and self.input_bq_load_config as the serialized_bq_table_schema parameter
        # of regroup_and_verify_file_chunks
        def _inner(
            normalized_chunks_result: List[str],
            _irrelevant_metadata: Any,
            _irrelevant_load_config: Any,
            **_kwargs: Any,
        ) -> Any:
            return func(
                normalized_chunks_result,
                self.input_bq_metadata,
                self.input_bq_load_config,
            )

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
            task_ids_or_regex=self.step_3_tasks,
            include_upstream=False,
            include_downstream=False,
        )

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                step_3_only_dag,
                session=session,
                expected_failure_task_id_regexes=[],
                expected_skipped_task_id_regexes=[
                    # we expect this since airflow will skipped mapped tasks w/ an empty input
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_file_chunking_group.raw_data_file_chunking",
                    # these all have ALL_SUCCESS so ^^ being skipped will cause these to be skipped
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_file_chunking_group.push_kpo_mapped_task_output_from_gcs_to_xcom",
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.filter_chunking_results_by_errors",
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_file_chunking_errors",
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.generate_chunk_processing_pod_arguments",
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_chunk_normalization_group.raw_data_chunk_normalization",
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_chunk_normalization_group.push_kpo_mapped_task_output_from_gcs_to_xcom",
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
                [],
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
        self.input_bq_load_config = {
            "1": RawFileBigQueryLoadConfig(
                schema_fields=[], skip_leading_rows=0
            ).serialize(),
            "2": RawFileBigQueryLoadConfig(
                schema_fields=[], skip_leading_rows=0
            ).serialize(),
            "3": RawFileBigQueryLoadConfig(
                schema_fields=[], skip_leading_rows=0
            ).serialize(),
        }

        step_3_only_dag = self._create_dag().partial_subset(
            task_ids_or_regex=self.step_3_tasks,
            include_upstream=False,
            include_downstream=False,
        )

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                step_3_only_dag,
                session=session,
                expected_failure_task_id_regexes=[],  # none!
                expected_skipped_task_id_regexes=[],  # none!
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
                    bq_load_config=RawFileBigQueryLoadConfig(
                        schema_fields=[], skip_leading_rows=0
                    ),
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
                    bq_load_config=RawFileBigQueryLoadConfig(
                        schema_fields=[], skip_leading_rows=0
                    ),
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
                    bq_load_config=RawFileBigQueryLoadConfig(
                        schema_fields=[], skip_leading_rows=0
                    ),
                ),
            ]
        assert _comparable(regrouped_and_verified.results) == _comparable(
            expected_results
        )

    def test_failure_during_normalization(self) -> None:

        self.input_requires_normalization = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
            "testing/unprocessed_2024-01-24T16:35:33:617135_raw_singlePrimaryKey.csv",
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
                ),
                RequiresPreImportNormalizationFile(
                    path=GcsfsFilePath.from_absolute_path(
                        "testing/unprocessed_2024-01-24T16:35:33:617135_raw_singlePrimaryKey.csv",
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
                        #
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
                                "testing/unprocessed_2024-01-24T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            output_file_path=GcsfsFilePath.from_absolute_path(
                                "temp/unprocessed_2024-01-24T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                            ),
                            chunk_boundary=CsvChunkBoundary(
                                chunk_num=0, start_inclusive=0, end_exclusive=2
                            ),
                            crc32c=0,
                        ),
                        PreImportNormalizedCsvChunkResult(
                            input_file_path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-24T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            output_file_path=GcsfsFilePath.from_absolute_path(
                                "temp/unprocessed_2024-01-24T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                            ),
                            chunk_boundary=CsvChunkBoundary(
                                chunk_num=1, start_inclusive=4, end_exclusive=6
                            ),
                            crc32c=1,
                        ),
                        PreImportNormalizedCsvChunkResult(
                            input_file_path=GcsfsFilePath.from_absolute_path(
                                "testing/unprocessed_2024-01-24T16:35:33:617135_raw_singlePrimaryKey.csv",
                            ),
                            output_file_path=GcsfsFilePath.from_absolute_path(
                                "temp/unprocessed_2024-01-24T16:35:33:617135_raw_singlePrimaryKey-2.csv",
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
                    [],
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
                            "testing/unprocessed_2024-01-24T16:35:33:617135_raw_singlePrimaryKey.csv",
                        ),
                    )
                ],
                file_tag="singlePrimaryKey",
                update_datetime=datetime.datetime.fromisoformat(
                    "2024-01-24T16:35:33:617135Z"
                ),
                file_id=3,
            ).serialize(),
        ]
        self.input_bq_load_config = {
            "1": RawFileBigQueryLoadConfig(
                schema_fields=[], skip_leading_rows=0
            ).serialize(),
            "2": RawFileBigQueryLoadConfig(
                schema_fields=[], skip_leading_rows=0
            ).serialize(),
            "3": RawFileBigQueryLoadConfig(
                schema_fields=[], skip_leading_rows=0
            ).serialize(),
        }

        step_3_only_dag = self._create_dag().partial_subset(
            task_ids_or_regex=self.step_3_tasks,
            include_upstream=False,
            include_downstream=False,
        )

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                step_3_only_dag,
                session=session,
                expected_failure_task_id_regexes=[
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_chunk_normalization_errors"
                ],
                expected_skipped_task_id_regexes=[],  # none!
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
                    error_type=DirectIngestRawFileImportStatus.FAILED_IMPORT_BLOCKED,
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
                    error_type=DirectIngestRawFileImportStatus.FAILED_IMPORT_BLOCKED,
                ),
                RawFileProcessingError(
                    error_msg="Blocked Import: failed due to import-blocking failure from GcsfsFilePath(bucket_name='testing', blob_name='unprocessed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv')",
                    original_file_path=GcsfsFilePath(
                        bucket_name="testing",
                        blob_name="unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                    ),
                    temporary_file_paths=[
                        GcsfsFilePath(
                            bucket_name="temp",
                            blob_name="unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                        ),
                        GcsfsFilePath(
                            bucket_name="temp",
                            blob_name="unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                        ),
                        GcsfsFilePath(
                            bucket_name="temp",
                            blob_name="unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey-2.csv",
                        ),
                    ],
                    error_type=DirectIngestRawFileImportStatus.FAILED_IMPORT_BLOCKED,
                ),
            ]

            expected_results = [
                ImportReadyFile(
                    file_id=3,
                    file_tag="singlePrimaryKey",
                    update_datetime=datetime.datetime.fromisoformat(
                        "2024-01-24T16:35:33:617135Z"
                    ),
                    pre_import_normalized_file_paths=[
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-24T16:35:33:617135_raw_singlePrimaryKey-0.csv",
                        ),
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-24T16:35:33:617135_raw_singlePrimaryKey-1.csv",
                        ),
                        GcsfsFilePath.from_absolute_path(
                            "temp/unprocessed_2024-01-24T16:35:33:617135_raw_singlePrimaryKey-2.csv",
                        ),
                    ],
                    original_file_paths=[
                        GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-24T16:35:33:617135_raw_singlePrimaryKey.csv",
                        )
                    ],
                    bq_load_config=RawFileBigQueryLoadConfig(
                        schema_fields=[], skip_leading_rows=0
                    ),
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
                                "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
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
                                chunk_num=1, start_inclusive=4, end_exclusive=6
                            ),
                            crc32c=1,
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
        self.input_bq_load_config = {
            "1": RawFileBigQueryLoadConfig(
                schema_fields=[], skip_leading_rows=0
            ).serialize(),
            "2": RawFileBigQueryLoadConfig(
                schema_fields=[], skip_leading_rows=0
            ).serialize(),
            "3": RawFileBigQueryLoadConfig(
                schema_fields=[], skip_leading_rows=0
            ).serialize(),
        }

        step_3_only_dag = self._create_dag().partial_subset(
            task_ids_or_regex=self.step_3_tasks,
            include_upstream=False,
            include_downstream=False,
        )

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                step_3_only_dag,
                session=session,
                expected_failure_task_id_regexes=[
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_file_chunking_errors"
                ],
                expected_skipped_task_id_regexes=[],  # none!
            )
            self.assertEqual(DagRunState.FAILED, result.dag_run_state)

            regrouped_and_verified_jsonb = self.get_xcom_for_task_id(
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.regroup_and_verify_file_chunks",
                session=session,
                key="return_value",
            )

            raw_data_file_chunking_jsonb = self.get_xcom_for_task_id(
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_file_chunking_group.push_kpo_mapped_task_output_from_gcs_to_xcom",
                session=session,
                key="return_value",
            )

            chunking_errors_jsonb = self.get_xcom_for_task_id(
                "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.filter_chunking_results_by_errors",
                session=session,
                key=CHUNKING_ERRORS,
            )

            assert isinstance(raw_data_file_chunking_jsonb, bytes)

            chunking_output = MappedBatchedTaskOutput.deserialize(
                list(json.loads(raw_data_file_chunking_jsonb)),
                result_cls=RequiresPreImportNormalizationFile,
                error_cls=RawFileProcessingError,
            )

            assert chunking_output.flatten_errors() == [
                RawFileProcessingError(
                    original_file_path=GcsfsFilePath.from_absolute_path(
                        "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                    ),
                    temporary_file_paths=None,
                    error_msg="Ooops!",
                )
            ]

            assert isinstance(chunking_errors_jsonb, bytes)

            chunking_errors = list(
                sorted(
                    [
                        RawFileProcessingError.deserialize(error)
                        for error in json.loads(chunking_errors_jsonb)
                    ],
                    key=lambda x: x.parts.utc_upload_datetime,
                )
            )

            assert len(chunking_errors) == 2
            assert chunking_errors[0] == RawFileProcessingError(
                original_file_path=GcsfsFilePath.from_absolute_path(
                    "testing/unprocessed_2024-01-26T16:35:33:617135_raw_singlePrimaryKey.csv",
                ),
                temporary_file_paths=None,
                error_msg="Ooops!",
            )
            assert chunking_errors[
                1
            ].original_file_path == GcsfsFilePath.from_absolute_path(
                "testing/unprocessed_2024-01-27T16:35:33:617135_raw_singlePrimaryKey.csv",
            )

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
                    bq_load_config=RawFileBigQueryLoadConfig(
                        schema_fields=[], skip_leading_rows=0
                    ),
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

        self.ingest_states_patcher = patch(
            "recidiviz.airflow.dags.raw_data.raw_data_branching.get_direct_ingest_states_launched_in_env",
            MagicMock(return_value=[StateCode.US_XX, StateCode.US_YY]),
        )
        self.ingest_states_patcher.start()

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
        self.ingest_states_patcher.stop()
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
                "raw_data_branching.us_xx_primary_import_branch.big_query_load.load_and_prep_paths_for_batch",
                "raw_data_branching.us_xx_primary_import_branch.big_query_load.raise_load_prep_errors",
                "raw_data_branching.us_xx_primary_import_branch.big_query_load.generate_append_batches",
                "raw_data_branching.us_xx_primary_import_branch.big_query_load.append_ready_file_batches_from_generate_append_batches",
                "raw_data_branching.us_xx_primary_import_branch.big_query_load.append_to_raw_data_table_for_batch",
                "raw_data_branching.us_xx_primary_import_branch.big_query_load.raise_append_errors",
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
                bq_load_config=RawFileBigQueryLoadConfig(
                    schema_fields=[], skip_leading_rows=0
                ),
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
                bq_load_config=RawFileBigQueryLoadConfig(
                    schema_fields=[], skip_leading_rows=0
                ),
            ),
        ]

        self.coalesce_mock.side_effect = fake_task_function_with_return_value(
            [[file.serialize()] for file in import_ready_files]
        )

        self.bq_mock().load_table_from_cloud_storage().output_rows = 100
        self.bq_mock().create_table_from_query().total_rows = 90

        original_paths, _ = self._register_files(import_ready_files)
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_sub_dag(),
                session=session,
                expected_failure_task_id_regexes=[],  # none!
                expected_skipped_task_id_regexes=[],  # none!
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # validate xcom ---

            load_and_prep_jsonb = self.get_xcom_for_task_id(
                "raw_data_branching.us_xx_primary_import_branch.big_query_load.load_and_prep_paths_for_batch",
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
                        table_id="test__singlePrimaryKey__2__transformed",
                    ),
                    raw_rows_count=100,
                ),
                AppendReadyFile(
                    import_ready_file=import_ready_files[1],
                    append_ready_table_address=BigQueryAddress(
                        dataset_id="us_xx_primary_raw_data_temp_load",
                        table_id="test__singlePrimaryKey__3__transformed",
                    ),
                    raw_rows_count=100,
                ),
            ]

            append_result_jsonb = self.get_xcom_for_task_id(
                "raw_data_branching.us_xx_primary_import_branch.big_query_load.append_to_raw_data_table_for_batch",
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


class RawDataImportDagE2ETest(AirflowIntegrationTest):
    """end to end tests for the raw data import dag"""

    metas = [OperationsBase]
    conn_id = "operations_postgres_conn_id"

    def setUp(self) -> None:
        super().setUp()

        # env mocks ---

        self.dag_id = get_raw_data_import_dag_id(_PROJECT_ID)
        self.project_patcher = patch(
            "recidiviz.utils.metadata.project_id", return_value=_PROJECT_ID
        )
        self.project_patcher.start()
        self.ingest_states_patcher = patch(
            "recidiviz.airflow.dags.raw_data.raw_data_branching.get_direct_ingest_states_launched_in_env",
            MagicMock(return_value=[StateCode.US_XX, StateCode.US_LL]),
        )
        self.ingest_states_patcher.start()
        self.region_module_patch = [
            patch(target, fake_regions)
            for target in [
                "recidiviz.airflow.dags.raw_data.utils.direct_ingest_regions_module",
                "recidiviz.ingest.direct.raw_data.raw_file_configs.direct_ingest_regions_module",
                "recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration_collector.regions",
            ]
        ]

        for patcher in self.region_module_patch:
            patcher.start()

        self.delegate_patcher = patch(
            "recidiviz.airflow.dags.raw_data.get_all_unprocessed_bq_file_metadata_sql_query_generator.RawDataImportDelegateFactory",
            FakeRawDataImportDelegateFactory,
        )
        self.delegate_patcher.start()

        # operator mocks ---

        self.cloud_sql_db_hook_patcher = patch(
            "recidiviz.airflow.dags.operators.cloud_sql_query_operator.CloudSQLDatabaseHook"
        )
        self.mock_cloud_sql_db_hook = self.cloud_sql_db_hook_patcher.start()
        self.mock_cloud_sql_db_hook().get_database_hook.return_value = PostgresHook(
            self.conn_id
        )

        self.gcs_operator_patcher = patch(
            "recidiviz.airflow.dags.raw_data_import_dag.DirectIngestListNormalizedUnprocessedFilesOperator",
            side_effect=fake_operator_from_callable(self._return_unprocessed_paths),
        )
        self.list_normalized_unprocessed_files_mock = self.gcs_operator_patcher.start()

        self.kpo_operator_patcher = patch(
            "recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator.RecidivizKubernetesPodOperator.partial",
            side_effect=self._fake_k8s_operator_wrapper,
        )
        self.kpo_operator_mock = self.kpo_operator_patcher.start()

        self.dag_kick_off_patcher = patch(
            "recidiviz.airflow.dags.raw_data.sequencing_tasks.trigger_dag"
        )
        self.dag_kick_off_mock = self.dag_kick_off_patcher.start()

        # task interaction mocks ---

        self.fs = FakeGCSFileSystem()

        self.gcs_patchers = [
            patch(target, return_value=self.fs)
            for target in [
                "recidiviz.airflow.dags.raw_data.gcs_file_processing_tasks.GcsfsFactory.build",
                "recidiviz.airflow.dags.raw_data.bq_load_tasks.GcsfsFactory.build",
                "recidiviz.airflow.dags.raw_data.clean_up_tasks.GcsfsFactory.build",
                "recidiviz.airflow.dags.utils.kubernetes_pod_operator_task_groups.get_gcsfs_from_hook",
                "recidiviz.entrypoints.raw_data.divide_raw_file_into_chunks.GcsfsFactory.build",
                "recidiviz.entrypoints.raw_data.normalize_raw_file_chunks.GcsfsFactory.build",
            ]
        ]
        for patcher in self.gcs_patchers:  # type: ignore
            patcher.start()

        self.load_tasks_bq_patcher = patch(
            "recidiviz.airflow.dags.raw_data.bq_load_tasks.BigQueryClientImpl",
        )
        self.load_bq_mock = self.load_tasks_bq_patcher.start()

        def row_iterator() -> Iterator[Row]:
            mock_row = MagicMock(spec=Row)
            mock_row.items.return_value = {"COL1": "value1"}.items()
            return iter([mock_row])

        single_row_iterator_mock, future_mock = MagicMock(), MagicMock()
        single_row_iterator_mock.__iter__.side_effect = row_iterator
        future_mock.result.return_value = single_row_iterator_mock
        self.load_bq_mock().run_query_async.return_value = future_mock

        # compatibility issues w/ freezegun, see https://github.com/apache/airflow/pull/25511#issuecomment-1204297524
        self.frozen_time = datetime.datetime(2024, 1, 26, 3, 4, 6, tzinfo=datetime.UTC)
        self.processed_time_patcher = patch(
            "recidiviz.airflow.dags.raw_data.write_import_completions_query_generator.datetime",
        )
        self.processed_time_patcher.start().datetime.now.return_value = self.frozen_time

    def tearDown(self) -> None:
        # env ---
        self.project_patcher.stop()
        self.ingest_states_patcher.stop()
        for patcher in self.region_module_patch:
            patcher.start()
        # operators ---
        self.cloud_sql_db_hook_patcher.stop()
        self.kpo_operator_patcher.stop()
        self.dag_kick_off_patcher.stop()
        self.delegate_patcher.stop()
        # task interactions
        for patcher in self.gcs_patchers:  # type: ignore
            patcher.stop()
        self.load_tasks_bq_patcher.stop()
        self.processed_time_patcher.stop()
        super().tearDown()

    def _return_unprocessed_paths(self, operator: Any, _context: Any) -> List[str]:
        return [
            file.abs_path()
            for file in self.fs.ls_with_blob_prefix(
                operator.kwargs["bucket"], "unprocessed"
            )
        ]

    def _fake_k8s_operator_wrapper(self, *args: Any, **kwargs: Any) -> OperatorPartial:
        """because the raw data import dag calls Operator.partial, we need to mock out
        the return value of that function in order to properly mock out the k8s operator
        """

        entrypoint: type[EntrypointInterface]

        if kwargs["task_id"] == "raw_data_file_chunking":
            entrypoint = RawDataFileChunkingEntrypoint
        elif kwargs["task_id"] == "raw_data_chunk_normalization":
            entrypoint = RawDataChunkNormalizationEntrypoint
        else:
            raise ValueError("!!!!!!")

        return partial(
            fake_k8s_operator_for_entrypoint(entrypoint),
            *args,
            **kwargs,
        )

    def _create_dag(self) -> DAG:
        # pylint: disable=import-outside-toplevel
        from recidiviz.airflow.dags.raw_data_import_dag import (
            create_raw_data_import_dag,
        )

        return create_raw_data_import_dag()

    def _acquire_a_resource_lock(self, states: List[StateCode]) -> None:
        # pylint: disable=import-outside-toplevel
        from recidiviz.airflow.dags.raw_data.acquire_resource_lock_sql_query_generator import (
            AcquireRawDataResourceLockSqlQueryGenerator,
        )

        for state in states:
            AcquireRawDataResourceLockSqlQueryGenerator(
                region_code=state.value,
                raw_data_instance=DirectIngestInstance.PRIMARY,
                resources=[
                    DirectIngestRawDataResourceLockResource.BIG_QUERY_RAW_DATA_DATASET
                ],
                lock_description="t3st!ng",
                lock_ttl_seconds=100000,
            ).execute_postgres_query(
                create_autospec(CloudSqlQueryOperator),
                PostgresHook(self.conn_id),
                create_autospec(Context),
            )

    def _load_fixture_data(self, bucket: str, *tags: str) -> None:
        fixture_directory = os.path.dirname(raw_data_fixtures.__file__)
        for file in sorted(os.listdir(fixture_directory)):
            if any(tag in file for tag in tags):
                self.fs.test_add_path(
                    path=GcsfsFilePath.from_bucket_and_blob_name(
                        bucket_name=bucket,
                        blob_name=file,
                    ),
                    local_path=os.path.join(fixture_directory, file),
                )

    def test_branching_resource_lock_acquisition_fails(self) -> None:
        with Session(bind=self.engine) as session, patch(
            "recidiviz.airflow.dags.raw_data.acquire_resource_lock_sql_query_generator.PostgresHook.get_records",
            return_value=None,
        ):
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf={"ingest_instance": "PRIMARY"},
                expected_failure_task_id_regexes=[
                    # intended tasks to fail
                    r".*_primary_import_branch\.acquire_raw_data_resource_locks",
                ],
                expected_skipped_task_id_regexes=[
                    # everything but first and last should be skipped
                    r".*_primary_import_branch\.(?!acquire_raw_data_resource_locks|successfully_acquired_all_locks)",
                    # non-primary branches
                    r".*_secondary_import_branch\..*",
                    "raw_data_branching.branch_end",  # ew, dont make it skip this
                ],
                expected_success_task_id_regexes=[
                    r".*_primary_import_branch.successfully_acquired_all_locks",
                    r"initialize_dag..*",
                    "raw_data_branching.branch_start",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            assert not list(
                session.execute(
                    text("SELECT * FROM direct_ingest_raw_data_resource_lock")
                )
            )

    def test_branching_resource_lock_already_held(self) -> None:

        self._acquire_a_resource_lock(states=[StateCode.US_XX, StateCode.US_LL])

        with Session(bind=self.engine) as session:

            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf={"ingest_instance": "PRIMARY"},
                expected_failure_task_id_regexes=[],
                expected_skipped_task_id_regexes=[
                    # everything but first and last should be skipped
                    r".*_primary_import_branch\.(?!acquire_raw_data_resource_locks|successfully_acquired_all_locks)",
                    # non-primary branches
                    r".*_secondary_import_branch\..*",
                    "raw_data_branching.branch_end",  # ew, dont make it skip this
                ],
                expected_success_task_id_regexes=[
                    r".*_primary_import_branch\.acquire_raw_data_resource_locks",
                    r".*_primary_import_branch.successfully_acquired_all_locks",
                    r"initialize_dag..*",
                    "raw_data_branching.branch_start",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

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
                expected_failure_task_id_regexes=[
                    # intended failure
                    r".*_primary_import_branch\.list_normalized_unprocessed_gcs_file_paths",
                    # this will fail as a result of split_by_pre_import norm not running
                    r".*_primary_import_branch\.big_query_load\..*",
                    # this will fail as a result of write_import_start not running
                    r".*_primary_import_branch.cleanup_and_storage.write_import_completions",
                    # downstream failures
                    r".*_primary_import_branch\.get_all_unprocessed_gcs_file_metadata",
                    r".*_primary_import_branch\.get_all_unprocessed_bq_file_metadata",
                    r".*_primary_import_branch\.has_files_to_import",
                    r".*_primary_import_branch\.get_files_to_import_this_run",
                    r".*_primary_import_branch\.write_import_start",
                    r".*primary_import_branch\.raise_operations_registration_errors",
                    r".*_primary_import_branch\.read_and_verify_column_headers",
                    r".*_primary_import_branch\.raise_header_verification_errors",
                    r".*_primary_import_branch\.split_by_pre_import_normalization_type",
                    r".*_primary_import_branch\.pre_import_normalization\.*",
                    r".*primary_import_branch.coalesce_import_ready_files",
                    r".*_primary_import_branch.cleanup_and_storage.write_file_processed_time",
                    r".*_primary_import_branch.maybe_trigger_dag_rerun",
                    "raw_data_branching.branch_end",
                ],
                expected_skipped_task_id_regexes=[
                    r".*_secondary_import_branch\..*",
                ],
            )
            # failed state will only happen when resource releasing failed
            self.assertEqual(DagRunState.FAILED, result.dag_run_state)

            for lock_released in session.execute(
                text("SELECT released FROM direct_ingest_raw_data_resource_lock")
            ):
                assert lock_released[0]

    def test_no_files_to_import(self) -> None:

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf={"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"},
                expected_failure_task_id_regexes=[],  # none!
                expected_skipped_task_id_regexes=[
                    # step 2 tasks
                    "raw_data_branching.us_xx_primary_import_branch.write_import_start",
                    "raw_data_branching.us_xx_primary_import_branch.get_files_to_import_this_run",
                    "raw_data_branching.us_xx_primary_import_branch.read_and_verify_column_headers",
                    "raw_data_branching.us_xx_primary_import_branch.raise_header_verification_errors",
                    "raw_data_branching.us_xx_primary_import_branch.split_by_pre_import_normalization_type",
                    # step 3, 4 all skipped
                    r"raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.*",
                    "raw_data_branching.us_xx_primary_import_branch.coalesce_import_ready_files",
                    r"raw_data_branching.us_xx_primary_import_branch.big_query_load.*",
                    # clean still runs, we just don't have any metadata to write
                    "raw_data_branching.us_xx_primary_import_branch.cleanup_and_storage.write_import_completions",
                    "raw_data_branching.us_xx_primary_import_branch.cleanup_and_storage.write_file_processed_time",
                    "raw_data_branching.us_xx_primary_import_branch.maybe_trigger_dag_rerun",
                    # other branches
                    r"raw_data_branching.us_ll_primary_import_branch.*",
                    r".*_secondary_import_branch\..*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            for lock_released in session.execute(
                text("SELECT released FROM direct_ingest_raw_data_resource_lock")
            ):
                assert lock_released[0]

            self.dag_kick_off_mock.assert_not_called()

    def test_single_requires_pre_import_norm(self) -> None:

        self._load_fixture_data(
            gcsfs_direct_ingest_bucket_for_state(
                project_id=_PROJECT_ID,
                region_code="US_XX",
                ingest_instance=DirectIngestInstance.PRIMARY,
            ).bucket_name,
            "singlePrimaryKey",
        )

        self.load_bq_mock().load_table_from_cloud_storage().output_rows = 100
        self.load_bq_mock().create_table_from_query().total_rows = 90

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf={"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"},
                expected_failure_task_id_regexes=[],  # none!
                expected_skipped_task_id_regexes=[
                    r".*_secondary_import_branch\..*",
                    r".us_ll_primary_import_branch\..*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # conditions for success for a single file that successfully imported:
            # (1) all files have been moved to storage or cleaned up
            assert self.fs.all_paths == [
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv'
                )
            ]

            # (2) bq client has all the expected calls
            assert self.load_bq_mock().load_table_from_cloud_storage.call_count == 2
            assert self.load_bq_mock().create_table_from_query.call_count == 2
            self.load_bq_mock().create_table_from_query.assert_has_calls(
                [
                    call(),
                    call(
                        address=BigQueryAddress(
                            dataset_id="us_xx_primary_raw_data_temp_load",
                            table_id="1__singlePrimaryKey__1__transformed",
                        ),
                        query=ANY,
                        overwrite=True,
                        use_query_cache=False,
                        job_labels=ANY,
                    ),
                ]
            )

            assert self.load_bq_mock().delete_from_table_async.call_count == 1
            assert self.load_bq_mock().delete_table.call_count == 3
            assert (
                self.load_bq_mock().insert_into_table_from_table_async.call_count == 1
            )

            # (3) file metadata dbs match what we expect
            gcs_metadata = session.execute(
                text(
                    "SELECT file_id, gcs_file_id, update_datetime FROM direct_ingest_raw_gcs_file_metadata"
                )
            )

            assert set(gcs_metadata) == {
                (
                    1,
                    1,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                )
            }

            bq_metadata = session.execute(
                text(
                    "SELECT file_id, update_datetime, file_processed_time FROM direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    1,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                )
            }

            import_runs = session.execute(
                text(
                    "SELECT file_import_id, file_id, import_status, raw_rows FROM direct_ingest_raw_file_import"
                )
            )

            assert set(import_runs) == {(1, 1, "SUCCEEDED", 100)}

            # (4) resource locks are released
            for lock_released in session.execute(
                text("SELECT released FROM direct_ingest_raw_data_resource_lock")
            ):
                assert lock_released[0]

            self.dag_kick_off_mock.assert_not_called()

    def test_single_skips_pre_import_norm(self) -> None:

        self._load_fixture_data(
            gcsfs_direct_ingest_bucket_for_state(
                project_id=_PROJECT_ID,
                region_code="US_XX",
                ingest_instance=DirectIngestInstance.PRIMARY,
            ).bucket_name,
            "tagBasicData",
        )

        self.load_bq_mock().load_table_from_cloud_storage().output_rows = 100
        self.load_bq_mock().create_table_from_query().total_rows = 90

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf={"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"},
                expected_failure_task_id_regexes=[],  # none!
                expected_skipped_task_id_regexes=[
                    # branches that did not run
                    r".*_secondary_import_branch\..*",
                    r".us_ll_primary_import_branch\..*",
                    # skipped bc no pre import norm!
                    r"raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.(?!generate_file_chunking_pod_arguments)",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # conditions for success for a single file that successfully imported:
            # (1) all files have been moved to storage or cleaned up
            assert self.fs.all_paths == [
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_tagBasicData.csv'
                )
            ]

            # (2) bq client has all the expected calls
            assert self.load_bq_mock().load_table_from_cloud_storage.call_count == 2
            assert self.load_bq_mock().create_table_from_query.call_count == 2

            assert self.load_bq_mock().delete_from_table_async.call_count == 1
            assert self.load_bq_mock().delete_table.call_count == 3
            assert (
                self.load_bq_mock().insert_into_table_from_table_async.call_count == 1
            )

            # (3) file metadata dbs match what we expect
            gcs_metadata = session.execute(
                text(
                    "SELECT file_id, gcs_file_id, update_datetime FROM direct_ingest_raw_gcs_file_metadata"
                )
            )

            assert set(gcs_metadata) == {
                (
                    1,
                    1,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                )
            }

            bq_metadata = session.execute(
                text(
                    "SELECT file_id, update_datetime, file_processed_time FROM direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    1,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                )
            }

            import_runs = session.execute(
                text(
                    "SELECT file_import_id, file_id, import_status, raw_rows FROM direct_ingest_raw_file_import"
                )
            )

            assert set(import_runs) == {(1, 1, "SUCCEEDED", 100)}

            # (4) resource locks are released
            for lock_released in session.execute(
                text("SELECT released FROM direct_ingest_raw_data_resource_lock")
            ):
                assert lock_released[0]

            self.dag_kick_off_mock.assert_not_called()

    @patch(
        "recidiviz.airflow.dags.raw_data.file_metadata_tasks.MAX_NUMBER_OF_CHUNKS_FOR_SECONDARY_IMPORT",
        1,
    )
    def test_get_files_to_import_this_run(self) -> None:
        self._load_fixture_data(
            gcsfs_direct_ingest_bucket_for_state(
                project_id=_PROJECT_ID,
                region_code="US_XX",
                ingest_instance=DirectIngestInstance.SECONDARY,
            ).bucket_name,
            "tagMoreBasicData",
        )

        self.load_bq_mock().load_table_from_cloud_storage().output_rows = 100
        self.load_bq_mock().create_table_from_query().total_rows = 90

        with Session(bind=self.engine) as session:
            dag = self._create_dag()
            result = self.run_dag_test(
                dag=dag,
                session=session,
                run_conf={"ingest_instance": "SECONDARY", "state_code_filter": "US_XX"},
                expected_failure_task_id_regexes=[],  # none!
                expected_skipped_task_id_regexes=[
                    # branches that did not run
                    r".us_ll_secondary_import_branch\..*",
                    r"..*primary_import_branch\..*",
                    # skipped bc no pre import norm!
                    r"raw_data_branching.us_xx_secondary_import_branch.pre_import_normalization.(?!generate_file_chunking_pod_arguments)",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # conditions for success for a single file that successfully imported:
            # - earliest file was imported, moved to storage
            # - newer file is still in the ingest bucket
            assert set(self.fs.all_paths) == {
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.SECONDARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_tagMoreBasicData.csv'
                ),
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_bucket_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.SECONDARY).abs_path()}/unprocessed_2024-01-26T16:35:33:617135_raw_tagMoreBasicData.csv'
                ),
            }

            # (2) bq client has all the expected calls
            assert self.load_bq_mock().load_table_from_cloud_storage.call_count == 2
            assert self.load_bq_mock().create_table_from_query.call_count == 2

            assert self.load_bq_mock().delete_from_table_async.call_count == 1
            assert self.load_bq_mock().delete_table.call_count == 3
            assert (
                self.load_bq_mock().insert_into_table_from_table_async.call_count == 1
            )

            # (3) file metadata dbs match what we expect
            gcs_metadata = session.execute(
                text("SELECT update_datetime FROM direct_ingest_raw_gcs_file_metadata")
            )

            assert set(gcs_metadata) == {
                (datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),),
                (datetime.datetime.fromisoformat("2024-01-26T16:35:33:617135Z"),),
            }

            bq_metadata = session.execute(
                text(
                    "SELECT update_datetime, file_processed_time FROM direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
                (
                    datetime.datetime.fromisoformat("2024-01-26T16:35:33:617135Z"),
                    None,
                ),
            }

            import_runs = session.execute(
                text(
                    "SELECT import_status, raw_rows FROM direct_ingest_raw_file_import"
                )
            )

            assert set(import_runs) == {("SUCCEEDED", 100), ("DEFERRED", None)}

            # (4) resource locks are released
            for lock_released in session.execute(
                text("SELECT released FROM direct_ingest_raw_data_resource_lock")
            ):
                assert lock_released[0]

            self.dag_kick_off_mock.assert_called_once_with(
                dag_id=self.dag_id,
                conf={
                    "state_code_filter": "US_XX",
                    "raw_data_instance": "SECONDARY",
                },
            )
            self.dag_kick_off_mock.reset_mock()

            # yay! we run again! to import the second file!

            result = self.run_dag_test(
                dag=dag,
                session=session,
                run_conf={"ingest_instance": "SECONDARY", "state_code_filter": "US_XX"},
                expected_failure_task_id_regexes=[],  # none!
                expected_skipped_task_id_regexes=[
                    # branches that did not run
                    r".us_ll_secondary_import_branch\..*",
                    r"..*primary_import_branch\..*",
                    # skipped bc no pre import norm!
                    r"raw_data_branching.us_xx_secondary_import_branch.pre_import_normalization.(?!generate_file_chunking_pod_arguments)",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # BOTH files are no imported successfully
            assert set(self.fs.all_paths) == {
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.SECONDARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_tagMoreBasicData.csv'
                ),
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.SECONDARY).abs_path()}raw/2024/01/26/processed_2024-01-26T16:35:33:617135_raw_tagMoreBasicData.csv'
                ),
            }

            # (2) bq client has all the expected calls
            assert self.load_bq_mock().load_table_from_cloud_storage.call_count == 1 + 2
            assert self.load_bq_mock().create_table_from_query.call_count == 1 + 2

            assert self.load_bq_mock().delete_from_table_async.call_count == 1 * 2
            assert self.load_bq_mock().delete_table.call_count == 3 * 2
            assert (
                self.load_bq_mock().insert_into_table_from_table_async.call_count
                == 1 * 2
            )

            # (3) file metadata dbs match what we expect
            gcs_metadata = session.execute(
                text("SELECT update_datetime FROM direct_ingest_raw_gcs_file_metadata")
            )

            assert set(gcs_metadata) == {
                (datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),),
                (datetime.datetime.fromisoformat("2024-01-26T16:35:33:617135Z"),),
            }

            bq_metadata = session.execute(
                text(
                    "SELECT update_datetime, file_processed_time FROM direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
                (
                    datetime.datetime.fromisoformat("2024-01-26T16:35:33:617135Z"),
                    self.frozen_time,
                ),
            }

            import_runs = session.execute(
                text(
                    "SELECT fi.import_status, fi.raw_rows, bq.update_datetime FROM direct_ingest_raw_file_import fi INNER JOIN direct_ingest_raw_big_query_file_metadata bq USING(file_id)"
                )
            )

            assert set(import_runs) == {
                (
                    "SUCCEEDED",
                    100,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                ),
                (
                    "DEFERRED",
                    None,
                    datetime.datetime.fromisoformat("2024-01-26T16:35:33:617135Z"),
                ),
                (
                    "SUCCEEDED",
                    100,
                    datetime.datetime.fromisoformat("2024-01-26T16:35:33:617135Z"),
                ),
            }

            # (4) resource locks are released
            for lock_released in session.execute(
                text("SELECT released FROM direct_ingest_raw_data_resource_lock")
            ):
                assert lock_released[0]

            self.dag_kick_off_mock.assert_not_called()

    def test_chunked_file_requires_pre_import_norm(self) -> None:

        self._load_fixture_data(
            gcsfs_direct_ingest_bucket_for_state(
                project_id=_PROJECT_ID,
                region_code="US_XX",
                ingest_instance=DirectIngestInstance.PRIMARY,
            ).bucket_name,
            "tagChunkedFile",
        )

        self.load_bq_mock().load_table_from_cloud_storage().output_rows = 100
        self.load_bq_mock().create_table_from_query().total_rows = 90

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf={"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"},
                expected_failure_task_id_regexes=[],  # none!
                expected_skipped_task_id_regexes=[
                    r".*_secondary_import_branch\..*",
                    r".us_ll_primary_import_branch\..*",
                ],
            )
            import_ready_files_jsonb = self.get_xcom_for_task_id(
                "raw_data_branching.us_xx_primary_import_branch.get_all_unprocessed_bq_file_metadata",
                session=session,
                key="skipped_file_errors",
            )
            print("booya")
            print(import_ready_files_jsonb)
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # conditions for success for a single file that successfully imported:
            # (1) all files have been moved to storage or cleaned up
            storage_path = gcsfs_direct_ingest_storage_directory_path_for_state(
                region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY
            ).abs_path()
            assert set(self.fs.all_paths) == {
                GcsfsFilePath.from_absolute_path(
                    f"{storage_path}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-1.csv"
                ),
                GcsfsFilePath.from_absolute_path(
                    f"{storage_path}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-2.csv"
                ),
                GcsfsFilePath.from_absolute_path(
                    f"{storage_path}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-3.csv"
                ),
            }

            # (2) bq client has all the expected calls
            assert self.load_bq_mock().load_table_from_cloud_storage.call_count == 2
            assert self.load_bq_mock().create_table_from_query.call_count == 2

            assert self.load_bq_mock().delete_from_table_async.call_count == 1
            assert self.load_bq_mock().delete_table.call_count == 3
            assert (
                self.load_bq_mock().insert_into_table_from_table_async.call_count == 1
            )

            # (3) file metadata dbs match what we expect
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
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                ),
                (
                    1,
                    3,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                ),
            }

            bq_metadata = session.execute(
                text(
                    "select file_id, update_datetime, file_processed_time from direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    1,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                )
            }

            import_runs = session.execute(
                text(
                    "select file_import_id, file_id, import_status, raw_rows from direct_ingest_raw_file_import"
                )
            )

            assert set(import_runs) == {(1, 1, "SUCCEEDED", 100)}

            # (4) resource locks are released
            for lock_released in session.execute(
                text("select released from direct_ingest_raw_data_resource_lock")
            ):
                assert lock_released[0]

            self.dag_kick_off_mock.assert_not_called()

    def test_some_pre_import_some_not_same_state(self) -> None:

        self._load_fixture_data(
            gcsfs_direct_ingest_bucket_for_state(
                project_id=_PROJECT_ID,
                region_code="US_XX",
                ingest_instance=DirectIngestInstance.PRIMARY,
            ).bucket_name,
            "tagChunkedFile",
            "singlePrimaryKey",
            "tagBasicData",
        )

        self.load_bq_mock().load_table_from_cloud_storage().output_rows = 100
        self.load_bq_mock().create_table_from_query().total_rows = 90

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf={"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"},
                expected_failure_task_id_regexes=[],  # none!
                expected_skipped_task_id_regexes=[
                    r".*_secondary_import_branch\..*",
                    r".us_ll_primary_import_branch\..*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # conditions for success for a single file that successfully imported:
            # (1) all files have been moved to storage or cleaned up
            storage_path = gcsfs_direct_ingest_storage_directory_path_for_state(
                region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY
            ).abs_path()
            assert set(self.fs.all_paths) == {
                GcsfsFilePath.from_absolute_path(
                    f"{storage_path}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-1.csv"
                ),
                GcsfsFilePath.from_absolute_path(
                    f"{storage_path}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-2.csv"
                ),
                GcsfsFilePath.from_absolute_path(
                    f"{storage_path}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-3.csv"
                ),
                GcsfsFilePath.from_absolute_path(
                    f"{storage_path}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv"
                ),
                GcsfsFilePath.from_absolute_path(
                    f"{storage_path}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_tagBasicData.csv"
                ),
            }

            # (2) bq client has all the expected calls
            assert self.load_bq_mock().load_table_from_cloud_storage.call_count == 1 + 3
            assert self.load_bq_mock().create_table_from_query.call_count == 1 + 3

            assert self.load_bq_mock().delete_from_table_async.call_count == 1 * 3
            assert self.load_bq_mock().delete_table.call_count == 3 * 3
            assert (
                self.load_bq_mock().insert_into_table_from_table_async.call_count
                == 1 * 3
            )

            # (3) file metadata dbs match what we expect
            gcs_metadata_counts = session.execute(
                text(
                    "select count(file_id) from direct_ingest_raw_gcs_file_metadata group by file_id"
                )
            )

            assert sorted(gcs_metadata_counts) == [(1,), (1,), (3,)]

            bq_metadata = session.execute(
                text(
                    "select file_id, update_datetime, file_processed_time from direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    1,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
                (
                    2,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
                (
                    3,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
            }

            import_runs = session.execute(
                text(
                    "select file_id, import_status, raw_rows from direct_ingest_raw_file_import"
                )
            )

            assert set(import_runs) == {
                (1, "SUCCEEDED", 100),
                (2, "SUCCEEDED", 100),
                (3, "SUCCEEDED", 100),
            }

            # (4) resource locks are released
            for lock_released in session.execute(
                text("select released from direct_ingest_raw_data_resource_lock")
            ):
                assert lock_released[0]

            self.dag_kick_off_mock.assert_not_called()

    def test_single_file_for_one_state_but_all_primary_conf(self) -> None:

        self._load_fixture_data(
            gcsfs_direct_ingest_bucket_for_state(
                project_id=_PROJECT_ID,
                region_code="US_XX",
                ingest_instance=DirectIngestInstance.PRIMARY,
            ).bucket_name,
            "singlePrimaryKey",
        )

        self.load_bq_mock().load_table_from_cloud_storage().output_rows = 100
        self.load_bq_mock().create_table_from_query().total_rows = 90

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf={"ingest_instance": "PRIMARY"},
                expected_failure_task_id_regexes=[],  # none!
                expected_skipped_task_id_regexes=[
                    r".*_secondary_import_branch\..*",
                    # us_ll primary had no files
                    "raw_data_branching.us_ll_primary_import_branch.write_import_start",
                    "raw_data_branching.us_ll_primary_import_branch.get_files_to_import_this_run",
                    "raw_data_branching.us_ll_primary_import_branch.read_and_verify_column_headers",
                    "raw_data_branching.us_ll_primary_import_branch.raise_header_verification_errors",
                    "raw_data_branching.us_ll_primary_import_branch.split_by_pre_import_normalization_type",
                    r"raw_data_branching\.us_ll_primary_import_branch\.pre_import_normalization\..*",
                    "raw_data_branching.us_ll_primary_import_branch.coalesce_import_ready_files",
                    "raw_data_branching.us_ll_primary_import_branch.maybe_trigger_dag_rerun",
                    r"raw_data_branching\.us_ll_primary_import_branch\.big_query_load\..*",
                    r"raw_data_branching\.us_ll_primary_import_branch\.cleanup_and_storage\.write_import_completions",
                    r"raw_data_branching\.us_ll_primary_import_branch\.cleanup_and_storage\.write_file_processed_time",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # conditions for success for a single file that successfully imported:
            # (1) all files have been moved to storage or cleaned up
            assert self.fs.all_paths == [
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv'
                )
            ]

            # (2) bq client has all the expected calls
            assert self.load_bq_mock().load_table_from_cloud_storage.call_count == 2
            assert self.load_bq_mock().create_table_from_query.call_count == 2

            assert self.load_bq_mock().delete_from_table_async.call_count == 1
            assert self.load_bq_mock().delete_table.call_count == 3
            assert (
                self.load_bq_mock().insert_into_table_from_table_async.call_count == 1
            )

            # (3) file metadata dbs match what we expect
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
                )
            }

            bq_metadata = session.execute(
                text(
                    "select file_id, update_datetime, file_processed_time from direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    1,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                )
            }

            import_runs = session.execute(
                text(
                    "select file_import_id, file_id, import_status, raw_rows from direct_ingest_raw_file_import"
                )
            )

            assert set(import_runs) == {(1, 1, "SUCCEEDED", 100)}

            # (4) resource locks are released
            for lock_released in session.execute(
                text("select released from direct_ingest_raw_data_resource_lock")
            ):
                assert lock_released[0]

            self.dag_kick_off_mock.assert_not_called()

    def test_single_file_for_all_states(self) -> None:

        self._load_fixture_data(
            gcsfs_direct_ingest_bucket_for_state(
                project_id=_PROJECT_ID,
                region_code="US_XX",
                ingest_instance=DirectIngestInstance.PRIMARY,
            ).bucket_name,
            "singlePrimaryKey",
        )
        self._load_fixture_data(
            gcsfs_direct_ingest_bucket_for_state(
                project_id=_PROJECT_ID,
                region_code="US_LL",
                ingest_instance=DirectIngestInstance.PRIMARY,
            ).bucket_name,
            "customDatetimeSql",
        )

        self.load_bq_mock().load_table_from_cloud_storage().output_rows = 100
        self.load_bq_mock().create_table_from_query().total_rows = 90

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf={"ingest_instance": "PRIMARY"},
                expected_failure_task_id_regexes=[],  # none!
                expected_skipped_task_id_regexes=[
                    r".*_secondary_import_branch\..*",
                    # us_ll primary had no pre-import norm
                    r"raw_data_branching.us_ll_primary_import_branch.pre_import_normalization.(?!generate_file_chunking_pod_arguments)",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # conditions for success for a single file that successfully imported:
            # (1) all files have been moved to storage or cleaned up
            assert set(self.fs.all_paths) == {
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv'
                ),
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_LL", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_customDatetimeSql.csv'
                ),
            }

            # (2) bq client has all the expected calls
            assert self.load_bq_mock().load_table_from_cloud_storage.call_count == 1 + 2
            assert self.load_bq_mock().create_table_from_query.call_count == 1 + 2

            assert self.load_bq_mock().delete_from_table_async.call_count == 1 * 2
            assert self.load_bq_mock().delete_table.call_count == 3 * 2
            assert (
                self.load_bq_mock().insert_into_table_from_table_async.call_count
                == 1 * 2
            )

            # (3) file metadata dbs match what we expect
            gcs_metadata = session.execute(
                text(
                    "select update_datetime, region_code from direct_ingest_raw_gcs_file_metadata"
                )
            )

            assert set(gcs_metadata) == {
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    "US_XX",
                ),
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    "US_LL",
                ),
            }

            bq_metadata = session.execute(
                text(
                    "select update_datetime, file_processed_time, region_code from direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                    "US_XX",
                ),
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                    "US_LL",
                ),
            }

            import_runs = session.execute(
                text(
                    "select file_id, import_status, raw_rows from direct_ingest_raw_file_import"
                )
            )

            assert set(import_runs) == {
                (1, "SUCCEEDED", 100),
                (2, "SUCCEEDED", 100),
            }

            # (4) resource locks are released
            locks = list(
                session.execute(
                    text("select released from direct_ingest_raw_data_resource_lock")
                )
            )
            assert len(locks) == 6
            assert all(l[0] for l in locks)

            self.dag_kick_off_mock.assert_not_called()

    def test_errors_during_file_chunking(self) -> None:

        self._load_fixture_data(
            gcsfs_direct_ingest_bucket_for_state(
                project_id=_PROJECT_ID,
                region_code="US_XX",
                ingest_instance=DirectIngestInstance.PRIMARY,
            ).bucket_name,
            "singlePrimaryKey",
            "tagCustomLineTerminatorNonUTF8",
        )

        self.load_bq_mock().load_table_from_cloud_storage().output_rows = 100
        self.load_bq_mock().create_table_from_query().total_rows = 90

        def _fail_wrapper(original: Callable) -> Callable:
            def _fail_single_primary_key(fs: Any, path: GcsfsFilePath, cg: Any) -> Any:
                if "tagCustomLineTerminatorNonUTF8" in path.blob_name:
                    raise ValueError(f"Intentional failure for {path.blob_name}")

                return original(fs, path, cg)

            return _fail_single_primary_key

        chunking_patcher = patch(
            "recidiviz.entrypoints.raw_data.divide_raw_file_into_chunks._extract_file_chunks"
        )
        original, _ = chunking_patcher.get_original()

        with Session(bind=self.engine) as session, chunking_patcher as chunking_mock:

            dag = self._create_dag()
            chunking_mock.side_effect = _fail_wrapper(original)
            result = self.run_dag_test(
                dag,
                session=session,
                run_conf={"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"},
                expected_failure_task_id_regexes=[
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_file_chunking_errors"
                ],
                expected_skipped_task_id_regexes=[
                    # branches that did not run
                    r".*_secondary_import_branch\..*",
                    r".us_ll_primary_import_branch\..*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # conditions for success for a single file and failure for another:
            # (1) successful file has moved to storage, failed file stayed put
            assert set(self.fs.all_paths) == {
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv'
                ),
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_bucket_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}/unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv'
                ),
            }

            # (2) bq client has all the expected calls
            assert self.load_bq_mock().load_table_from_cloud_storage.call_count == 2
            assert self.load_bq_mock().create_table_from_query.call_count == 2

            assert self.load_bq_mock().delete_from_table_async.call_count == 1
            assert self.load_bq_mock().delete_table.call_count == 3
            assert (
                self.load_bq_mock().insert_into_table_from_table_async.call_count == 1
            )

            # (3) file metadata dbs match what we expect
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
                    2,
                    2,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                ),
            }

            bq_metadata = session.execute(
                text(
                    "select update_datetime, file_processed_time from direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    None,
                ),
            }

            import_runs = session.execute(
                text(
                    "select import_status, raw_rows from direct_ingest_raw_file_import"
                )
            )

            assert sorted(import_runs, key=lambda x: x[0]) == [
                ("FAILED_PRE_IMPORT_NORMALIZATION_STEP", None),
                ("SUCCEEDED", 100),
            ]

            error_messages = session.execute(
                text(
                    "select import_status, error_message from direct_ingest_raw_file_import"
                )
            )

            error_mes = {
                ms["import_status"]: ms["error_message"] for ms in error_messages
            }

            assert error_mes["SUCCEEDED"] is None
            assert error_mes["FAILED_PRE_IMPORT_NORMALIZATION_STEP"].startswith(
                "recidiviz-testing-direct-ingest-state-us-xx/unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv: Intentional failure for unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv"
            )

            # (4) resource locks are released
            locks = list(
                session.execute(
                    text("select released from direct_ingest_raw_data_resource_lock")
                )
            )
            assert len(locks) == 3
            assert all(l[0] for l in locks)
            self.dag_kick_off_mock.assert_not_called()

            # rerun!! ------------

            result_two = self.run_dag_test(
                dag,
                session=session,
                run_conf={"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"},
                expected_failure_task_id_regexes=[
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_file_chunking_errors"
                ],
                expected_skipped_task_id_regexes=[
                    # branches that did not run
                    r".*_secondary_import_branch\..*",
                    r".us_ll_primary_import_branch\..*",
                    # skipped since no files to import after failed chunking step!
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_chunk_normalization_group.raw_data_chunk_normalization",
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raw_data_chunk_normalization_group.push_kpo_mapped_task_output_from_gcs_to_xcom",
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.regroup_and_verify_file_chunks",
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_chunk_normalization_errors",
                    r"raw_data_branching\.us_xx_primary_import_branch.big_query_load\..*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result_two.dag_run_state)

            # conditions for success for rerun ending in failure:
            # (1) successful file stayed put, failed file stayed put
            assert set(self.fs.all_paths) == {
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv'
                ),
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_bucket_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}/unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv'
                ),
            }

            # (2) bq client has all the expected calls from last time
            assert self.load_bq_mock().load_table_from_cloud_storage.call_count == 2
            assert self.load_bq_mock().create_table_from_query.call_count == 2

            assert self.load_bq_mock().delete_from_table_async.call_count == 1
            assert self.load_bq_mock().delete_table.call_count == 3
            assert (
                self.load_bq_mock().insert_into_table_from_table_async.call_count == 1
            )

            # (3) file metadata dbs match what we expect
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
                    2,
                    2,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                ),
            }

            bq_metadata = session.execute(
                text(
                    "select update_datetime, file_processed_time from direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    None,
                ),
            }

            import_runs = session.execute(
                text(
                    "select import_status, raw_rows from direct_ingest_raw_file_import"
                )
            )

            assert sorted(import_runs, key=lambda x: x[0]) == [
                ("FAILED_PRE_IMPORT_NORMALIZATION_STEP", None),
                ("FAILED_PRE_IMPORT_NORMALIZATION_STEP", None),
                ("SUCCEEDED", 100),
            ]

            # (4) resource locks are released
            locks = list(
                session.execute(
                    text("select released from direct_ingest_raw_data_resource_lock")
                )
            )
            assert len(locks) == 6
            assert all(l[0] for l in locks)
            self.dag_kick_off_mock.assert_not_called()

            chunking_mock.side_effect = original

            # third time, no failures this time!
            result_three = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf={"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"},
                expected_failure_task_id_regexes=[],  # noooooone!
                expected_skipped_task_id_regexes=[
                    # branches that did not run
                    r".*_secondary_import_branch\..*",
                    r".us_ll_primary_import_branch\..*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result_three.dag_run_state)

            # conditions for success for rerun ending in failure:
            # (1) successful file stayed put, failed file stayed put
            assert set(self.fs.all_paths) == {
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv'
                ),
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv'
                ),
            }

            # (2) bq client has all the expected calls from last time
            assert self.load_bq_mock().load_table_from_cloud_storage.call_count == 1 + 2
            assert self.load_bq_mock().create_table_from_query.call_count == 1 + 2

            assert self.load_bq_mock().delete_from_table_async.call_count == 2
            assert self.load_bq_mock().delete_table.call_count == 3 * 2
            assert (
                self.load_bq_mock().insert_into_table_from_table_async.call_count == 2
            )

            # (3) file metadata dbs match what we expect
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
                    2,
                    2,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                ),
            }

            bq_metadata = session.execute(
                text(
                    "select update_datetime, file_processed_time from direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
            }

            import_runs = session.execute(
                text(
                    "select import_status, raw_rows from direct_ingest_raw_file_import"
                )
            )

            assert sorted(import_runs, key=lambda x: x[0]) == [
                ("FAILED_PRE_IMPORT_NORMALIZATION_STEP", None),
                ("FAILED_PRE_IMPORT_NORMALIZATION_STEP", None),
                ("SUCCEEDED", 100),
                ("SUCCEEDED", 100),
            ]

            # (4) resource locks are released
            locks = list(
                session.execute(
                    text("select released from direct_ingest_raw_data_resource_lock")
                )
            )
            assert len(locks) == 9
            assert all(l[0] for l in locks)
            self.dag_kick_off_mock.assert_not_called()

    def test_errors_during_pre_import_norm(self) -> None:

        self._load_fixture_data(
            gcsfs_direct_ingest_bucket_for_state(
                project_id=_PROJECT_ID,
                region_code="US_XX",
                ingest_instance=DirectIngestInstance.PRIMARY,
            ).bucket_name,
            "singlePrimaryKey",
            "tagCustomLineTerminatorNonUTF8",
        )

        self.load_bq_mock().load_table_from_cloud_storage().output_rows = 100
        self.load_bq_mock().create_table_from_query().total_rows = 90

        def _fail_wrapper(original: Callable) -> Callable:
            def _fail(f: Any, **kwargs: GcsfsFilePath) -> Any:
                if kwargs["name"] == "tagCustomLineTerminatorNonUTF8":
                    raise ValueError(f"Intentional failure for {kwargs['name']}")

                return original(f, **kwargs)

            return _fail

        chunking_patcher = patch(
            "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_pre_import_normalizer.BytesChunkReader"
        )
        original, _ = chunking_patcher.get_original()

        with Session(bind=self.engine) as session, chunking_patcher as chunking_mock:

            dag = self._create_dag()
            chunking_mock.side_effect = _fail_wrapper(original)
            result = self.run_dag_test(
                dag,
                session=session,
                run_conf={"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"},
                expected_failure_task_id_regexes=[
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_chunk_normalization_errors"
                ],
                expected_skipped_task_id_regexes=[
                    # branches that did not run
                    r".*_secondary_import_branch\..*",
                    r".us_ll_primary_import_branch\..*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # conditions for success for a single file and failure for another:
            # (1) successful file has moved to storage, failed file stayed put
            assert set(self.fs.all_paths) == {
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv'
                ),
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_bucket_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}/unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv'
                ),
            }

            # (2) bq client has all the expected calls
            assert self.load_bq_mock().load_table_from_cloud_storage.call_count == 2
            assert self.load_bq_mock().create_table_from_query.call_count == 2

            assert self.load_bq_mock().delete_from_table_async.call_count == 1
            assert self.load_bq_mock().delete_table.call_count == 3
            assert (
                self.load_bq_mock().insert_into_table_from_table_async.call_count == 1
            )

            # (3) file metadata dbs match what we expect
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
                    2,
                    2,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                ),
            }

            bq_metadata = session.execute(
                text(
                    "select update_datetime, file_processed_time from direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    None,
                ),
            }

            import_runs = session.execute(
                text(
                    "select import_status, raw_rows from direct_ingest_raw_file_import"
                )
            )

            assert sorted(import_runs, key=lambda x: x[0]) == [
                ("FAILED_PRE_IMPORT_NORMALIZATION_STEP", None),
                ("SUCCEEDED", 100),
            ]

            error_messages = session.execute(
                text(
                    "select import_status, error_message from direct_ingest_raw_file_import"
                )
            )

            error_mes = {
                ms["import_status"]: ms["error_message"] for ms in error_messages
            }

            assert error_mes["SUCCEEDED"] is None
            assert error_mes["FAILED_PRE_IMPORT_NORMALIZATION_STEP"].startswith(
                "Error for file GcsfsFilePath(bucket_name='recidiviz-testing-direct-ingest-state-us-xx', blob_name='unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv') chunk CsvChunkBoundary(start_inclusive=0, end_exclusive=241, chunk_num=0): Intentional failure for tagCustomLineTerminatorNonUTF8\nTraceback"
            )

            # (4) resource locks are released
            locks = list(
                session.execute(
                    text("select released from direct_ingest_raw_data_resource_lock")
                )
            )
            assert len(locks) == 3
            assert all(l[0] for l in locks)
            self.dag_kick_off_mock.assert_not_called()

            # rerun!! ------------

            result_two = self.run_dag_test(
                dag,
                session=session,
                run_conf={"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"},
                expected_failure_task_id_regexes=[
                    "raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_chunk_normalization_errors"
                ],
                expected_skipped_task_id_regexes=[
                    # branches that did not run
                    r".*_secondary_import_branch\..*",
                    r".us_ll_primary_import_branch\..*",
                    # skipped since no files to import after failed chunking step!
                    r"raw_data_branching\.us_xx_primary_import_branch.big_query_load\..*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result_two.dag_run_state)

            # conditions for success for rerun ending in failure:
            # (1) successful file stayed put, failed file stayed put
            assert set(self.fs.all_paths) == {
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv'
                ),
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_bucket_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}/unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv'
                ),
            }

            # (2) bq client has all the expected calls from last time
            assert self.load_bq_mock().load_table_from_cloud_storage.call_count == 2
            assert self.load_bq_mock().create_table_from_query.call_count == 2

            assert self.load_bq_mock().delete_from_table_async.call_count == 1
            assert self.load_bq_mock().delete_table.call_count == 3
            assert (
                self.load_bq_mock().insert_into_table_from_table_async.call_count == 1
            )

            # (3) file metadata dbs match what we expect
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
                    2,
                    2,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                ),
            }

            bq_metadata = session.execute(
                text(
                    "select update_datetime, file_processed_time from direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    None,
                ),
            }

            import_runs = session.execute(
                text(
                    "select import_status, raw_rows from direct_ingest_raw_file_import"
                )
            )

            assert sorted(import_runs, key=lambda x: x[0]) == [
                ("FAILED_PRE_IMPORT_NORMALIZATION_STEP", None),
                ("FAILED_PRE_IMPORT_NORMALIZATION_STEP", None),
                ("SUCCEEDED", 100),
            ]

            # (4) resource locks are released
            locks = list(
                session.execute(
                    text("select released from direct_ingest_raw_data_resource_lock")
                )
            )
            assert len(locks) == 6
            assert all(l[0] for l in locks)
            self.dag_kick_off_mock.assert_not_called()

            chunking_mock.side_effect = original

            # third time, no failures this time!
            result_three = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf={"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"},
                expected_failure_task_id_regexes=[],  # noooooone!
                expected_skipped_task_id_regexes=[
                    # branches that did not run
                    r".*_secondary_import_branch\..*",
                    r".us_ll_primary_import_branch\..*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result_three.dag_run_state)

            # conditions for success for rerun ending in failure:
            # (1) successful file stayed put, failed file stayed put
            assert set(self.fs.all_paths) == {
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv'
                ),
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv'
                ),
            }

            # (2) bq client has all the expected calls from last time
            assert self.load_bq_mock().load_table_from_cloud_storage.call_count == 1 + 2
            assert self.load_bq_mock().create_table_from_query.call_count == 1 + 2

            assert self.load_bq_mock().delete_from_table_async.call_count == 2
            assert self.load_bq_mock().delete_table.call_count == 3 * 2
            assert (
                self.load_bq_mock().insert_into_table_from_table_async.call_count == 2
            )

            # (3) file metadata dbs match what we expect
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
                    2,
                    2,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                ),
            }

            bq_metadata = session.execute(
                text(
                    "select update_datetime, file_processed_time from direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
            }

            import_runs = session.execute(
                text(
                    "select import_status, raw_rows from direct_ingest_raw_file_import"
                )
            )

            assert sorted(import_runs, key=lambda x: x[0]) == [
                ("FAILED_PRE_IMPORT_NORMALIZATION_STEP", None),
                ("FAILED_PRE_IMPORT_NORMALIZATION_STEP", None),
                ("SUCCEEDED", 100),
                ("SUCCEEDED", 100),
            ]

            # (4) resource locks are released
            locks = list(
                session.execute(
                    text("select released from direct_ingest_raw_data_resource_lock")
                )
            )
            assert len(locks) == 9
            assert all(l[0] for l in locks)
            self.dag_kick_off_mock.assert_not_called()

    def test_errors_during_load_and_prep(self) -> None:

        self._load_fixture_data(
            gcsfs_direct_ingest_bucket_for_state(
                project_id=_PROJECT_ID,
                region_code="US_XX",
                ingest_instance=DirectIngestInstance.PRIMARY,
            ).bucket_name,
            "singlePrimaryKey",
            "tagCustomLineTerminatorNonUTF8",
        )

        should_fail = True

        def _fail_wrapper(**kwargs: Any) -> MagicMock:
            if should_fail and any(
                "tagCustomLineTerminatorNonUTF8" in uri for uri in kwargs["source_uris"]
            ):
                raise ValueError(f"Intentional failure for {kwargs['source_uris']}")

            mock = MagicMock()
            mock.output_rows = 100
            return mock

        self.load_bq_mock().load_table_from_cloud_storage.side_effect = _fail_wrapper
        self.load_bq_mock().create_table_from_query().total_rows = 90

        with Session(bind=self.engine) as session:

            dag = self._create_dag()
            result = self.run_dag_test(
                dag,
                session=session,
                run_conf={"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"},
                expected_failure_task_id_regexes=[
                    "raw_data_branching.us_xx_primary_import_branch.big_query_load.raise_load_prep_errors"
                ],
                expected_skipped_task_id_regexes=[
                    # branches that did not run
                    r".*_secondary_import_branch\..*",
                    r".us_ll_primary_import_branch\..*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # conditions for success for a single file and failure for another:
            # (1) successful file has moved to storage, failed file stayed put
            # and we left the temp files in place
            assert set(self.fs.all_paths) == {
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv'
                ),
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_bucket_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}/unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv'
                ),
                GcsfsFilePath(
                    bucket_name="recidiviz-testing-direct-ingest-temporary-files",
                    blob_name="temp_unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8_0.csv",
                ),
            }

            # (2) bq client has all the expected calls
            assert self.load_bq_mock().load_table_from_cloud_storage.call_count == 2
            assert self.load_bq_mock().create_table_from_query.call_count == 2

            assert self.load_bq_mock().delete_from_table_async.call_count == 1
            # we shouldn't delete the tables for the failed files
            assert self.load_bq_mock().delete_table.call_count == 3
            assert (
                self.load_bq_mock().insert_into_table_from_table_async.call_count == 1
            )

            # (3) file metadata dbs match what we expect
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
                    2,
                    2,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                ),
            }

            bq_metadata = session.execute(
                text(
                    "select update_datetime, file_processed_time from direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    None,
                ),
            }

            import_runs = session.execute(
                text(
                    "select import_status, raw_rows from direct_ingest_raw_file_import"
                )
            )

            assert sorted(import_runs, key=lambda x: x[0]) == [
                ("FAILED_LOAD_STEP", None),
                ("SUCCEEDED", 100),
            ]

            error_messages = session.execute(
                text(
                    "select import_status, error_message from direct_ingest_raw_file_import"
                )
            )

            error_mes = {
                ms["import_status"]: ms["error_message"] for ms in error_messages
            }

            assert error_mes["SUCCEEDED"] is None
            assert error_mes["FAILED_LOAD_STEP"].startswith(
                "Intentional failure for ['gs://recidiviz-testing-direct-ingest-temporary-files/temp_unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8_0.csv']"
            )

            # (4) resource locks are released
            locks = list(
                session.execute(
                    text("select released from direct_ingest_raw_data_resource_lock")
                )
            )
            assert len(locks) == 3
            assert all(l[0] for l in locks)
            self.dag_kick_off_mock.assert_not_called()

            # rerun!! ------------

            result_two = self.run_dag_test(
                dag,
                session=session,
                run_conf={"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"},
                expected_failure_task_id_regexes=[
                    "raw_data_branching.us_xx_primary_import_branch.big_query_load.raise_load_prep_errors"
                ],
                expected_skipped_task_id_regexes=[
                    # branches that did not run
                    r".*_secondary_import_branch\..*",
                    r".us_ll_primary_import_branch\..*",
                    # skipped since no files to import after no load prep!
                    "raw_data_branching.us_xx_primary_import_branch.big_query_load.raise_append_errors",
                    "raw_data_branching.us_xx_primary_import_branch.big_query_load.append_to_raw_data_table_for_batch",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result_two.dag_run_state)

            # conditions for success for rerun ending in failure:
            # (1) successful file stayed put, failed file stayed put
            assert set(self.fs.all_paths) == {
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv'
                ),
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_bucket_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}/unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv'
                ),
                GcsfsFilePath(
                    bucket_name="recidiviz-testing-direct-ingest-temporary-files",
                    blob_name="temp_unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8_0.csv",
                ),
            }

            # (2) bq client has all the expected calls from last time
            assert self.load_bq_mock().load_table_from_cloud_storage.call_count == 3
            assert self.load_bq_mock().create_table_from_query.call_count == 2

            assert self.load_bq_mock().delete_from_table_async.call_count == 1
            assert self.load_bq_mock().delete_table.call_count == 3
            assert (
                self.load_bq_mock().insert_into_table_from_table_async.call_count == 1
            )

            # (3) file metadata dbs match what we expect
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
                    2,
                    2,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                ),
            }

            bq_metadata = session.execute(
                text(
                    "select update_datetime, file_processed_time from direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    None,
                ),
            }

            import_runs = session.execute(
                text(
                    "select import_status, raw_rows from direct_ingest_raw_file_import"
                )
            )

            assert sorted(import_runs, key=lambda x: x[0]) == [
                ("FAILED_LOAD_STEP", None),
                ("FAILED_LOAD_STEP", None),
                ("SUCCEEDED", 100),
            ]

            # (4) resource locks are released
            locks = list(
                session.execute(
                    text("select released from direct_ingest_raw_data_resource_lock")
                )
            )
            assert len(locks) == 6
            assert all(l[0] for l in locks)

            # third time, no failures this time!
            should_fail = False

            result_three = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf={"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"},
                expected_failure_task_id_regexes=[],  # noooooone!
                expected_skipped_task_id_regexes=[
                    # branches that did not run
                    r".*_secondary_import_branch\..*",
                    r".us_ll_primary_import_branch\..*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result_three.dag_run_state)

            # conditions for success for rerun ending in failure:
            # (1) successful file stayed put, failed file stayed put
            assert set(self.fs.all_paths) == {
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv'
                ),
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv'
                ),
            }

            # (2) bq client has all the expected calls from last time
            assert self.load_bq_mock().load_table_from_cloud_storage.call_count == 1 + 3
            assert self.load_bq_mock().create_table_from_query.call_count == 3

            assert self.load_bq_mock().delete_from_table_async.call_count == 2
            assert self.load_bq_mock().delete_table.call_count == 6
            assert (
                self.load_bq_mock().insert_into_table_from_table_async.call_count == 2
            )

            # (3) file metadata dbs match what we expect
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
                    2,
                    2,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                ),
            }

            bq_metadata = session.execute(
                text(
                    "select update_datetime, file_processed_time from direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
            }

            import_runs = session.execute(
                text(
                    "select import_status, raw_rows from direct_ingest_raw_file_import"
                )
            )

            assert sorted(import_runs, key=lambda x: x[0]) == [
                ("FAILED_LOAD_STEP", None),
                ("FAILED_LOAD_STEP", None),
                ("SUCCEEDED", 100),
                ("SUCCEEDED", 100),
            ]

            # (4) resource locks are released
            locks = list(
                session.execute(
                    text("select released from direct_ingest_raw_data_resource_lock")
                )
            )
            assert len(locks) == 9
            assert all(l[0] for l in locks)
            self.dag_kick_off_mock.assert_not_called()

    def test_errors_during_append_step(self) -> None:

        self._load_fixture_data(
            gcsfs_direct_ingest_bucket_for_state(
                project_id=_PROJECT_ID,
                region_code="US_XX",
                ingest_instance=DirectIngestInstance.PRIMARY,
            ).bucket_name,
            "singlePrimaryKey",
            "tagCustomLineTerminatorNonUTF8",
        )

        should_fail = True

        def _fail_wrapper(**kwargs: Any) -> MagicMock:
            def _fail() -> None:
                raise ValueError("Error")

            mock = MagicMock()
            if (
                should_fail
                and kwargs["destination_address"].table_id
                == "tagCustomLineTerminatorNonUTF8"
            ):
                mock.result.side_effect = _fail

            return mock

        self.load_bq_mock().insert_into_table_from_table_async.side_effect = (
            _fail_wrapper
        )
        self.load_bq_mock().load_table_from_cloud_storage().output_rows = 100
        self.load_bq_mock().create_table_from_query().total_rows = 90

        with Session(bind=self.engine) as session:

            dag = self._create_dag()
            result = self.run_dag_test(
                dag,
                session=session,
                run_conf={"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"},
                expected_failure_task_id_regexes=[
                    "raw_data_branching.us_xx_primary_import_branch.big_query_load.raise_append_errors"
                ],
                expected_skipped_task_id_regexes=[
                    # branches that did not run
                    r".*_secondary_import_branch\..*",
                    r".us_ll_primary_import_branch\..*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # conditions for success for a single file and failure for another:
            # (1) successful file has moved to storage, failed file stayed put
            assert set(self.fs.all_paths) == {
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv'
                ),
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_bucket_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}/unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv'
                ),
            }

            # (2) bq client has all the expected calls
            assert self.load_bq_mock().load_table_from_cloud_storage.call_count == 1 + 2
            assert self.load_bq_mock().create_table_from_query.call_count == 1 + 2

            assert self.load_bq_mock().delete_from_table_async.call_count == 2
            # we should only delete the original temp table for the failed file
            # since it is deleted after the temp transformed table is created
            assert self.load_bq_mock().delete_table.call_count == (3 * 1) + (1 * 1)
            assert (
                self.load_bq_mock().insert_into_table_from_table_async.call_count == 2
            )

            # (3) file metadata dbs match what we expect
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
                    2,
                    2,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                ),
            }

            bq_metadata = session.execute(
                text(
                    "select update_datetime, file_processed_time from direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    None,
                ),
            }

            import_runs = session.execute(
                text(
                    "select import_status, raw_rows from direct_ingest_raw_file_import"
                )
            )

            assert sorted(import_runs, key=lambda x: x[0]) == [
                ("FAILED_LOAD_STEP", None),
                ("SUCCEEDED", 100),
            ]

            # (4) resource locks are released
            locks = list(
                session.execute(
                    text("select released from direct_ingest_raw_data_resource_lock")
                )
            )
            assert len(locks) == 3
            assert all(l[0] for l in locks)
            self.dag_kick_off_mock.assert_not_called()

            # rerun!! ------------

            result_two = self.run_dag_test(
                dag,
                session=session,
                run_conf={"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"},
                expected_failure_task_id_regexes=[
                    "raw_data_branching.us_xx_primary_import_branch.big_query_load.raise_append_errors"
                ],
                expected_skipped_task_id_regexes=[
                    # branches that did not run
                    r".*_secondary_import_branch\..*",
                    r".us_ll_primary_import_branch\..*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result_two.dag_run_state)

            # conditions for success for rerun ending in failure:
            # (1) successful file stayed put, failed file stayed put
            assert set(self.fs.all_paths) == {
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv'
                ),
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_bucket_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}/unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv'
                ),
            }

            # (2) bq client has all the expected calls
            assert (
                self.load_bq_mock().load_table_from_cloud_storage.call_count
                == 1 + 2 + 1
            )
            assert self.load_bq_mock().create_table_from_query.call_count == 1 + 2 + 1

            assert self.load_bq_mock().delete_from_table_async.call_count == 2 + 1
            assert self.load_bq_mock().delete_table.call_count == (3 * 1) + (1 * 2)
            assert (
                self.load_bq_mock().insert_into_table_from_table_async.call_count
                == 2 + 1
            )
            # (3) file metadata dbs match what we expect
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
                    2,
                    2,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                ),
            }

            bq_metadata = session.execute(
                text(
                    "select update_datetime, file_processed_time from direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    None,
                ),
            }

            import_runs = session.execute(
                text(
                    "select import_status, raw_rows from direct_ingest_raw_file_import"
                )
            )

            assert sorted(import_runs, key=lambda x: x[0]) == [
                ("FAILED_LOAD_STEP", None),
                ("FAILED_LOAD_STEP", None),
                ("SUCCEEDED", 100),
            ]

            # (4) resource locks are released
            locks = list(
                session.execute(
                    text("select released from direct_ingest_raw_data_resource_lock")
                )
            )
            assert len(locks) == 6
            assert all(l[0] for l in locks)
            self.dag_kick_off_mock.assert_not_called()

            # third time, no failures this time!
            should_fail = False

            result_three = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf={"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"},
                expected_failure_task_id_regexes=[],  # noooooone!
                expected_skipped_task_id_regexes=[
                    # branches that did not run
                    r".*_secondary_import_branch\..*",
                    r".us_ll_primary_import_branch\..*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result_three.dag_run_state)

            # conditions for success for rerun ending in failure:
            # (1) successful file stayed put, failed file stayed put
            assert set(self.fs.all_paths) == {
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_singlePrimaryKey.csv'
                ),
                GcsfsFilePath.from_absolute_path(
                    f'{gcsfs_direct_ingest_storage_directory_path_for_state(region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY).abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv'
                ),
            }

            # (2) bq client has all the expected calls
            assert (
                self.load_bq_mock().load_table_from_cloud_storage.call_count
                == 1 + 2 + 1 + 1
            )
            assert (
                self.load_bq_mock().create_table_from_query.call_count == 1 + 2 + 1 + 1
            )

            assert self.load_bq_mock().delete_from_table_async.call_count == 2 + 1 + 1
            assert self.load_bq_mock().delete_table.call_count == (3 * 2) + (1 * 2)
            assert (
                self.load_bq_mock().insert_into_table_from_table_async.call_count
                == 2 + 1 + 1
            )

            # (3) file metadata dbs match what we expect
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
                    2,
                    2,
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                ),
            }

            bq_metadata = session.execute(
                text(
                    "select update_datetime, file_processed_time from direct_ingest_raw_big_query_file_metadata"
                )
            )

            assert set(bq_metadata) == {
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
                (
                    datetime.datetime.fromisoformat("2024-01-25T16:35:33:617135Z"),
                    self.frozen_time,
                ),
            }

            import_runs = session.execute(
                text(
                    "select import_status, raw_rows from direct_ingest_raw_file_import"
                )
            )

            assert sorted(import_runs, key=lambda x: x[0]) == [
                ("FAILED_LOAD_STEP", None),
                ("FAILED_LOAD_STEP", None),
                ("SUCCEEDED", 100),
                ("SUCCEEDED", 100),
            ]

            # (4) resource locks are released
            locks = list(
                session.execute(
                    text("select released from direct_ingest_raw_data_resource_lock")
                )
            )
            assert len(locks) == 9
            assert all(l[0] for l in locks)
            self.dag_kick_off_mock.assert_not_called()
