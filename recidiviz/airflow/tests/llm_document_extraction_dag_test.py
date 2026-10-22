# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for the LLM document extraction DAG"""

import json
import os
from typing import Any
from unittest.mock import MagicMock, patch

from airflow.models import DAG, DagBag
from airflow.utils.context import Context
from airflow.utils.state import DagRunState
from sqlalchemy.orm import Session

from recidiviz.airflow.dags.monitoring.dag_registry import (
    get_llm_document_extraction_dag_id,
)
from recidiviz.airflow.dags.utils.constants import (
    AFTER_UPLOAD_NOOP_TASK_ID,
    BUILD_DOCUMENT_UPLOAD_POD_ARGUMENTS_TASK_ID,
    CHECK_HAS_JOB_TASK_ID,
    CHECK_HAS_UPDATES_TASK_ID,
    DOCUMENT_UPLOAD_TASK_ID,
    IDENTIFY_JOB_DOCUMENTS_TASK_ID,
    RECORD_DOCUMENT_UPLOAD_RESULTS_TASK_ID,
    RUN_DOCUMENT_DISCOVERY_TASK_ID,
    RUN_EXTRACTION_TASK_ID,
    SYNC_EXTRACTOR_METADATA_TASK_ID,
    UPDATE_BIG_QUERY_TABLE_SCHEMATA_TASK_ID,
    UPDATE_FIRST_ORDER_VIEWS_TASK_ID,
    VALIDATE_SOURCE_TABLE_DATASETS_TASK_ID,
)
from recidiviz.airflow.tests.test_utils import DAG_FOLDER, AirflowIntegrationTest
from recidiviz.airflow.tests.utils.dag_helper_functions import (
    fake_failing_operator_constructor,
    fake_operator_constructor,
)
from recidiviz.airflow.tests.utils.kubernetes_helper_functions import (
    fake_noop_kpo_partial,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    load_first_order_llm_extractor_configs,
)
from recidiviz.documents.store import (
    document_collection_config,
    document_collection_config_collectors,
)
from recidiviz.documents.store.document_collection_config import (
    TEMP_DOCUMENT_GENERATION_OUTPUT_TABLE_ID_PREFIX,
    TEMP_METADATA_UPDATES_TABLE_ID_PREFIX,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_UPLOAD_BATCH_NUM_COLUMN_NAME,
)
from recidiviz.tests.documents import fake_config as fake_config_module
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    patch_fake_entity_resolution_model_config_name,
)
from recidiviz.utils.types import assert_type

# Per-document-collection tasks defined inside each document collection's task group, in
# upstream-to-downstream order.
ALL_PER_DOCUMENT_COLLECTION_TASK_IDS: list[str] = [
    RUN_DOCUMENT_DISCOVERY_TASK_ID,
    CHECK_HAS_UPDATES_TASK_ID,
    BUILD_DOCUMENT_UPLOAD_POD_ARGUMENTS_TASK_ID,
    DOCUMENT_UPLOAD_TASK_ID,
    AFTER_UPLOAD_NOOP_TASK_ID,
    RECORD_DOCUMENT_UPLOAD_RESULTS_TASK_ID,
]

# Per-extractor-collection tasks defined inside each extractor collection's task group,
# in upstream-to-downstream order.
ALL_PER_EXTRACTOR_COLLECTION_TASK_IDS: list[str] = [
    SYNC_EXTRACTOR_METADATA_TASK_ID,
    IDENTIFY_JOB_DOCUMENTS_TASK_ID,
    CHECK_HAS_JOB_TASK_ID,
    RUN_EXTRACTION_TASK_ID,
    UPDATE_FIRST_ORDER_VIEWS_TASK_ID,
]

# Module globals whose `default_config_module` must be swapped to the fake config so the
# DAG enumerates the fake extractor collections.
_FAKE_EXTRACTOR_CONFIG_MODULE_PATHS: list[str] = [
    "recidiviz.documents.extraction.llm_extractor_config_collectors.default_config_module",
    "recidiviz.documents.extraction.models.llm_model_registry.default_config_module",
    "recidiviz.documents.extraction.models.llm_extractor_collection_config.default_config_module",
    "recidiviz.documents.extraction.models.reference_data.reference_data_registry.default_config_module",
]

# The single extractor collection the fake config defines, for US_XX only.
FAKE_EXTRACTOR_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"


class LlmDocumentExtractionDagTest(AirflowIntegrationTest):
    """Tests for the LLM document extraction DAG."""

    def setUp(self) -> None:
        super().setUp()

        self.project_id = "recidiviz-testing"

        self.dag_id = get_llm_document_extraction_dag_id(self.project_id)

        # State-level branches.
        self.us_xx_branch = "extraction.us_xx"
        self.us_yy_branch = "extraction.us_yy"

        # Per-document-collection branches under us_xx.
        self.us_xx_fake_case_notes_collection_branch = (
            f"{self.us_xx_branch}.document_collections.FAKE_CASE_NOTES"
        )
        self.us_xx_fake_input_notes_collection_branch = (
            f"{self.us_xx_branch}.document_collections.FAKE_INPUT_NOTES"
        )
        self.us_xx_fake_person_id_notes_collection_branch = (
            f"{self.us_xx_branch}.document_collections.FAKE_PERSON_ID_NOTES"
        )
        self.us_xx_fake_staff_id_reports_collection_branch = (
            f"{self.us_xx_branch}.document_collections.FAKE_STAFF_ID_REPORTS"
        )
        self.us_xx_fake_staff_reports_collection_branch = (
            f"{self.us_xx_branch}.document_collections.FAKE_STAFF_REPORTS"
        )

        # The single per-extractor-collection branch, defined only under us_xx
        self.us_xx_fake_extractor_collection_branch = f"{self.us_xx_branch}.extractor_collections.{FAKE_EXTRACTOR_COLLECTION_NAME}"

        # after_upload_noop (per-document-collection) and branch_end (document_collections)
        # are excluded from skip regexes because their ALL_DONE trigger rules let
        # them run even when their upstreams are skipped, so they succeed rather
        # than skipping.
        self.us_yy_all_skippable_tasks_regex = (
            rf"^{self.us_yy_branch}\..*(?<!after_upload_noop)(?<!branch_end)$"
        )
        self.us_yy_always_succeeds_tasks_regex = (
            rf"^{self.us_yy_branch}\.(?:.+\.)?(?:after_upload_noop|branch_end)$"
        )
        self.us_xx_other_document_collections_skippable_tasks_regexes = [
            rf"^{self.us_xx_fake_input_notes_collection_branch}\..*(?<!after_upload_noop)$",
        ]
        self.us_xx_other_document_collections_always_succeeds_tasks_regex = (
            rf"^{self.us_xx_branch}\.document_collections\."
            rf"(?!FAKE_CASE_NOTES\.)[^.]+\.after_upload_noop$"
        )
        # Combined helper for tests scoped to us_xx + fake_case_notes that need
        # to skip every other document collection across both states.
        self.non_target_document_collection_skipped_regexes = [
            self.us_yy_all_skippable_tasks_regex,
            *self.us_xx_other_document_collections_skippable_tasks_regexes,
        ]
        self.non_target_document_collection_always_succeeds_regexes = [
            self.us_yy_always_succeeds_tasks_regex,
            self.us_xx_other_document_collections_always_succeeds_tasks_regex,
        ]
        # The whole us_xx extraction subtree (the per-extractor-collection branch plus
        # its branching scaffolding).
        self.us_xx_extraction_subtree_regex = (
            rf"^{self.us_xx_branch}\.extractor_collections\..*$"
        )

        # Frame tasks (initialize, branching scaffolding, ALL_DONE noops in
        # non-target document collections) that succeed in any happy-path test scoped to
        # us_xx + fake_case_notes.
        self.us_xx_fake_case_notes_frame_success_regexes = [
            r"^initialize_dag\..*$",
            rf"^{UPDATE_BIG_QUERY_TABLE_SCHEMATA_TASK_ID}$",
            rf"^{VALIDATE_SOURCE_TABLE_DATASETS_TASK_ID}$",
            r"^extraction\.branch_(start|end)$",
            rf"^{self.us_xx_branch}\.document_collections\.branch_(start|end)$",
            self.us_xx_extraction_subtree_regex,
            *self.non_target_document_collection_always_succeeds_regexes,
        ]
        # Subset of the frame above for failure-path tests where branch_end
        # also fails. Excludes the branch_end tasks because they propagate the
        # failure rather than succeeding.
        self.us_xx_fake_case_notes_frame_success_regexes_excluding_branch_ends = [
            r"^initialize_dag\..*$",
            rf"^{UPDATE_BIG_QUERY_TABLE_SCHEMATA_TASK_ID}$",
            rf"^{VALIDATE_SOURCE_TABLE_DATASETS_TASK_ID}$",
            r"^extraction\.branch_start$",
            rf"^{self.us_xx_branch}\.document_collections\.branch_start$",
            *self.non_target_document_collection_always_succeeds_regexes,
        ]

        self.environment_patcher = patch.dict(
            os.environ, {"GCP_PROJECT": self.project_id}
        )
        self.environment_patcher.start()

        self.project_patcher = patch(
            "recidiviz.utils.metadata.project_id", return_value=self.project_id
        )
        self.project_patcher.start()

        self.config_module_patcher = patch.object(
            document_collection_config, "default_config_module", fake_config_module
        )
        self.config_module_patcher.start()

        self.collectors_config_module_patcher = patch.object(
            document_collection_config_collectors,
            "default_config_module",
            fake_config_module,
        )
        self.collectors_config_module_patcher.start()

        # Swap every extractor-config module to the fake config and clear the cached
        # loader so the DAG enumerates the fake extractor collections.
        for module_path in _FAKE_EXTRACTOR_CONFIG_MODULE_PATHS:
            self.enterContext(patch(module_path, fake_config_module))
        # get_extractor_config generates ER configs, whose builder's default model
        # config name is absent from the fake registry; point it at a fake-registry one.
        self.enterContext(patch_fake_entity_resolution_model_config_name())
        load_first_order_llm_extractor_configs.cache_clear()
        self.addCleanup(load_first_order_llm_extractor_configs.cache_clear)

        self.mock_extraction_session = MagicMock()
        self.extraction_session_patcher = patch(
            "recidiviz.airflow.dags.llm_document_extraction.extraction_tasks."
            "airflow_cloud_sql_session_for_connection",
        )
        self.mock_extraction_session_cm = self.extraction_session_patcher.start()
        self.mock_extraction_session_cm.return_value.__enter__.return_value = (
            self.mock_extraction_session
        )

        self.sync_extractor_metadata_patcher = patch(
            "recidiviz.airflow.dags.llm_document_extraction.extraction_tasks."
            "sync_extractor_metadata",
            return_value="fake-extractor-version-id",
        )
        self.mock_sync_extractor_metadata = self.sync_extractor_metadata_patcher.start()

        self.identify_job_documents_patcher = patch(
            "recidiviz.airflow.dags.llm_document_extraction.extraction_tasks."
            "identify_job_documents",
            return_value="fake-job-id",
        )
        self.mock_identify_job_documents = self.identify_job_documents_patcher.start()

        self.mock_bq_client = MagicMock()
        self._mock_bq_client_no_updates()
        self.bq_client_patcher = patch(
            "recidiviz.airflow.dags.llm_document_extraction.document_store_tasks.BigQueryClientImpl",
            return_value=self.mock_bq_client,
        )
        self.bq_client_patcher.start()

        self.kpo_patcher = patch(
            "recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator.RecidivizKubernetesPodOperator",
            side_effect=fake_operator_constructor,
        )
        self.mock_kpo_constructor = self.kpo_patcher.start()
        self.mock_kpo_constructor.partial.side_effect = fake_noop_kpo_partial()

    def tearDown(self) -> None:
        self.environment_patcher.stop()
        self.project_patcher.stop()
        self.config_module_patcher.stop()
        self.collectors_config_module_patcher.stop()
        self.extraction_session_patcher.stop()
        self.sync_extractor_metadata_patcher.stop()
        self.identify_job_documents_patcher.stop()
        self.bq_client_patcher.stop()
        self.kpo_patcher.stop()
        super().tearDown()

    @staticmethod
    def _row_iterator_mock(total_rows: int) -> MagicMock:
        row_iterator = MagicMock()
        row_iterator.total_rows = total_rows
        return row_iterator

    @staticmethod
    def _query_job_mock(
        *,
        num_dml_affected_rows: int | None = None,
        rows: list[dict] | None = None,
    ) -> MagicMock:
        query_job = MagicMock()
        result = MagicMock()
        result.num_dml_affected_rows = num_dml_affected_rows
        result.__iter__ = lambda _self: iter(rows or [])
        query_job.result.return_value = result
        return query_job

    @staticmethod
    def _run_conf(
        *,
        state_code_filter: str | None = None,
        document_collection_name_filter: str | None = None,
    ) -> dict[str, str]:
        conf: dict[str, str] = {}
        if state_code_filter is not None:
            conf["state_code_filter"] = state_code_filter
        if document_collection_name_filter is not None:
            conf["document_collection_name_filter"] = document_collection_name_filter
        return conf

    def _mock_bq_client_no_updates(self) -> None:
        self._mock_bq_client(
            metadata_rows=0,
            content_rows=0,
            batch_numbers=[],
            recorder_rows_inserted=0,
        )

    def _mock_bq_client_has_updates(self, *, rows_inserted: int = 10) -> None:
        self._mock_bq_client(
            metadata_rows=10,
            content_rows=5,
            batch_numbers=[0, 1],
            recorder_rows_inserted=rows_inserted,
        )

    def _mock_bq_client(
        self,
        *,
        metadata_rows: int,
        content_rows: int,
        batch_numbers: list[int],
        recorder_rows_inserted: int,
    ) -> None:
        """Wires up the mock BQ client end-to-end for a single DAG run."""

        def _create_table_side_effect(*, address: Any, **_kwargs: Any) -> MagicMock:
            if address.table_id.startswith(
                TEMP_DOCUMENT_GENERATION_OUTPUT_TABLE_ID_PREFIX
            ):
                return MagicMock()
            if address.table_id.startswith(TEMP_METADATA_UPDATES_TABLE_ID_PREFIX):
                return self._row_iterator_mock(metadata_rows)
            if "_document_contents_" in address.table_id:
                return self._row_iterator_mock(content_rows)
            raise ValueError(
                f"Unexpected create_table_from_query address [{address.to_str()}]"
            )

        self.mock_bq_client.create_table_from_query.side_effect = (
            _create_table_side_effect
        )

        def _run_query_side_effect(*, query_str: str, **_kwargs: Any) -> MagicMock:
            if "INSERT INTO" in query_str:
                return self._query_job_mock(
                    num_dml_affected_rows=recorder_rows_inserted
                )
            return self._query_job_mock(
                rows=[{DOCUMENT_UPLOAD_BATCH_NUM_COLUMN_NAME: n} for n in batch_numbers]
            )

        self.mock_bq_client.run_query_async.side_effect = _run_query_side_effect

    def _create_dag(self) -> DAG:
        return DagBag(dag_folder=DAG_FOLDER, include_examples=False).dags[self.dag_id]

    def _us_xx_fake_case_notes_collection_branch_regex(self, task_name: str) -> str:
        return rf"^{self.us_xx_fake_case_notes_collection_branch}\.{task_name}$"

    def _get_insert_query_strs(self) -> list[str]:
        return [
            c.kwargs["query_str"]
            for c in self.mock_bq_client.run_query_async.call_args_list
            if "INSERT INTO" in c.kwargs["query_str"]
        ]

    def test_source_table_validation_is_non_blocking_side_branch(self) -> None:
        """Validation runs after the schema update (so the group's tables exist) but is
        a leaf: nothing downstream depends on it, so a validation failure surfaces the
        run as failed without blocking the extraction branches."""
        dag = self._create_dag()
        self.assertIn(VALIDATE_SOURCE_TABLE_DATASETS_TASK_ID, dag.task_ids)
        validate_task = dag.get_task(VALIDATE_SOURCE_TABLE_DATASETS_TASK_ID)
        self.assertEqual(
            {UPDATE_BIG_QUERY_TABLE_SCHEMATA_TASK_ID},
            validate_task.upstream_task_ids,
        )
        self.assertEqual(set(), validate_task.downstream_task_ids)

    def test_no_discovery_results(self) -> None:
        """When discovery finds no metadata updates, check_has_updates
        short-circuits and all downstream tasks for the document collection are
        skipped."""
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf=self._run_conf(
                    state_code_filter=StateCode.US_XX.value,
                    document_collection_name_filter="FAKE_CASE_NOTES",
                ),
                expected_failure_task_id_regexes=[],
                expected_skipped_task_id_regexes=[
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        BUILD_DOCUMENT_UPLOAD_POD_ARGUMENTS_TASK_ID
                    ),
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        DOCUMENT_UPLOAD_TASK_ID
                    ),
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        AFTER_UPLOAD_NOOP_TASK_ID
                    ),
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        RECORD_DOCUMENT_UPLOAD_RESULTS_TASK_ID
                    ),
                    *self.non_target_document_collection_skipped_regexes,
                ],
                expected_success_task_id_regexes=[
                    *self.us_xx_fake_case_notes_frame_success_regexes,
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        RUN_DOCUMENT_DISCOVERY_TASK_ID
                    ),
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        CHECK_HAS_UPDATES_TASK_ID
                    ),
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # Only the generation-output and metadata-updates temp tables are
            # written; the new-contents step is short-circuited when there are
            # no metadata updates.
            self.assertEqual(self.mock_bq_client.create_table_from_query.call_count, 2)
            self.mock_bq_client.run_query_async.assert_not_called()
            self.mock_bq_client.load_table_from_cloud_storage.assert_not_called()

    def test_discovery_results_run_end_to_end(self) -> None:
        self._mock_bq_client_has_updates()

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf=self._run_conf(
                    state_code_filter=StateCode.US_XX.value,
                    document_collection_name_filter="FAKE_CASE_NOTES",
                ),
                expected_failure_task_id_regexes=[],
                expected_skipped_task_id_regexes=self.non_target_document_collection_skipped_regexes,
                expected_success_task_id_regexes=[
                    *self.us_xx_fake_case_notes_frame_success_regexes,
                    rf"^{self.us_xx_fake_case_notes_collection_branch}\..*$",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # Result recorder ran two INSERTs: one to hydrate the document collection's
            # document_contents table and one to append to the document collection's
            # metadata table.
            insert_queries = self._get_insert_query_strs()
            self.assertEqual(len(insert_queries), 2)

            # Status CSV load happened once (content_rows > 0).
            self.assertEqual(
                self.mock_bq_client.load_table_from_cloud_storage.call_count, 1
            )

            # All rows inserted, so all three temp tables for the document
            # collection are deleted: the generation-output table by discovery,
            # the metadata updates and new contents tables by the recorder.
            self.assertEqual(self.mock_bq_client.delete_table.call_count, 3)

    def test_no_filters_runs_all_states_and_document_collections(self) -> None:
        """Without any filter, every (state, document collection) branch launched in
        this project runs to completion. With the default 'no updates' mock, each
        document collection's discovery succeeds and check_has_updates short-circuits
        the rest of its branch."""
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf=self._run_conf(),
                expected_failure_task_id_regexes=[],
                expected_skipped_task_id_regexes=[
                    # check_has_updates short-circuits the rest of each branch
                    # because metadata_rows=0.
                    rf"^extraction\.[^.]+\.document_collections\."
                    rf"[^.]+\."
                    rf"(?:{BUILD_DOCUMENT_UPLOAD_POD_ARGUMENTS_TASK_ID}|"
                    rf"{DOCUMENT_UPLOAD_TASK_ID}|"
                    rf"{AFTER_UPLOAD_NOOP_TASK_ID}|"
                    rf"{RECORD_DOCUMENT_UPLOAD_RESULTS_TASK_ID})$",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # Two create_table_from_query calls per document collection (the
            # generation-output and metadata-updates temp tables; 0 metadata
            # rows → no contents call). 2 launched US_XX document collections + 1
            # launched US_YY document collection = 3 collections × 2 = 6.
            self.assertEqual(self.mock_bq_client.create_table_from_query.call_count, 6)
            self.mock_bq_client.run_query_async.assert_not_called()
            self.mock_bq_client.load_table_from_cloud_storage.assert_not_called()

    def test_discovery_failure_skips_remaining(self) -> None:
        """If discovery fails, build_args/upload/record upstream-fail and the
        recorder is never invoked. after_upload_noop runs but has no work to do.
        """
        self.mock_bq_client.create_table_from_query.side_effect = ValueError(
            "discovery failed"
        )

        with Session(bind=self.engine) as session:
            self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf=self._run_conf(
                    state_code_filter=StateCode.US_XX.value,
                    document_collection_name_filter="FAKE_CASE_NOTES",
                ),
                expected_failure_task_id_regexes=[
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        RUN_DOCUMENT_DISCOVERY_TASK_ID
                    ),
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        CHECK_HAS_UPDATES_TASK_ID
                    ),
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        BUILD_DOCUMENT_UPLOAD_POD_ARGUMENTS_TASK_ID
                    ),
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        DOCUMENT_UPLOAD_TASK_ID
                    ),
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        RECORD_DOCUMENT_UPLOAD_RESULTS_TASK_ID
                    ),
                    rf"^{self.us_xx_branch}\.document_collections\.branch_end$",
                    r"^extraction\.branch_end$",
                    # Extraction runs after the whole document-collections branching, so
                    # the failed branch_end upstream-fails the entire extraction subtree.
                    self.us_xx_extraction_subtree_regex,
                ],
                expected_skipped_task_id_regexes=self.non_target_document_collection_skipped_regexes,
                expected_success_task_id_regexes=[
                    *self.us_xx_fake_case_notes_frame_success_regexes_excluding_branch_ends,
                    # after_upload_noop runs (ALL_DONE), but with no work to do.
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        AFTER_UPLOAD_NOOP_TASK_ID
                    ),
                ],
            )
            self.mock_bq_client.run_query_async.assert_not_called()
            self.mock_bq_client.load_table_from_cloud_storage.assert_not_called()

    def test_build_upload_pod_arguments_filters_empty_task_instances(self) -> None:
        """build_document_upload_pod_arguments should emit one argv list per
        non-empty task-instance batch. With 2 batches distributed across 10
        task instances, 8 instances will be empty and must be filtered out."""
        self._mock_bq_client_has_updates()

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf=self._run_conf(
                    state_code_filter=StateCode.US_XX.value,
                    document_collection_name_filter="FAKE_CASE_NOTES",
                ),
                expected_failure_task_id_regexes=[],
                expected_skipped_task_id_regexes=self.non_target_document_collection_skipped_regexes,
                expected_success_task_id_regexes=[
                    *self.us_xx_fake_case_notes_frame_success_regexes,
                    rf"^{self.us_xx_fake_case_notes_collection_branch}\..*$",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            argv_xcom = self.get_xcom_for_task_id(
                f"{self.us_xx_fake_case_notes_collection_branch}."
                f"{BUILD_DOCUMENT_UPLOAD_POD_ARGUMENTS_TASK_ID}",
                session=session,
            )
            argv_lists = json.loads(assert_type(argv_xcom, bytes))
            # 2 non-empty batches → 2 argv lists.
            self.assertEqual(len(argv_lists), 2)
            for argv in argv_lists:
                self.assertIn("--entrypoint=DocumentUploadEntrypoint", argv)
                self.assertIn(f"--state_code={StateCode.US_XX.value}", argv)
                self.assertTrue(
                    any("FAKE_CASE_NOTES" in arg for arg in argv),
                    f"Expected document collection name in argv, got [{argv}]",
                )

    def test_initialize_failure_skips_everything(self) -> None:
        """When a task in initialize_dag fails, all remaining tasks are skipped."""
        with patch(
            "recidiviz.airflow.dags.llm_document_extraction.initialize_llm_document_extraction_dag_group.verify_parameters.function",
            side_effect=ValueError("bad params"),
        ):
            with Session(bind=self.engine) as session:
                self.run_dag_test(
                    self._create_dag(),
                    session=session,
                    run_conf=self._run_conf(
                        state_code_filter=StateCode.US_XX.value,
                        document_collection_name_filter="FAKE_CASE_NOTES",
                    ),
                    expected_failure_task_id_regexes=[
                        r"^initialize_dag\.verify_parameters$",
                    ],
                    expected_skipped_task_id_regexes=[
                        r"^initialize_dag\.(record_dag_run_metadata|"
                        r"wait_to_continue_or_cancel|handle_queueing_result)$",
                        rf"^{UPDATE_BIG_QUERY_TABLE_SCHEMATA_TASK_ID}$",
                        rf"^{VALIDATE_SOURCE_TABLE_DATASETS_TASK_ID}$",
                        r"^extraction\..*$",
                    ],
                    expected_success_task_id_regexes=[
                        # handle_params_check is ALL_DONE; it runs and re-raises.
                        r"^initialize_dag\.handle_params_check$",
                    ],
                )
                self.mock_bq_client.create_table_from_query.assert_not_called()
                self.mock_bq_client.run_query_async.assert_not_called()

    def test_schema_update_failure_skips_branches(self) -> None:
        """When the schema-update task fails, the extraction branching that depends on
        it does not run: every branching task upstream-fails, except the ALL_DONE
        after_upload_noop tasks, which still run and succeed."""
        self.mock_kpo_constructor.side_effect = lambda **kwargs: (
            fake_failing_operator_constructor(**kwargs)
            if kwargs["task_id"] == UPDATE_BIG_QUERY_TABLE_SCHEMATA_TASK_ID
            else fake_operator_constructor(**kwargs)
        )

        with Session(bind=self.engine) as session:
            self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf=self._run_conf(
                    state_code_filter=StateCode.US_XX.value,
                    document_collection_name_filter="FAKE_CASE_NOTES",
                ),
                expected_failure_task_id_regexes=[
                    rf"^{UPDATE_BIG_QUERY_TABLE_SCHEMATA_TASK_ID}$",
                    # The validation side branch depends on the schema update, so it
                    # upstream-fails alongside the branches.
                    rf"^{VALIDATE_SOURCE_TABLE_DATASETS_TASK_ID}$",
                    # Everything under extraction upstream-fails except the
                    # ALL_DONE after_upload_noop tasks below. The branch_end scaffolding
                    # tasks also upstream-fail (their inputs failed rather than skipped).
                    r"^extraction\.(?!.*after_upload_noop$).*$",
                ],
                expected_skipped_task_id_regexes=[],
                expected_success_task_id_regexes=[
                    r"^initialize_dag\..*$",
                    r"^extraction\..*after_upload_noop$",
                ],
            )
            self.mock_bq_client.create_table_from_query.assert_not_called()
            self.mock_bq_client.run_query_async.assert_not_called()

    def test_metadata_updates_only_run_end_to_end(self) -> None:
        """When discovery finds metadata updates but no new document contents, the
        upload step is short-circuited but the recorder still runs and successfully inserts
        the metadata rows."""
        self._mock_bq_client(
            metadata_rows=5,
            content_rows=0,
            batch_numbers=[],
            recorder_rows_inserted=5,
        )

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf=self._run_conf(
                    state_code_filter=StateCode.US_XX.value,
                    document_collection_name_filter="FAKE_CASE_NOTES",
                ),
                expected_failure_task_id_regexes=[],
                expected_skipped_task_id_regexes=[
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        DOCUMENT_UPLOAD_TASK_ID
                    ),
                    *self.non_target_document_collection_skipped_regexes,
                ],
                expected_success_task_id_regexes=[
                    *self.us_xx_fake_case_notes_frame_success_regexes,
                    rf"^{self.us_xx_fake_case_notes_collection_branch}\."
                    rf"(?!{DOCUMENT_UPLOAD_TASK_ID}$)[^.]+$",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # Only the metadata INSERT ran; document_contents hydration was
            # skipped because num_new_document_contents_rows == 0.
            insert_queries = self._get_insert_query_strs()
            self.assertEqual(len(insert_queries), 1)
            self.assertNotIn("_document_contents`", insert_queries[0])

            # No upload status CSVs to load.
            self.mock_bq_client.load_table_from_cloud_storage.assert_not_called()

            # Full success: discovery deleted the generation-output temp table
            # and the recorder deleted the metadata updates and new contents
            # temp tables.
            self.assertEqual(self.mock_bq_client.delete_table.call_count, 3)

    def test_state_filter_fans_out_to_all_document_collections(self) -> None:
        """state_code_filter alone runs all document collections launched for the state.
        Since in the tests we are defaulting to 'no updates', each document collection's
        discovery runs once and check_has_updates short-circuits."""
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf=self._run_conf(state_code_filter=StateCode.US_XX.value),
                expected_failure_task_id_regexes=[],
                expected_skipped_task_id_regexes=[
                    self.us_yy_all_skippable_tasks_regex,
                    rf"^{self.us_xx_branch}\.document_collections\."
                    r"[^.]+\."
                    rf"(?:{BUILD_DOCUMENT_UPLOAD_POD_ARGUMENTS_TASK_ID}|{DOCUMENT_UPLOAD_TASK_ID}|"
                    rf"{AFTER_UPLOAD_NOOP_TASK_ID}|{RECORD_DOCUMENT_UPLOAD_RESULTS_TASK_ID})$",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # Two create_table_from_query calls per document collection (the
            # generation-output and metadata-updates temp tables; 0 metadata
            # rows → no contents call). The fake config launches 2 US_XX document
            # collections under staging, so 2 × 2 = 4.
            self.assertEqual(self.mock_bq_client.create_table_from_query.call_count, 4)
            self.mock_bq_client.run_query_async.assert_not_called()
            self.mock_bq_client.load_table_from_cloud_storage.assert_not_called()

    def test_unknown_document_collection_name_filter_fails_branching(self) -> None:
        with Session(bind=self.engine) as session:
            self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf=self._run_conf(
                    state_code_filter=StateCode.US_XX.value,
                    document_collection_name_filter="does_not_exist",
                ),
                expected_failure_task_id_regexes=[
                    rf"^{self.us_xx_branch}\.document_collections\.branch_start$",
                    rf"^{self.us_xx_branch}\.document_collections\.branch_end$",
                    r"^extraction\.branch_end$",
                    # Every task in every us_xx document collection branch becomes
                    # upstream_failed except after_upload_noop, which is an
                    # ALL_DONE EmptyOperator with no body and succeeds.
                    rf"^{self.us_xx_branch}\.document_collections\."
                    rf"[^.]+\.(?!{AFTER_UPLOAD_NOOP_TASK_ID}$)[^.]+$",
                    # Extraction runs after the whole document-collections branching, so
                    # the failed branch_end upstream-fails the entire extraction subtree.
                    self.us_xx_extraction_subtree_regex,
                ],
                expected_skipped_task_id_regexes=[self.us_yy_all_skippable_tasks_regex],
                expected_success_task_id_regexes=[
                    r"^initialize_dag\..*$",
                    rf"^{UPDATE_BIG_QUERY_TABLE_SCHEMATA_TASK_ID}$",
                    rf"^{VALIDATE_SOURCE_TABLE_DATASETS_TASK_ID}$",
                    r"^extraction\.branch_start$",
                    # All us_xx document collection branches' after_upload_noop tasks
                    # succeed (ALL_DONE).
                    rf"^{self.us_xx_branch}\.document_collections\."
                    rf"[^.]+\.{AFTER_UPLOAD_NOOP_TASK_ID}$",
                    self.us_yy_always_succeeds_tasks_regex,
                ],
            )
            self.mock_bq_client.create_table_from_query.assert_not_called()
            self.mock_bq_client.run_query_async.assert_not_called()

    def test_document_collection_filter_without_state_filter_fails_validation(
        self,
    ) -> None:
        """verify_parameters' raises when document_collection_name_filter is supplied without
        state_code_filter (document collection names are only unique within a state)."""
        with Session(bind=self.engine) as session:
            self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf=self._run_conf(
                    document_collection_name_filter="FAKE_CASE_NOTES"
                ),
                expected_failure_task_id_regexes=[
                    r"^initialize_dag\.verify_parameters$",
                ],
                expected_skipped_task_id_regexes=[
                    r"^initialize_dag\.(record_dag_run_metadata|"
                    r"wait_to_continue_or_cancel|handle_queueing_result)$",
                    rf"^{UPDATE_BIG_QUERY_TABLE_SCHEMATA_TASK_ID}$",
                    rf"^{VALIDATE_SOURCE_TABLE_DATASETS_TASK_ID}$",
                    r"^extraction\..*$",
                ],
                expected_success_task_id_regexes=[
                    # handle_params_check is ALL_DONE; it runs and re-raises.
                    r"^initialize_dag\.handle_params_check$",
                ],
            )
            self.mock_bq_client.create_table_from_query.assert_not_called()
            self.mock_bq_client.run_query_async.assert_not_called()

    def test_upload_failure_still_records_with_partial_success(self) -> None:
        """When the upload task fails (per-document error or pod crash both
        raise from the entrypoint), the ALL_DONE after_upload_noop barrier
        absorbs the failure and record_document_upload_results still runs.

        Recorder inserts a partial row count (fewer than the discovery
        expected), so the document collection's temp tables are preserved for
        debugging rather than deleted."""
        # Partial success: fewer rows inserted than the discovery expected.
        self._mock_bq_client_has_updates(rows_inserted=7)

        def _fail(_context: Context) -> None:
            raise ValueError("upload failed")

        # Make the mapped document_upload task fail; the constructor path (used by the
        # schema task) keeps succeeding.
        self.mock_kpo_constructor.partial.side_effect = fake_noop_kpo_partial(
            execute_fn=_fail
        )
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf=self._run_conf(
                    state_code_filter=StateCode.US_XX.value,
                    document_collection_name_filter="FAKE_CASE_NOTES",
                ),
                expected_failure_task_id_regexes=[
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        DOCUMENT_UPLOAD_TASK_ID
                    ),
                ],
                expected_skipped_task_id_regexes=self.non_target_document_collection_skipped_regexes,
                expected_success_task_id_regexes=[
                    *self.us_xx_fake_case_notes_frame_success_regexes,
                    rf"^{self.us_xx_fake_case_notes_collection_branch}\."
                    rf"(?!{DOCUMENT_UPLOAD_TASK_ID}$)[^.]+$",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # Recorder still ran both INSERTs (document_contents + metadata)
            # for the document collection.
            insert_queries = self._get_insert_query_strs()
            self.assertEqual(len(insert_queries), 2)

            # Partial-success path: the recorder preserves its temp tables
            # for debugging. Discovery itself succeeded, so it already
            # deleted the generation-output temp table.
            self.assertEqual(self.mock_bq_client.delete_table.call_count, 1)

    def _constructed_pod_arguments(self, task_id: str) -> list[str]:
        """Returns the argv the DAG constructed the KPO named |task_id| with."""
        for call in self.mock_kpo_constructor.call_args_list:
            if call.kwargs.get("task_id") == task_id:
                return assert_type(call.kwargs["arguments"], list)
        raise ValueError(f"No KPO constructed with task_id [{task_id}].")

    def test_extraction_runs_when_job_is_generated(self) -> None:
        """When identify_job_documents returns a job, check_has_job proceeds and both the
        extraction pod and the first-order view update pod run."""
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf=self._run_conf(state_code_filter=StateCode.US_XX.value),
                expected_failure_task_id_regexes=[],
                expected_skipped_task_id_regexes=[
                    self.us_yy_all_skippable_tasks_regex,
                    # Each document collection's upload is short-circuited (no updates),
                    # but that does not gate extraction.
                    rf"^{self.us_xx_branch}\.document_collections\."
                    r"[^.]+\."
                    rf"(?:{BUILD_DOCUMENT_UPLOAD_POD_ARGUMENTS_TASK_ID}|{DOCUMENT_UPLOAD_TASK_ID}|"
                    rf"{AFTER_UPLOAD_NOOP_TASK_ID}|{RECORD_DOCUMENT_UPLOAD_RESULTS_TASK_ID})$",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            self.mock_sync_extractor_metadata.assert_called_once()
            self.mock_identify_job_documents.assert_called_once()

            run_extraction_argv = self._constructed_pod_arguments(
                RUN_EXTRACTION_TASK_ID
            )
            self.assertIn(
                "--entrypoint=LLMDocumentExtractionSyncJobEntrypoint",
                run_extraction_argv,
            )
            self.assertIn(f"--state_code={StateCode.US_XX.value}", run_extraction_argv)
            self.assertIn(
                f"--extractor_collection_name={FAKE_EXTRACTOR_COLLECTION_NAME}",
                run_extraction_argv,
            )
            # The job id is pulled from the identify task's XCom at pod render time.
            self.assertIn(
                f"--job_id={{{{ task_instance.xcom_pull(task_ids="
                f"'{self.us_xx_fake_extractor_collection_branch}."
                f"{IDENTIFY_JOB_DOCUMENTS_TASK_ID}') }}}}",
                run_extraction_argv,
            )

            update_views_argv = self._constructed_pod_arguments(
                UPDATE_FIRST_ORDER_VIEWS_TASK_ID
            )
            self.assertIn(
                "--entrypoint=UpdateLLMExtractionManagedViewGraphEntrypoint",
                update_views_argv,
            )
            self.assertIn(f"--state_code={StateCode.US_XX.value}", update_views_argv)
            self.assertIn(
                f"--extractor_collection_name={FAKE_EXTRACTOR_COLLECTION_NAME}",
                update_views_argv,
            )
            self.assertIn("--stage=first_order", update_views_argv)

    def test_extraction_short_circuits_when_no_job_is_generated(self) -> None:
        """When identify_job_documents returns None (no documents need processing),
        check_has_job short-circuits and the extraction and view update pods are skipped.
        """
        self.mock_identify_job_documents.return_value = None

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf=self._run_conf(
                    state_code_filter=StateCode.US_XX.value,
                    document_collection_name_filter="FAKE_CASE_NOTES",
                ),
                expected_failure_task_id_regexes=[],
                expected_skipped_task_id_regexes=[
                    *self.non_target_document_collection_skipped_regexes,
                    # check_has_updates short-circuits the (unfiltered) case-notes upload.
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        BUILD_DOCUMENT_UPLOAD_POD_ARGUMENTS_TASK_ID
                    ),
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        DOCUMENT_UPLOAD_TASK_ID
                    ),
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        AFTER_UPLOAD_NOOP_TASK_ID
                    ),
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        RECORD_DOCUMENT_UPLOAD_RESULTS_TASK_ID
                    ),
                    # check_has_job short-circuits the extraction and view update pods.
                    rf"^{self.us_xx_fake_extractor_collection_branch}\."
                    rf"(?:{RUN_EXTRACTION_TASK_ID}|{UPDATE_FIRST_ORDER_VIEWS_TASK_ID})$",
                ],
                expected_success_task_id_regexes=[
                    # Frame successes, but NOT the whole extraction subtree (part of it
                    # is short-circuited below), so they are listed explicitly here
                    # rather than via us_xx_fake_case_notes_frame_success_regexes.
                    r"^initialize_dag\..*$",
                    rf"^{UPDATE_BIG_QUERY_TABLE_SCHEMATA_TASK_ID}$",
                    rf"^{VALIDATE_SOURCE_TABLE_DATASETS_TASK_ID}$",
                    r"^extraction\.branch_(start|end)$",
                    rf"^{self.us_xx_branch}\.document_collections\.branch_(start|end)$",
                    rf"^{self.us_xx_branch}\.extractor_collections\.branch_(start|end)$",
                    *self.non_target_document_collection_always_succeeds_regexes,
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        RUN_DOCUMENT_DISCOVERY_TASK_ID
                    ),
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        CHECK_HAS_UPDATES_TASK_ID
                    ),
                    # sync, identify, and the short-circuit itself run and succeed.
                    rf"^{self.us_xx_fake_extractor_collection_branch}\."
                    rf"(?:{SYNC_EXTRACTOR_METADATA_TASK_ID}|{IDENTIFY_JOB_DOCUMENTS_TASK_ID}|"
                    rf"{CHECK_HAS_JOB_TASK_ID})$",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)
