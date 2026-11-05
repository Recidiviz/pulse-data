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
import re
from typing import Any
from unittest.mock import MagicMock, patch

from airflow.models import DAG, DagBag
from airflow.utils.context import Context
from airflow.utils.state import DagRunState
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy.orm import Session

from recidiviz.airflow.dags.monitoring.dag_registry import (
    get_llm_document_extraction_dag_id,
)
from recidiviz.airflow.dags.utils.constants import (
    AFTER_UPLOAD_NOOP_TASK_ID,
    BUILD_DOCUMENT_UPLOAD_POD_ARGUMENTS_TASK_ID,
    CHECK_EXTRACTION_FAILURES_TASK_ID,
    CHECK_HAS_JOB_TASK_ID,
    CHECK_HAS_UPDATES_TASK_ID,
    DOCUMENT_UPLOAD_TASK_ID,
    IDENTIFY_JOB_DOCUMENTS_TASK_ID,
    RECORD_DOCUMENT_UPLOAD_RESULTS_TASK_ID,
    RUN_DOCUMENT_DISCOVERY_TASK_ID,
    RUN_EXTRACTION_TASK_ID,
    SYNC_EXTRACTOR_METADATA_TASK_ID,
    UPDATE_BIG_QUERY_TABLE_SCHEMATA_TASK_ID,
    UPDATE_ENTITY_RESOLUTION_VIEWS_TASK_ID,
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
from recidiviz.documents.extraction.entity_resolution.entity_resolution_collection_names import (
    entity_resolution_collection_name,
    entity_resolution_collection_name_suffix,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_entry_source_map_table import (
    ENTRY_SOURCE_MAP_TABLE_ID_SUFFIX,
)
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
    CHECK_EXTRACTION_FAILURES_TASK_ID,
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

        # The two source-table tasks are nested in a task group at the DAG top level.
        self.update_schemata_task = (
            f"update_source_tables.{UPDATE_BIG_QUERY_TABLE_SCHEMATA_TASK_ID}"
        )
        self.validate_source_tables_task = (
            f"update_source_tables.{VALIDATE_SOURCE_TABLE_DATASETS_TASK_ID}"
        )

        # State-level branches live directly at the DAG top level (no wrapping group).
        self.us_xx_branch = "us_xx"
        self.us_yy_branch = "us_yy"

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
        # The FAKE_EXTRACTOR_COLLECTION declares three entity groups, so its branch nests
        # three entity-resolution sub-branches. An ER sub-branch's task statuses vary by
        # scenario, so the subtree regex below excludes the whole ER sub-branch and each
        # test matches its three behavior groups (below) explicitly.
        #
        # 1. Discovery + check_has_updates always run and succeed once the first-order
        #    view deploy they run after has succeeded (they rebuild the entry→source map
        #    regardless of whether any new composites were found).
        er_branch_regex = (
            rf"^{self.us_xx_fake_extractor_collection_branch}\."
            rf"[^.]+_ENTITY_RESOLUTION\."
        )
        self.us_xx_entity_resolution_always_succeeds_regex = (
            rf"{er_branch_regex}"
            rf"(?:{RUN_DOCUMENT_DISCOVERY_TASK_ID}|{CHECK_HAS_UPDATES_TASK_ID})$"
        )
        # 2. The upload path (build_args/upload/record) and the ALL_DONE after_upload_noop
        #    skip when composite discovery finds no new documents (the 'no updates' mock),
        #    mirroring the first-order upload path, and run when it does. (check_has_updates
        #    actively skips after_upload_noop via its explicit edge when it short-circuits.)
        self.us_xx_entity_resolution_upload_path_regex = (
            rf"{er_branch_regex}"
            rf"(?:{BUILD_DOCUMENT_UPLOAD_POD_ARGUMENTS_TASK_ID}|{DOCUMENT_UPLOAD_TASK_ID}|"
            rf"{AFTER_UPLOAD_NOOP_TASK_ID}|{RECORD_DOCUMENT_UPLOAD_RESULTS_TASK_ID})$"
        )
        # 3. The extraction (sync/identify/check_has_job/run) runs directly after
        #    record_composites, so it skips whenever the upload path skips (no new
        #    composites, or the first-order deploy upstream was skipped) and runs when it
        #    does not. The entity_resolution view deploy is NOT part of an ER sub-branch:
        #    it is a single task at the extractor-branch level, matched separately below.
        self.us_xx_entity_resolution_extraction_regex = (
            rf"{er_branch_regex}"
            rf"(?:{SYNC_EXTRACTOR_METADATA_TASK_ID}|{IDENTIFY_JOB_DOCUMENTS_TASK_ID}|"
            rf"{CHECK_HAS_JOB_TASK_ID}|{RUN_EXTRACTION_TASK_ID}|"
            rf"{CHECK_EXTRACTION_FAILURES_TASK_ID})$"
        )
        # The single entity_resolution view deploy for the extractor. The ER view graph
        # covers all the extractor's entity groups together, so it is deployed once, at the
        # extractor-branch level, downstream of every ER sub-branch's run_extraction (with
        # NONE_FAILED_MIN_ONE_SUCCESS). It succeeds when at least one group extracts, and
        # skips when every group short-circuits.
        self.us_xx_entity_resolution_view_deploy_task_id = (
            f"{self.us_xx_fake_extractor_collection_branch}."
            f"{UPDATE_ENTITY_RESOLUTION_VIEWS_TASK_ID}"
        )

        # The whole us_xx extraction subtree (the per-extractor-collection branch plus
        # its branching scaffolding), minus the ER sub-branches and the single
        # extractor-level entity_resolution view deploy, which are handled separately (all
        # vary by scenario). With the sync/identify tasks mocked to return a job, every
        # first-order task in it succeeds whenever the schema update it depends on does; it
        # upstream-fails as a unit when the document-collections branching it runs after
        # fails.
        self.us_xx_extraction_subtree_regex = (
            rf"^(?!{re.escape(self.us_xx_entity_resolution_view_deploy_task_id)}$)"
            rf"{self.us_xx_branch}\.extractor_collections\."
            rf"(?!.*_ENTITY_RESOLUTION\.).*$"
        )
        # us_yy has no extractor config, so its state branch has no extraction branching
        # at all — nothing to assert for it here.

        # Frame tasks (initialize, branching scaffolding, ALL_DONE noops in
        # non-target document collections) that succeed in any happy-path test scoped to
        # us_xx + fake_case_notes. Includes the whole us_xx extraction subtree, which
        # succeeds in every happy-path test (the sync/identify tasks are mocked to return
        # a job). The one test that short-circuits part of the subtree does not use this
        # frame.
        self.us_xx_fake_case_notes_frame_success_regexes = [
            r"^initialize_dag\..*$",
            rf"^{re.escape(self.update_schemata_task)}$",
            rf"^{re.escape(self.validate_source_tables_task)}$",
            r"^branch_(start|end)$",
            rf"^{self.us_xx_branch}\.document_collections\.branch_(start|end)$",
            self.us_xx_extraction_subtree_regex,
            # Each ER sub-branch's discovery/check_has_updates/after_upload_noop succeed in
            # every happy-path test (the first-order deploy they run after succeeds); the
            # ER upload path and extraction are scenario-dependent and matched per-test.
            self.us_xx_entity_resolution_always_succeeds_regex,
            *self.non_target_document_collection_always_succeeds_regexes,
        ]
        # Subset of the frame above for failure-path tests where branch_end
        # also fails. Excludes the branch_end tasks because they propagate the
        # failure rather than succeeding.
        self.us_xx_fake_case_notes_frame_success_regexes_excluding_branch_ends = [
            r"^initialize_dag\..*$",
            rf"^{re.escape(self.update_schemata_task)}$",
            rf"^{re.escape(self.validate_source_tables_task)}$",
            r"^branch_start$",
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

        self.raise_on_failures_patcher = patch(
            "recidiviz.airflow.dags.llm_document_extraction.extraction_tasks."
            "raise_on_attention_required_extraction_failures",
        )
        self.mock_raise_on_failures = self.raise_on_failures_patcher.start()

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
        self.raise_on_failures_patcher.stop()
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
            # Discovery for an entity-resolution composite collection rebuilds its
            # entry→source map table; the write's return value is not consumed.
            if address.table_id.endswith(f"_{ENTRY_SOURCE_MAP_TABLE_ID_SUFFIX}"):
                return MagicMock()
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

    def _us_xx_entity_resolution_view_deploy_regex(self) -> str:
        """Anchored exact-match regex for the single extractor-level ER view deploy."""
        return rf"^{re.escape(self.us_xx_entity_resolution_view_deploy_task_id)}$"

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
        self.assertIn(self.validate_source_tables_task, dag.task_ids)
        validate_task = dag.get_task(self.validate_source_tables_task)
        self.assertEqual(
            {self.update_schemata_task},
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
                    # Composite discovery for each ER sub-branch also finds no updates,
                    # short-circuiting its upload path and, in turn, its extraction. With
                    # every group's extraction skipped, the extractor-level ER view deploy
                    # (NONE_FAILED_MIN_ONE_SUCCESS) skips too.
                    self.us_xx_entity_resolution_upload_path_regex,
                    self.us_xx_entity_resolution_extraction_regex,
                    self._us_xx_entity_resolution_view_deploy_regex(),
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

            # The first-order case-notes discovery writes the generation-output and
            # metadata-updates temp tables (no metadata updates → no contents call) = 2.
            # Each of the 3 ER composite discoveries additionally rebuilds its
            # entry→source map table, so writes 3 (generation-output, entry→source map,
            # metadata-updates) = 9. 2 + 9 = 11.
            self.assertEqual(self.mock_bq_client.create_table_from_query.call_count, 11)
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
                    # Composite discovery finds updates too, so every ER sub-branch's
                    # upload path and extraction run and succeed, and the extractor-level
                    # ER view deploy runs after them.
                    self.us_xx_entity_resolution_upload_path_regex,
                    self.us_xx_entity_resolution_extraction_regex,
                    self._us_xx_entity_resolution_view_deploy_regex(),
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # The recorder runs for the first-order case-notes collection and each of the
            # 3 ER composite collections; each runs two INSERTs (document_contents +
            # metadata). 4 recorders × 2 = 8.
            insert_queries = self._get_insert_query_strs()
            self.assertEqual(len(insert_queries), 8)

            # Status CSV load happened once per collection with content (content_rows > 0):
            # case-notes + 3 ER composites = 4.
            self.assertEqual(
                self.mock_bq_client.load_table_from_cloud_storage.call_count, 4
            )

            # All rows inserted, so every collection's temp tables are deleted. The
            # case-notes collection deletes 3 (generation-output by discovery; metadata
            # updates and new contents by the recorder). Each ER composite collection
            # deletes the same 3. 4 collections × 3 = 12.
            self.assertEqual(self.mock_bq_client.delete_table.call_count, 12)

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
                    rf"^[^.]+\.document_collections\."
                    rf"[^.]+\."
                    rf"(?:{BUILD_DOCUMENT_UPLOAD_POD_ARGUMENTS_TASK_ID}|"
                    rf"{DOCUMENT_UPLOAD_TASK_ID}|"
                    rf"{AFTER_UPLOAD_NOOP_TASK_ID}|"
                    rf"{RECORD_DOCUMENT_UPLOAD_RESULTS_TASK_ID})$",
                    # Each ER sub-branch's composite discovery also finds no updates, so
                    # its upload path short-circuits too, which skips its extraction. With
                    # every group's extraction skipped, the extractor-level ER view deploy
                    # skips too.
                    self.us_xx_entity_resolution_upload_path_regex,
                    self.us_xx_entity_resolution_extraction_regex,
                    self._us_xx_entity_resolution_view_deploy_regex(),
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # Two create_table_from_query calls per document collection (the
            # generation-output and metadata-updates temp tables; 0 metadata
            # rows → no contents call). 2 launched US_XX document collections + 1
            # launched US_YY document collection = 3 collections × 2 = 6. Each of the 3
            # ER composite discoveries additionally rebuilds its entry→source map table,
            # so writes 3 (generation-output, entry→source map, metadata-updates) = 9.
            # 6 + 9 = 15.
            self.assertEqual(self.mock_bq_client.create_table_from_query.call_count, 15)
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
                    r"^branch_end$",
                    # Extraction runs after the whole document-collections branching, so
                    # the failed branch_end upstream-fails the entire extraction subtree,
                    # including every task in each ER sub-branch (except its ALL_DONE
                    # after_upload_noop, which still runs — see successes below) and the
                    # single extractor-level ER view deploy.
                    self.us_xx_extraction_subtree_regex,
                    self._us_xx_entity_resolution_view_deploy_regex(),
                    rf"^{self.us_xx_fake_extractor_collection_branch}\."
                    rf"[^.]+_ENTITY_RESOLUTION\."
                    rf"(?!{AFTER_UPLOAD_NOOP_TASK_ID}$)[^.]+$",
                ],
                expected_skipped_task_id_regexes=self.non_target_document_collection_skipped_regexes,
                expected_success_task_id_regexes=[
                    *self.us_xx_fake_case_notes_frame_success_regexes_excluding_branch_ends,
                    # after_upload_noop runs (ALL_DONE), but with no work to do — both for
                    # the case-notes collection and for each ER sub-branch.
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        AFTER_UPLOAD_NOOP_TASK_ID
                    ),
                    rf"^{self.us_xx_fake_extractor_collection_branch}\."
                    rf"[^.]+_ENTITY_RESOLUTION\.{AFTER_UPLOAD_NOOP_TASK_ID}$",
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
                    # Composite discovery finds updates too, so every ER sub-branch's
                    # upload path and extraction run and succeed, and the extractor-level
                    # ER view deploy runs after them.
                    self.us_xx_entity_resolution_upload_path_regex,
                    self.us_xx_entity_resolution_extraction_regex,
                    self._us_xx_entity_resolution_view_deploy_regex(),
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
                        rf"^{re.escape(self.update_schemata_task)}$",
                        rf"^{re.escape(self.validate_source_tables_task)}$",
                        r"^branch_(start|end)$",
                        r"^us_(xx|yy)\..*$",
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
                    rf"^{re.escape(self.update_schemata_task)}$",
                    # The validation side branch depends on the schema update, so it
                    # upstream-fails alongside the branches.
                    rf"^{re.escape(self.validate_source_tables_task)}$",
                    # Every state-branching task upstream-fails except the ALL_DONE
                    # after_upload_noop tasks below. The top-level branch_start/branch_end
                    # scaffolding also upstream-fails (its inputs failed rather than
                    # skipped).
                    r"^branch_(start|end)$",
                    r"^us_(xx|yy)\.(?!.*after_upload_noop$).*$",
                ],
                expected_skipped_task_id_regexes=[],
                expected_success_task_id_regexes=[
                    r"^initialize_dag\..*$",
                    r"^us_(xx|yy)\..*after_upload_noop$",
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
                    # Only document_upload skips in each ER sub-branch (no new contents);
                    # the rest of its upload path still runs.
                    rf"^{self.us_xx_fake_extractor_collection_branch}\."
                    rf"[^.]+_ENTITY_RESOLUTION\.{DOCUMENT_UPLOAD_TASK_ID}$",
                    *self.non_target_document_collection_skipped_regexes,
                ],
                expected_success_task_id_regexes=[
                    *self.us_xx_fake_case_notes_frame_success_regexes,
                    rf"^{self.us_xx_fake_case_notes_collection_branch}\."
                    rf"(?!{DOCUMENT_UPLOAD_TASK_ID}$)[^.]+$",
                    # Each ER sub-branch's build-args/after_upload_noop/record tasks run
                    # (the recorder still inserts the metadata rows), so its extraction
                    # runs too, and the extractor-level ER view deploy runs after them.
                    rf"^{self.us_xx_fake_extractor_collection_branch}\."
                    rf"[^.]+_ENTITY_RESOLUTION\."
                    rf"(?:{BUILD_DOCUMENT_UPLOAD_POD_ARGUMENTS_TASK_ID}|"
                    rf"{AFTER_UPLOAD_NOOP_TASK_ID}|{RECORD_DOCUMENT_UPLOAD_RESULTS_TASK_ID})$",
                    self.us_xx_entity_resolution_extraction_regex,
                    self._us_xx_entity_resolution_view_deploy_regex(),
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # One metadata INSERT per collection; document_contents hydration was
            # skipped because num_new_document_contents_rows == 0. Case-notes + 3 ER
            # composites = 4.
            insert_queries = self._get_insert_query_strs()
            self.assertEqual(len(insert_queries), 4)
            for insert_query in insert_queries:
                self.assertNotIn("_document_contents`", insert_query)

            # No upload status CSVs to load.
            self.mock_bq_client.load_table_from_cloud_storage.assert_not_called()

            # Full success for each collection: discovery deleted the generation-output
            # temp table and the recorder deleted the metadata updates and new contents
            # temp tables. Case-notes + 3 ER composites = 4 collections × 3 = 12.
            self.assertEqual(self.mock_bq_client.delete_table.call_count, 12)

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
                    # Each ER sub-branch's composite discovery also finds no updates, so
                    # its upload path short-circuits too, which skips its extraction. With
                    # every group's extraction skipped, the extractor-level ER view deploy
                    # skips too.
                    self.us_xx_entity_resolution_upload_path_regex,
                    self.us_xx_entity_resolution_extraction_regex,
                    self._us_xx_entity_resolution_view_deploy_regex(),
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # Two create_table_from_query calls per document collection (the
            # generation-output and metadata-updates temp tables; 0 metadata
            # rows → no contents call). The fake config launches 2 US_XX document
            # collections under staging, so 2 × 2 = 4. Each of the 3 ER composite
            # discoveries additionally rebuilds its entry→source map table, so writes 3
            # (generation-output, entry→source map, metadata-updates) = 9. 4 + 9 = 13.
            self.assertEqual(self.mock_bq_client.create_table_from_query.call_count, 13)
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
                    r"^branch_end$",
                    # Every task in every us_xx document collection branch becomes
                    # upstream_failed except after_upload_noop, which is an
                    # ALL_DONE EmptyOperator with no body and succeeds.
                    rf"^{self.us_xx_branch}\.document_collections\."
                    rf"[^.]+\.(?!{AFTER_UPLOAD_NOOP_TASK_ID}$)[^.]+$",
                    # Extraction runs after the whole document-collections branching, so
                    # the failed branch_end upstream-fails the entire extraction subtree,
                    # including every task in each ER sub-branch except its ALL_DONE
                    # after_upload_noop (see successes below) and the single extractor-level
                    # ER view deploy.
                    self.us_xx_extraction_subtree_regex,
                    self._us_xx_entity_resolution_view_deploy_regex(),
                    rf"^{self.us_xx_fake_extractor_collection_branch}\."
                    rf"[^.]+_ENTITY_RESOLUTION\."
                    rf"(?!{AFTER_UPLOAD_NOOP_TASK_ID}$)[^.]+$",
                ],
                expected_skipped_task_id_regexes=[self.us_yy_all_skippable_tasks_regex],
                expected_success_task_id_regexes=[
                    r"^initialize_dag\..*$",
                    rf"^{re.escape(self.update_schemata_task)}$",
                    rf"^{re.escape(self.validate_source_tables_task)}$",
                    r"^branch_start$",
                    # All us_xx document collection branches' after_upload_noop tasks
                    # succeed (ALL_DONE), as do each ER sub-branch's.
                    rf"^{self.us_xx_branch}\.document_collections\."
                    rf"[^.]+\.{AFTER_UPLOAD_NOOP_TASK_ID}$",
                    rf"^{self.us_xx_fake_extractor_collection_branch}\."
                    rf"[^.]+_ENTITY_RESOLUTION\.{AFTER_UPLOAD_NOOP_TASK_ID}$",
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
                    rf"^{re.escape(self.update_schemata_task)}$",
                    rf"^{re.escape(self.validate_source_tables_task)}$",
                    r"^branch_(start|end)$",
                    r"^us_(xx|yy)\..*$",
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
                    # Every mapped document_upload fails, including each ER sub-branch's.
                    rf"^{self.us_xx_fake_extractor_collection_branch}\."
                    rf"[^.]+_ENTITY_RESOLUTION\.{DOCUMENT_UPLOAD_TASK_ID}$",
                ],
                expected_skipped_task_id_regexes=self.non_target_document_collection_skipped_regexes,
                expected_success_task_id_regexes=[
                    *self.us_xx_fake_case_notes_frame_success_regexes,
                    rf"^{self.us_xx_fake_case_notes_collection_branch}\."
                    rf"(?!{DOCUMENT_UPLOAD_TASK_ID}$)[^.]+$",
                    # Each ER sub-branch's after_upload_noop absorbs the upload failure, so
                    # build-args/after_upload_noop/record still succeed and the extraction
                    # runs, and the extractor-level ER view deploy runs after them.
                    rf"^{self.us_xx_fake_extractor_collection_branch}\."
                    rf"[^.]+_ENTITY_RESOLUTION\."
                    rf"(?:{BUILD_DOCUMENT_UPLOAD_POD_ARGUMENTS_TASK_ID}|"
                    rf"{AFTER_UPLOAD_NOOP_TASK_ID}|{RECORD_DOCUMENT_UPLOAD_RESULTS_TASK_ID})$",
                    self.us_xx_entity_resolution_extraction_regex,
                    self._us_xx_entity_resolution_view_deploy_regex(),
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # The recorder still ran both INSERTs (document_contents + metadata) for the
            # case-notes collection and each of the 3 ER composites. 4 recorders × 2 = 8.
            insert_queries = self._get_insert_query_strs()
            self.assertEqual(len(insert_queries), 8)

            # Partial-success path: each recorder preserves its temp tables for debugging.
            # Discovery itself succeeded, so it already deleted the generation-output temp
            # table for the case-notes collection and each of the 3 ER composites = 4.
            self.assertEqual(self.mock_bq_client.delete_table.call_count, 4)

    def _constructed_pod_arguments(
        self, task_id: str, *, containing_arg: str | None = None
    ) -> list[str]:
        """Returns the argv the DAG constructed the KPO named |task_id| with. When the
        same task_id is used by multiple KPOs (e.g. the shared run_extraction task across
        the first-order and ER extractor branches), |containing_arg| disambiguates to the
        one whose argv contains it."""
        for call in self.mock_kpo_constructor.call_args_list:
            if call.kwargs.get("task_id") != task_id:
                continue
            arguments = assert_type(call.kwargs["arguments"], list)
            if containing_arg is None or containing_arg in arguments:
                return arguments
        raise ValueError(f"No KPO constructed with task_id [{task_id}].")

    def test_extraction_runs_when_job_is_generated(self) -> None:
        """When identify_job_documents returns a job, check_has_job proceeds and both the
        extraction pod and the first-order view update pod run. The extraction pod's argv
        pulls the job id from the identify task's XCom; the view update pod names the
        first_order stage."""
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
                    # Each ER sub-branch's composite upload is short-circuited (no new
                    # composites), which skips its extraction too. With every group's
                    # extraction skipped, the extractor-level ER view deploy skips too.
                    self.us_xx_entity_resolution_upload_path_regex,
                    self.us_xx_entity_resolution_extraction_regex,
                    self._us_xx_entity_resolution_view_deploy_regex(),
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            # sync/identify run once for the first-order extractor; each ER extraction is
            # short-circuited before them because its composites found no updates.
            self.assertEqual(self.mock_sync_extractor_metadata.call_count, 1)
            self.assertEqual(self.mock_identify_job_documents.call_count, 1)

            # The first-order and the 3 ER extractions share the run_extraction task_id;
            # disambiguate to the first-order one by its (exact) extractor collection name.
            run_extraction_argv = self._constructed_pod_arguments(
                RUN_EXTRACTION_TASK_ID,
                containing_arg=f"--extractor_collection_name={FAKE_EXTRACTOR_COLLECTION_NAME}",
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

    def test_extraction_failures_fail_run_without_blocking_view_deploy(self) -> None:
        """When the run produced attention-required document failures, the
        check_extraction_failures side branch fails but the first-order view deploy
        proceeds as normal."""
        self.mock_raise_on_failures.side_effect = ValueError(
            "attention-required failures"
        )

        with Session(bind=self.engine) as session:
            self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf=self._run_conf(state_code_filter=StateCode.US_XX.value),
                # first order view deploy should not be in the failed or skipped sets.
                expected_failure_task_id_regexes=[
                    # Only the first-order extraction runs (each ER extraction
                    # short-circuits on no composite updates), so only its failure check
                    # runs and fails.
                    rf"^{self.us_xx_fake_extractor_collection_branch}\."
                    rf"{CHECK_EXTRACTION_FAILURES_TASK_ID}$",
                    # branch_end tasks are ALL_DONE but re-raise when an upstream failed,
                    # propagating the failure up the extraction branching scaffolding.
                    r"^branch_end$",
                    rf"^{self.us_xx_branch}\.extractor_collections\.branch_end$",
                ],
                expected_skipped_task_id_regexes=[
                    self.us_yy_all_skippable_tasks_regex,
                    rf"^{self.us_xx_branch}\.document_collections\."
                    r"[^.]+\."
                    rf"(?:{BUILD_DOCUMENT_UPLOAD_POD_ARGUMENTS_TASK_ID}|{DOCUMENT_UPLOAD_TASK_ID}|"
                    rf"{AFTER_UPLOAD_NOOP_TASK_ID}|{RECORD_DOCUMENT_UPLOAD_RESULTS_TASK_ID})$",
                    self.us_xx_entity_resolution_upload_path_regex,
                    self.us_xx_entity_resolution_extraction_regex,
                    # Every group's ER extraction short-circuits, so the extractor-level ER
                    # view deploy skips too.
                    self._us_xx_entity_resolution_view_deploy_regex(),
                ],
            )

    def test_extraction_short_circuits_when_no_job_is_generated(self) -> None:
        """When identify_job_documents returns None (no documents need processing),
        check_has_job short-circuits and the first-order extraction and view update pods
        are skipped; the metadata sync and job identification still run and succeed. The
        skipped first-order view deploy propagates into each ER sub-branch, skipping it
        entirely except its ALL_DONE after_upload_noop."""
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
                    # check_has_job short-circuits the first-order extraction, its failure
                    # check, and the view update pods. That skips update_first_order_views,
                    # which the ER sub-branches run after, so the entire ER sub-branch skips
                    # with it: with no barrier un-skipping the extraction, every ER task
                    # skips except its ALL_DONE after_upload_noop (see successes below).
                    rf"^{self.us_xx_fake_extractor_collection_branch}\."
                    rf"(?:{RUN_EXTRACTION_TASK_ID}|{CHECK_EXTRACTION_FAILURES_TASK_ID}|"
                    rf"{UPDATE_FIRST_ORDER_VIEWS_TASK_ID})$",
                    rf"^{self.us_xx_fake_extractor_collection_branch}\."
                    rf"[^.]+_ENTITY_RESOLUTION\."
                    rf"(?:{RUN_DOCUMENT_DISCOVERY_TASK_ID}|{CHECK_HAS_UPDATES_TASK_ID}|"
                    rf"{BUILD_DOCUMENT_UPLOAD_POD_ARGUMENTS_TASK_ID}|{DOCUMENT_UPLOAD_TASK_ID}|"
                    rf"{RECORD_DOCUMENT_UPLOAD_RESULTS_TASK_ID}|"
                    rf"{SYNC_EXTRACTOR_METADATA_TASK_ID}|{IDENTIFY_JOB_DOCUMENTS_TASK_ID}|"
                    rf"{CHECK_HAS_JOB_TASK_ID}|{RUN_EXTRACTION_TASK_ID}|"
                    rf"{CHECK_EXTRACTION_FAILURES_TASK_ID})$",
                    # Every group's ER extraction is skipped, so the extractor-level ER
                    # view deploy skips too.
                    self._us_xx_entity_resolution_view_deploy_regex(),
                ],
                expected_success_task_id_regexes=[
                    # Frame successes, but NOT the whole extraction subtree (part of it
                    # is short-circuited below), so they are listed explicitly here
                    # rather than via us_xx_fake_case_notes_frame_success_regexes.
                    r"^initialize_dag\..*$",
                    rf"^{re.escape(self.update_schemata_task)}$",
                    rf"^{re.escape(self.validate_source_tables_task)}$",
                    r"^branch_(start|end)$",
                    rf"^{self.us_xx_branch}\.document_collections\.branch_(start|end)$",
                    rf"^{self.us_xx_branch}\.extractor_collections\.branch_(start|end)$",
                    *self.non_target_document_collection_always_succeeds_regexes,
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        RUN_DOCUMENT_DISCOVERY_TASK_ID
                    ),
                    self._us_xx_fake_case_notes_collection_branch_regex(
                        CHECK_HAS_UPDATES_TASK_ID
                    ),
                    # sync, identify, and the short-circuit itself run and succeed for the
                    # first-order extractor.
                    rf"^{self.us_xx_fake_extractor_collection_branch}\."
                    rf"(?:{SYNC_EXTRACTOR_METADATA_TASK_ID}|{IDENTIFY_JOB_DOCUMENTS_TASK_ID}|"
                    rf"{CHECK_HAS_JOB_TASK_ID})$",
                    # The first-order skip propagates into each ER sub-branch's
                    # document-store flow, skipping its discovery; only its ALL_DONE
                    # after_upload_noop still runs (and succeeds with nothing to do).
                    rf"^{self.us_xx_fake_extractor_collection_branch}\."
                    rf"[^.]+_ENTITY_RESOLUTION\.{AFTER_UPLOAD_NOOP_TASK_ID}$",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

    def _us_xx_entity_resolution_branch(self, er_branch_suffix: str) -> str:
        # The ER branch group is named with only the (entity group, ER suffix) segment of
        # the ER collection name, not the full name (which repeats the first-order prefix).
        return f"{self.us_xx_fake_extractor_collection_branch}.{er_branch_suffix}"

    def test_entity_resolution_pass_runs_per_entity_group(self) -> None:
        """Each entity group of an extractor that declares them gets its own ER branch —
        composite generation + upload, then an ER extraction whose pod pulls its own job
        id — running after the extractor's first-order view deploy. The ER view graph
        covers all the extractor's groups together, so it is deployed by a SINGLE
        extractor-level task (keyed on the first-order collection, never an ER collection)
        downstream of every group's ER extraction."""
        self._mock_bq_client_has_updates()

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self._create_dag(),
                session=session,
                run_conf=self._run_conf(state_code_filter=StateCode.US_XX.value),
                expected_failure_task_id_regexes=[],
                expected_skipped_task_id_regexes=[self.us_yy_all_skippable_tasks_regex],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            dag = self._create_dag()
            # Exactly one ER view deploy for the extractor — at the extractor-branch level,
            # NOT one per entity group.
            er_view_deploy_task_ids = {
                task.task_id
                for task in dag.tasks
                if task.task_id.endswith(f".{UPDATE_ENTITY_RESOLUTION_VIEWS_TASK_ID}")
            }
            self.assertEqual(
                {self.us_xx_entity_resolution_view_deploy_task_id},
                er_view_deploy_task_ids,
            )

            # The single ER deploy names the FIRST-ORDER collection (the entrypoint
            # resolves it via the first-order config map, which never contains an ER
            # collection name) and the entity_resolution stage.
            update_views_argv = self._constructed_pod_arguments(
                UPDATE_ENTITY_RESOLUTION_VIEWS_TASK_ID
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
            self.assertIn("--stage=entity_resolution", update_views_argv)

            # It runs downstream of every group's ER extraction, with
            # NONE_FAILED_MIN_ONE_SUCCESS so it deploys once at least one group extracted.
            er_deploy_task = dag.get_task(
                self.us_xx_entity_resolution_view_deploy_task_id
            )
            self.assertEqual(
                TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, er_deploy_task.trigger_rule
            )
            self.assertEqual(
                {
                    f"{self._us_xx_entity_resolution_branch(entity_resolution_collection_name_suffix(group))}.{RUN_EXTRACTION_TASK_ID}"
                    for group in ("location", "assignment", "pay_rate")
                },
                er_deploy_task.upstream_task_ids,
            )

            # Assert one entity group's ER extraction pod: it extracts its own ER
            # collection and pulls its own job id (not the first-order one). The pod arg
            # carries the full ER collection name; the branch path carries only its suffix.
            location_er_name = entity_resolution_collection_name(
                first_order_extractor_collection_name=FAKE_EXTRACTOR_COLLECTION_NAME,
                entity_group_name="location",
            )
            location_er_branch = self._us_xx_entity_resolution_branch(
                entity_resolution_collection_name_suffix("location")
            )

            run_extraction_argv = self._constructed_pod_arguments(
                RUN_EXTRACTION_TASK_ID,
                containing_arg=f"--extractor_collection_name={location_er_name}",
            )
            self.assertIn(
                "--entrypoint=LLMDocumentExtractionSyncJobEntrypoint",
                run_extraction_argv,
            )
            self.assertIn(f"--state_code={StateCode.US_XX.value}", run_extraction_argv)
            self.assertIn(
                f"--extractor_collection_name={location_er_name}", run_extraction_argv
            )
            self.assertIn(
                f"--job_id={{{{ task_instance.xcom_pull(task_ids="
                f"'{location_er_branch}.{IDENTIFY_JOB_DOCUMENTS_TASK_ID}') }}}}",
                run_extraction_argv,
            )

            # The ER composite generation reads the materialized first-order
            # __pre_resolution views, so the ER branch runs after the first-order view
            # deploy.
            er_discovery_task = dag.get_task(
                f"{location_er_branch}.{RUN_DOCUMENT_DISCOVERY_TASK_ID}"
            )
            self.assertIn(
                f"{self.us_xx_fake_extractor_collection_branch}."
                f"{UPDATE_FIRST_ORDER_VIEWS_TASK_ID}",
                er_discovery_task.upstream_task_ids,
            )
