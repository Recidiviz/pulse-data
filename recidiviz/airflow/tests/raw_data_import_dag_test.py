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
from recidiviz.airflow.dags.utils.branching_by_key import BRANCH_END_TASK_NAME
from recidiviz.airflow.tests.test_utils import DAG_FOLDER, AirflowIntegrationTest
from recidiviz.airflow.tests.utils.dag_helper_functions import (
    FakeFailureOperator,
    fake_operator_with_return_value,
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
            task_ids_or_regex=r"list_normalized_unprocessed_files",
            include_upstream=False,
            include_downstream=True,
        )
        step_2_task_ids = ["register_raw_gcs_file_metadata"]

        for root in step_2_root.roots:
            assert "list_normalized_unprocessed_files" in root.task_id
            curr_task = root
            for step_2_task_id in step_2_task_ids:
                assert len(curr_task.downstream_list) == 1
                next_task = curr_task.downstream_list[0]
                assert step_2_task_id in next_task.task_id
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

    def tearDown(self) -> None:
        self.environment_patcher.stop()
        self.raw_data_enabled_pairs.stop()
        self.raw_data_enabled_pairs_two.stop()
        self.raw_data_enabled_states.stop()
        self.cloud_sql_query_operator_patcher.stop()
        self.gcs_operator_patcher.stop()
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
                expected_skipped_ids=[r".*_secondary_import_branch"],
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
                    r".*_primary_import_branch",
                    "us_yy_secondary_import_branch",
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
                    r".*_primary_import_branch\.list_normalized_unprocessed_files",
                    r".*_primary_import_branch\.release_raw_data_resource_locks",
                    r".*_primary_import_branch\.register_raw_gcs_file_metadata",
                    BRANCH_END_TASK_NAME,
                ],
                expected_skipped_ids=[
                    r".*_secondary_import_branch",
                ],
            )
            self.assertEqual(DagRunState.FAILED, result.dag_run_state)
