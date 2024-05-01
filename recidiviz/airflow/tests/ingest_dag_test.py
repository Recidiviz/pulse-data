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
"""
Unit tests to test the ingest DAG.
"""
from typing import Any
from unittest.mock import MagicMock, patch

from airflow.models import BaseOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState
from sqlalchemy.orm import Session

from recidiviz.airflow.tests.test_utils import AirflowIntegrationTest
from recidiviz.airflow.tests.utils.dag_helper_functions import (
    fake_failure_task,
    fake_operator_constructor,
    fake_operator_with_return_value,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

_PROJECT_ID = "recidiviz-testing"

_VERIFY_PARAMETERS_TASK_ID = "initialize_ingest_dag.verify_parameters"
_WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID = "initialize_ingest_dag.wait_to_continue_or_cancel"
_HANDLE_QUEUEING_RESULT_TASK_ID = "initialize_ingest_dag.handle_queueing_result"


def _fake_pod_operator(*args: Any, **kwargs: Any) -> BaseOperator:
    if "--entrypoint=IngestPipelineShouldRunInDagEntrypoint" in kwargs["arguments"]:
        return fake_operator_with_return_value(True)(*args, **kwargs)

    return fake_operator_constructor(*args, **kwargs)


class TestIngestDagIntegration(AirflowIntegrationTest):
    """
    Integration test to test the Ingest DAG logic.
    """

    def setUp(self) -> None:
        super().setUp()
        self.environment_patcher = patch(
            "os.environ",
            {
                "GCP_PROJECT": _PROJECT_ID,
            },
        )
        self.environment_patcher.start()

        test_state_code_and_instance_pairs = [
            (StateCode.US_XX, DirectIngestInstance.PRIMARY),
            (StateCode.US_XX, DirectIngestInstance.SECONDARY),
            (StateCode.US_YY, DirectIngestInstance.PRIMARY),
            (StateCode.US_YY, DirectIngestInstance.SECONDARY),
        ]

        self.ingest_branching_get_all_enabled_state_and_instance_pairs_patcher = patch(
            "recidiviz.airflow.dags.ingest.ingest_branching.get_ingest_pipeline_enabled_state_and_instance_pairs",
            return_value=test_state_code_and_instance_pairs,
        )
        self.ingest_branching_get_all_enabled_state_and_instance_pairs_patcher.start()

        self.initialize_ingest_dag_get_all_enabled_state_and_instance_pairs_patcher = patch(
            "recidiviz.airflow.dags.ingest.initialize_ingest_dag_group.get_ingest_pipeline_enabled_state_and_instance_pairs",
            return_value=test_state_code_and_instance_pairs,
        )
        self.initialize_ingest_dag_get_all_enabled_state_and_instance_pairs_patcher.start()

        self.kubernetes_pod_operator_patcher = patch(
            "recidiviz.airflow.dags.ingest.single_ingest_pipeline_group.build_kubernetes_pod_task",
            side_effect=_fake_pod_operator,
        )
        self.kubernetes_pod_operator_patcher.start()

        self.cloud_sql_query_operator_patcher = patch(
            "recidiviz.airflow.dags.ingest.single_ingest_pipeline_group.CloudSqlQueryOperator",
            side_effect=fake_operator_with_return_value({}),
        )
        self.cloud_sql_query_operator_patcher.start()

        self.recidiviz_dataflow_operator_patcher = patch(
            "recidiviz.airflow.dags.ingest.single_ingest_pipeline_group.RecidivizDataflowFlexTemplateOperator",
            side_effect=fake_operator_constructor,
        )
        self.mock_dataflow_operator = self.recidiviz_dataflow_operator_patcher.start()

        self.default_ingest_pipeline_regions_by_state_code_patcher = patch.dict(
            "recidiviz.airflow.dags.ingest.single_ingest_pipeline_group.DEFAULT_INGEST_PIPELINE_REGIONS_BY_STATE_CODE",
            values={StateCode.US_XX: "us-east1-test", StateCode.US_YY: "us-east2-test"},
        )
        self.default_ingest_pipeline_regions_by_state_code_patcher.start()

        # Need to import ingest_dag inside test suite so environment variables are set before importing,
        # otherwise ingest_dag will raise an Error and not import.
        from recidiviz.airflow.dags.ingest_dag import (  # pylint: disable=import-outside-toplevel
            create_ingest_dag,
        )

        self.dag = create_ingest_dag()

    def tearDown(self) -> None:
        self.environment_patcher.stop()
        self.ingest_branching_get_all_enabled_state_and_instance_pairs_patcher.stop()
        self.initialize_ingest_dag_get_all_enabled_state_and_instance_pairs_patcher.stop()
        self.kubernetes_pod_operator_patcher.stop()
        self.cloud_sql_query_operator_patcher.stop()
        self.recidiviz_dataflow_operator_patcher.stop()
        self.default_ingest_pipeline_regions_by_state_code_patcher.stop()
        super().tearDown()

    def test_ingest_dag(self) -> None:
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(self.dag, session=session)
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

    def test_ingest_dag_with_filter(self) -> None:
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self.dag,
                session=session,
                run_conf={
                    "state_code_filter": StateCode.US_XX.value,
                    "ingest_instance": "PRIMARY",
                },
                expected_skipped_ids=[".*US_YY.*", ".*SECONDARY_DATAFLOW.*"],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

    def test_ingest_dag_with_filter_secondary(self) -> None:
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self.dag,
                session=session,
                run_conf={
                    "state_code_filter": StateCode.US_XX.value,
                    "ingest_instance": "SECONDARY",
                },
                expected_skipped_ids=[".*US_YY.*", ".*PRIMARY_DATAFLOW.*"],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

    def test_ingest_dag_with_filter_state_only(self) -> None:
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self.dag,
                session=session,
                run_conf={
                    "state_code_filter": StateCode.US_XX.value,
                },
                expected_failure_ids=[_VERIFY_PARAMETERS_TASK_ID],
                expected_skipped_ids=[
                    _WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID,
                    _HANDLE_QUEUEING_RESULT_TASK_ID,
                    ".*ingest_branching.*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)
            self.assertIn(
                "[ingest_instance] and [state_code_filter] must both be set or both be unset",
                result.failure_messages[_VERIFY_PARAMETERS_TASK_ID],
            )

    def test_ingest_dag_with_filter_ingest_instance_only(self) -> None:
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self.dag,
                session=session,
                run_conf={
                    "ingest_instance": "PRIMARY",
                },
                expected_failure_ids=[_VERIFY_PARAMETERS_TASK_ID],
                expected_skipped_ids=[
                    _WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID,
                    _HANDLE_QUEUEING_RESULT_TASK_ID,
                    ".*ingest_branching.*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)
            self.assertIn(
                "[ingest_instance] and [state_code_filter] must both be set or both be unset",
                result.failure_messages[_VERIFY_PARAMETERS_TASK_ID],
            )

    @patch("recidiviz.airflow.dags.ingest.single_ingest_pipeline_group._acquire_lock")
    def test_ingest_dag_fails_for_branch_failure(
        self, mock_acquire_lock: MagicMock
    ) -> None:
        # Need to re-import ingest_dag if you want to modify the creation of the dag for a specific test.
        from recidiviz.airflow.dags.ingest_dag import (  # pylint: disable=import-outside-toplevel
            create_ingest_dag,
        )

        def fail_for_us_xx_primary_acquire_lock(
            state_code: StateCode, instance: DirectIngestInstance
        ) -> BaseOperator:
            """
            Raises an exception to simulate a failure when the task id is "acquire_lock", otherwise passes
            through to the EmptyOperator.
            """

            if (
                state_code == StateCode.US_XX
                and instance == DirectIngestInstance.PRIMARY
            ):
                return fake_failure_task(task_id="acquire_lock")

            return EmptyOperator(task_id="acquire_lock")

        mock_acquire_lock.side_effect = fail_for_us_xx_primary_acquire_lock

        with Session(bind=self.engine) as session:
            dag = create_ingest_dag()
            result = self.run_dag_test(
                dag,
                session=session,
                expected_failure_ids=[
                    ".*us_xx_primary_dataflow.acquire_lock.*",
                    ".*us_xx_primary_dataflow.dataflow_pipeline.*",
                    ".*us_xx_primary_dataflow.write_ingest_job_completion",
                    ".*us_xx_primary_dataflow.write_upper_bounds.*",
                    ".*ingest_branching.branch_end.*",
                ],
            )
            self.assertEqual(DagRunState.FAILED, result.dag_run_state)

    @patch(
        "recidiviz.airflow.dags.ingest.single_ingest_pipeline_group._ingest_pipeline_should_run_in_dag"
    )
    def test_ingest_dag_one_branch_skips(
        self, mock_ingest_pipeline_should_run_in_dag: MagicMock
    ) -> None:
        # Need to re-import ingest_dag if you want to modify the creation of the dag for a specific test.
        from recidiviz.airflow.dags.ingest_dag import (  # pylint: disable=import-outside-toplevel
            create_ingest_dag,
        )

        operator_return_false = fake_operator_with_return_value(False)
        operator_return_true = fake_operator_with_return_value(True)

        def fake_ingest_pipeline_should_run_in_dag(
            state_code: StateCode, instance: DirectIngestInstance
        ) -> BaseOperator:
            """
            Return false when the task id is "ingest_pipeline_should_run_in_dag" for US_XX PRIMARY,
            otherwise returns true.
            """

            if (
                state_code == StateCode.US_XX
                and instance == DirectIngestInstance.PRIMARY
            ):
                return operator_return_false(
                    task_id="ingest_pipeline_should_run_in_dag"
                )

            return operator_return_true(task_id="ingest_pipeline_should_run_in_dag")

        mock_ingest_pipeline_should_run_in_dag.side_effect = (
            fake_ingest_pipeline_should_run_in_dag
        )

        with Session(bind=self.engine) as session:
            dag = create_ingest_dag()
            result = self.run_dag_test(
                dag,
                session=session,
                expected_skipped_ids=[
                    r".*us_xx_primary_dataflow.initialize_dataflow_pipeline.get_max_update_datetimes",
                    r".*us_xx_primary_dataflow.initialize_dataflow_pipeline.get_watermarks",
                    r".*us_xx_primary_dataflow.initialize_dataflow_pipeline.should_run_based_on_watermarks",
                    r".*us_xx_primary_dataflow.initialize_dataflow_pipeline.verify_raw_data_flashing_not_in_progress",
                    r".*us_xx_primary_dataflow.acquire_lock.*",
                    r".*us_xx_primary_dataflow.dataflow_pipeline.*",
                    r".*us_xx_primary_dataflow.write_ingest_job_completion",
                    r".*us_xx_primary_dataflow.write_upper_bounds.*",
                    r".*us_xx_primary_dataflow.release_lock.*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

    @patch("recidiviz.airflow.dags.ingest.single_ingest_pipeline_group._acquire_lock")
    @patch(
        "recidiviz.airflow.dags.ingest.single_ingest_pipeline_group._ingest_pipeline_should_run_in_dag"
    )
    def test_ingest_dag_one_branch_skips_one_branch_fails(
        self,
        mock_ingest_pipeline_should_run_in_dag: MagicMock,
        mock_acquire_lock: MagicMock,
    ) -> None:
        # Need to re-import ingest_dag if you want to modify the creation of the dag for a specific test.
        from recidiviz.airflow.dags.ingest_dag import (  # pylint: disable=import-outside-toplevel
            create_ingest_dag,
        )

        operator_return_false = fake_operator_with_return_value(False)
        operator_return_true = fake_operator_with_return_value(True)

        def fake_ingest_pipeline_should_run_in_dag(
            state_code: StateCode, instance: DirectIngestInstance
        ) -> BaseOperator:
            """
            Return false when the task id is "ingest_pipeline_should_run_in_dag" for US_XX PRIMARY,
            otherwise returns true.
            """

            if (
                state_code == StateCode.US_XX
                and instance == DirectIngestInstance.PRIMARY
            ):
                return operator_return_false(
                    task_id="ingest_pipeline_should_run_in_dag"
                )

            return operator_return_true(task_id="ingest_pipeline_should_run_in_dag")

        mock_ingest_pipeline_should_run_in_dag.side_effect = (
            fake_ingest_pipeline_should_run_in_dag
        )

        def fail_for_us_yy_primary_acquire_lock(
            state_code: StateCode, instance: DirectIngestInstance
        ) -> BaseOperator:
            """
            Raises an exception to simulate a failure when the task id is "acquire_lock", otherwise passes
            through to the EmptyOperator.
            """

            if (
                state_code == StateCode.US_YY
                and instance == DirectIngestInstance.PRIMARY
            ):
                return fake_failure_task(task_id="acquire_lock")

            return EmptyOperator(task_id="acquire_lock")

        mock_acquire_lock.side_effect = fail_for_us_yy_primary_acquire_lock

        with Session(bind=self.engine) as session:
            dag = create_ingest_dag()
            result = self.run_dag_test(
                dag,
                session=session,
                expected_skipped_ids=[
                    r".*us_xx_primary_dataflow.initialize_dataflow_pipeline.get_max_update_datetimes",
                    r".*us_xx_primary_dataflow.initialize_dataflow_pipeline.get_watermarks",
                    r".*us_xx_primary_dataflow.initialize_dataflow_pipeline.should_run_based_on_watermarks",
                    r".*us_xx_primary_dataflow.initialize_dataflow_pipeline.verify_raw_data_flashing_not_in_progress",
                    r".*us_xx_primary_dataflow.acquire_lock.*",
                    r".*us_xx_primary_dataflow.dataflow_pipeline.*",
                    r".*us_xx_primary_dataflow.write_ingest_job_completion",
                    r".*us_xx_primary_dataflow.write_upper_bounds.*",
                    r".*us_xx_primary_dataflow.release_lock.*",
                ],
                expected_failure_ids=[
                    ".*us_yy_primary_dataflow.acquire_lock.*",
                    ".*us_yy_primary_dataflow.dataflow_pipeline.*",
                    ".*us_yy_primary_dataflow.write_ingest_job_completion",
                    ".*us_yy_primary_dataflow.write_upper_bounds.*",
                    ".*ingest_branching.branch_end.*",
                ],
            )
            self.assertEqual(DagRunState.FAILED, result.dag_run_state)
