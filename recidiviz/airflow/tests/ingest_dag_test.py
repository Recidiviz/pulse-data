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
import os
from typing import Any
from unittest.mock import MagicMock, patch

from airflow.decorators import task
from airflow.models import BaseOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState
from sqlalchemy.orm import Session

from recidiviz.airflow.dags.monitoring.task_failure_alerts import (
    KNOWN_CONFIGURATION_PARAMETERS,
)
from recidiviz.airflow.tests.test_utils import AirflowIntegrationTest
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

_PROJECT_ID = "recidiviz-testing"

_VERIFY_PARAMETERS_TASK_ID = "initialize_ingest_dag.verify_parameters"
_CHECK_FOR_RUNNING_DAGS_TASK_ID = "initialize_ingest_dag.check_for_running_dags"


@patch.dict(
    KNOWN_CONFIGURATION_PARAMETERS,
    {
        f"{_PROJECT_ID}_ingest_dag": KNOWN_CONFIGURATION_PARAMETERS[
            f"{os.getenv('GCP_PROJECT')}_ingest_dag"
        ]
    },
)
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
            "recidiviz.airflow.dags.ingest.ingest_branching.get_all_enabled_state_and_instance_pairs",
            return_value=test_state_code_and_instance_pairs,
        )
        self.ingest_branching_get_all_enabled_state_and_instance_pairs_patcher.start()

        self.initialize_ingest_dag_get_all_enabled_state_and_instance_pairs_patcher = patch(
            "recidiviz.airflow.dags.ingest.initialize_ingest_dag_group.get_all_enabled_state_and_instance_pairs",
            return_value=test_state_code_and_instance_pairs,
        )
        self.initialize_ingest_dag_get_all_enabled_state_and_instance_pairs_patcher.start()

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
                    _CHECK_FOR_RUNNING_DAGS_TASK_ID,
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
                    _CHECK_FOR_RUNNING_DAGS_TASK_ID,
                    ".*ingest_branching.*",
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)
            self.assertIn(
                "[ingest_instance] and [state_code_filter] must both be set or both be unset",
                result.failure_messages[_VERIFY_PARAMETERS_TASK_ID],
            )

    # TODO(#23984): Update to mock to EmptyOperator and update side effect to extract and use the state code and instance args
    @patch(
        "recidiviz.airflow.dags.ingest.state_dataflow_pipeline._initialize_dataflow_pipeline"
    )
    def test_ingest_dag_fails_for_branch_failure(
        self, mock_initialize_dataflow_pipeline: MagicMock
    ) -> None:
        # Need to re-import ingest_dag if you want to modify the creation of the dag for a specific test.
        from recidiviz.airflow.dags.ingest_dag import (  # pylint: disable=import-outside-toplevel
            create_ingest_dag,
        )

        def fail_for_us_xx_primary_initialize_dataflow_pipeline(
            state_code: StateCode, instance: DirectIngestInstance
        ) -> BaseOperator:
            """
            Raises an exception to simulate a failure when the task id is "acquire_lock", otherwise passes
            through to the EmptyOperator.
            """

            @task(task_id="check_raw_data_max_update_time")
            def fake_failure_task() -> Any:
                """
                Raises an exception to simulate a failure.
                """
                raise ValueError("Test failure")

            if (
                state_code == StateCode.US_XX
                and instance == DirectIngestInstance.PRIMARY
            ):
                return fake_failure_task()

            return EmptyOperator(task_id="check_raw_data_max_update_time")

        mock_initialize_dataflow_pipeline.side_effect = (
            fail_for_us_xx_primary_initialize_dataflow_pipeline
        )

        with Session(bind=self.engine) as session:
            dag = create_ingest_dag()
            result = self.run_dag_test(
                dag,
                session=session,
                expected_failure_ids=[
                    ".*us_xx_primary_dataflow.check_raw_data_max_update_time.*",
                    ".*us_xx_primary_dataflow.acquire_lock.*",
                    ".*us_xx_primary_dataflow.dataflow_pipeline.*",
                    ".*us_xx_primary_dataflow.release_lock.*",
                    ".*us_xx_primary_dataflow.write_upper_bounds.*",
                    ".*ingest_branching.branch_end.*",
                ],
            )
            self.assertEqual(DagRunState.FAILED, result.dag_run_state)