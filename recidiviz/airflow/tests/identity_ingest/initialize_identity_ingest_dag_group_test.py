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
"""Tests for initialize_identity_ingest_dag_group.py"""
import unittest
from datetime import datetime
from unittest.mock import patch

from airflow.models.dag import DAG, dag
from airflow.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState
from sqlalchemy.orm import Session

from recidiviz.airflow.dags.identity_ingest.initialize_identity_ingest_dag_group import (
    initialize_identity_ingest_dag_group,
)
from recidiviz.airflow.dags.monitoring.dag_registry import get_identity_ingest_dag_id
from recidiviz.airflow.dags.utils.config_utils import TENANT_FILTER
from recidiviz.airflow.tests.test_utils import AirflowIntegrationTest
from recidiviz.common.constants.states import StateCode

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a disable expression-not-assigned because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned

_PROJECT_ID = "recidiviz-testing"
_VERIFY_PARAMETERS_TASK_ID = "initialize_dag.verify_parameters"
_HANDLE_PARAMS_CHECK_TASK_ID = "initialize_dag.handle_params_check"
_RECORD_PLATFORM_VERSION_TASK_ID = "initialize_dag.record_dag_run_metadata"
_WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID = "initialize_dag.wait_to_continue_or_cancel"
_HANDLE_QUEUEING_RESULT_TASK_ID = "initialize_dag.handle_queueing_result"
_DOWNSTREAM_TASK_ID = "downstream_task"


@dag(
    dag_id=get_identity_ingest_dag_id(_PROJECT_ID),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def _create_test_initialize_identity_ingest_dag() -> None:
    initialize_identity_ingest_dag_group() >> EmptyOperator(task_id=_DOWNSTREAM_TASK_ID)


test_dag: DAG = _create_test_initialize_identity_ingest_dag()


class TestInitializeIdentityIngestDagGroup(unittest.TestCase):
    """Tests for the initialize_identity_ingest_dag_group.py task group."""

    def test_verify_parameters_upstream_of_handle_params_check(self) -> None:
        verify_parameters_task = test_dag.get_task(_VERIFY_PARAMETERS_TASK_ID)
        handle_params_check = test_dag.get_task(_HANDLE_PARAMS_CHECK_TASK_ID)

        self.assertEqual(
            handle_params_check.upstream_task_ids,
            {verify_parameters_task.task_id},
        )

    def test_handle_params_check_upstream_of_record_dag_run_metadata(self) -> None:
        handle_params_check = test_dag.get_task(_HANDLE_PARAMS_CHECK_TASK_ID)
        record_dag_run_metadata = test_dag.get_task(_RECORD_PLATFORM_VERSION_TASK_ID)

        self.assertEqual(
            handle_params_check.downstream_task_ids,
            {record_dag_run_metadata.task_id},
        )
        self.assertEqual(
            record_dag_run_metadata.upstream_task_ids,
            {handle_params_check.task_id},
        )

    def test_record_dag_run_metadata_upstream_of_wait_to_continue_or_cancel(
        self,
    ) -> None:
        record_dag_run_metadata = test_dag.get_task(_RECORD_PLATFORM_VERSION_TASK_ID)
        wait_to_continue_or_cancel = test_dag.get_task(
            _WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID
        )

        self.assertEqual(
            record_dag_run_metadata.downstream_task_ids,
            {wait_to_continue_or_cancel.task_id},
        )
        self.assertEqual(
            wait_to_continue_or_cancel.upstream_task_ids,
            {record_dag_run_metadata.task_id},
        )

    def test_wait_to_continue_or_cancel_upstream_of_handle_queueing_result(
        self,
    ) -> None:
        wait_to_continue_or_cancel = test_dag.get_task(
            _WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID
        )
        handle_queueing_result = test_dag.get_task(_HANDLE_QUEUEING_RESULT_TASK_ID)

        self.assertEqual(
            wait_to_continue_or_cancel.downstream_task_ids,
            {handle_queueing_result.task_id},
        )
        self.assertEqual(
            handle_queueing_result.upstream_task_ids,
            {wait_to_continue_or_cancel.task_id},
        )


@patch(
    "os.environ",
    {
        "GCP_PROJECT": _PROJECT_ID,
    },
)
class TestInitializeIdentityIngestDagGroupIntegration(AirflowIntegrationTest):
    """Integration tests for the initialize_dag task group in
    initialize_identity_ingest_dag_group.py
    """

    def test_successfully_initializes_dag(self) -> None:
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(test_dag, session, run_conf={})
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

    def test_successfully_initializes_dag_with_tenant_filter(self) -> None:
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                test_dag,
                session,
                run_conf={TENANT_FILTER: StateCode.US_XX.value},
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

    def test_unknown_parameters(self) -> None:
        """A misspelled config key fails the run loudly. This guards against the
        DAG's most dangerous footgun: with the typo silently ignored, no tenant
        filter applies and every tenant's pipeline runs."""
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                dag=test_dag,
                session=session,
                run_conf={"tenant": StateCode.US_XX.value},
                expected_failure_task_id_regexes=[_VERIFY_PARAMETERS_TASK_ID],
                expected_skipped_task_id_regexes=[
                    _RECORD_PLATFORM_VERSION_TASK_ID,
                    _WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID,
                    _HANDLE_QUEUEING_RESULT_TASK_ID,
                    _DOWNSTREAM_TASK_ID,
                ],
            )

            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)
            self.assertEqual(
                result.failure_messages[_VERIFY_PARAMETERS_TASK_ID],
                "Unknown configuration parameters supplied: {'tenant'}",
            )

    def test_invalid_tenant_filter(self) -> None:
        """An invalid tenant_filter value fails the run loudly; it would
        otherwise match no branch and the run would silently do nothing."""
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                dag=test_dag,
                session=session,
                run_conf={TENANT_FILTER: "US_NOT_A_TENANT"},
                expected_failure_task_id_regexes=[_VERIFY_PARAMETERS_TASK_ID],
                expected_skipped_task_id_regexes=[
                    _RECORD_PLATFORM_VERSION_TASK_ID,
                    _WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID,
                    _HANDLE_QUEUEING_RESULT_TASK_ID,
                    _DOWNSTREAM_TASK_ID,
                ],
            )

            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)
            self.assertIn(
                "'US_NOT_A_TENANT' is not a valid",
                result.failure_messages[_VERIFY_PARAMETERS_TASK_ID],
            )
