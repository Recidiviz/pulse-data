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
"""Unit tests for JobRun"""
import datetime
from unittest import TestCase
from unittest.mock import patch

from recidiviz.airflow.dags.monitoring.dag_registry import INITIALIZE_DAG_GROUP_ID
from recidiviz.airflow.dags.monitoring.job_run import JobRun, JobRunState, JobRunType

TEST_DAG = "test_dag"


class JobRunTest(TestCase):
    """Tests for the JobRun test."""

    def setUp(self) -> None:
        self.environment_patcher = patch(
            "os.environ",
            {
                "GCP_PROJECT": "recidiviz-testing",
            },
        )
        self.environment_patcher.start()
        self.discrete_params = ["param_a", "param_b"]
        self.get_discrete_configuration_parameters_patcher = patch(
            "recidiviz.airflow.dags.monitoring.utils.get_discrete_configuration_parameters",
            return_value=self.discrete_params,
        )
        self.get_discrete_configuration_parameters_patcher.start()

    def tearDown(self) -> None:
        self.environment_patcher.stop()
        self.get_discrete_configuration_parameters_patcher.stop()

    def test_build_from_airflow_sorts_conf(self) -> None:
        exec_date = datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC)

        run = JobRun.from_airflow_task_instance(
            dag_id="dag_a",
            execution_date=exec_date,
            conf={
                "param_a": "a",
                "param_b": "b",
            },
            task_id="task_a",
            state=1,
            job_type=JobRunType.AIRFLOW_TASK_RUN,
            error_message=None,
            try_number=0,
            max_tries=None,
        )

        run_is_the_same = JobRun.from_airflow_task_instance(
            dag_id="dag_a",
            execution_date=exec_date,
            conf={
                "param_c": "c",
                "param_b": "b",
                "param_a": "a",
            },
            task_id="task_a",
            state=1,
            job_type=JobRunType.AIRFLOW_TASK_RUN,
            error_message=None,
            try_number=0,
            max_tries=None,
        )

        assert run == JobRun(
            dag_id="dag_a",
            execution_date=datetime.datetime(
                2024, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc
            ),
            dag_run_config='{"param_a": "a", "param_b": "b"}',
            job_id="task_a",
            state=JobRunState.UNKNOWN,
            error_message=None,
            job_type=JobRunType.AIRFLOW_TASK_RUN,
            job_run_num=0,
        )
        assert run == run_is_the_same

    def _make_job_run_from_airflow(self, task_id: str, conf: dict) -> JobRun:
        return JobRun.from_airflow_task_instance(
            dag_id=TEST_DAG,
            execution_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
            conf=conf,
            task_id=task_id,
            state=JobRunState.SUCCESS.value,
            job_type=JobRunType.AIRFLOW_TASK_RUN,
            error_message=None,
            try_number=0,
            max_tries=None,
        )

    def test_from_airflow_initialize_dag_sets_null_config(self) -> None:
        """initialize_dag tasks should have dag_run_config=None regardless of conf
        so that a success from any config partition resolves a failure from any other."""
        task_id = f"{INITIALIZE_DAG_GROUP_ID}.verify_parameters"
        no_config_run = self._make_job_run_from_airflow(task_id=task_id, conf={})
        primary_run = self._make_job_run_from_airflow(
            task_id=task_id, conf={"ingest_instance": "PRIMARY"}
        )
        secondary_run = self._make_job_run_from_airflow(
            task_id=task_id, conf={"ingest_instance": "SECONDARY"}
        )

        self.assertIsNone(no_config_run.dag_run_config)
        self.assertIsNone(primary_run.dag_run_config)
        self.assertIsNone(secondary_run.dag_run_config)
        self.assertEqual(no_config_run.unique_key, primary_run.unique_key)
        self.assertEqual(no_config_run.unique_key, secondary_run.unique_key)
        self.assertEqual(primary_run.unique_key, secondary_run.unique_key)

    def test_from_airflow_non_initialize_dag_respects_config(self) -> None:
        """Non-initialize_dag tasks should still partition by config."""
        task_id = "some_other_task"
        no_config_run = self._make_job_run_from_airflow(task_id=task_id, conf={})
        primary_run = self._make_job_run_from_airflow(
            task_id=task_id, conf={"param_a": "PRIMARY"}
        )
        secondary_run = self._make_job_run_from_airflow(
            task_id=task_id, conf={"param_a": "SECONDARY"}
        )

        self.assertNotEqual(no_config_run.unique_key, primary_run.unique_key)
        self.assertNotEqual(no_config_run.unique_key, secondary_run.unique_key)
        self.assertNotEqual(primary_run.unique_key, secondary_run.unique_key)
