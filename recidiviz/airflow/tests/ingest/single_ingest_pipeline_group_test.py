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
"""Tests for the state dataflow pipeline."""

import os
import unittest
from datetime import datetime
from typing import Dict, List
from unittest.mock import patch

import yaml
from airflow.models.dag import DAG, dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

from recidiviz.airflow.dags.ingest.ingest_branching import get_ingest_branch_key
from recidiviz.airflow.dags.ingest.single_ingest_pipeline_group import (
    _acquire_lock,
    create_single_ingest_pipeline_group,
)
from recidiviz.airflow.tests import fixtures
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCPEnvironment

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a disable expression-not-assigned because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned

_PROJECT_ID = "recidiviz-testing"
_TEST_DAG_ID = "test_single_ingest_pipeline_group"
_DOWNSTREAM_TASK_ID = "downstream_task"
_US_XX_PRIMARY_ACQUIRE_LOCK_TASK_ID = f"{get_ingest_branch_key(StateCode.US_XX.value, DirectIngestInstance.PRIMARY.value)}.acquire_lock"


def _create_test_single_ingest_pipeline_group_dag(
    state_code: StateCode, instance: DirectIngestInstance
) -> DAG:
    @dag(
        dag_id=_TEST_DAG_ID,
        start_date=datetime(2022, 1, 1),
        schedule=None,
        catchup=False,
    )
    def test_single_ingest_pipeline_group_dag() -> None:
        create_single_ingest_pipeline_group(state_code, instance) >> EmptyOperator(
            task_id=_DOWNSTREAM_TASK_ID
        )

    return test_single_ingest_pipeline_group_dag()


class TestSingleIngestPipelineGroup(unittest.TestCase):
    """Tests for the single ingest pipeline group ."""

    INGEST_DAG_ID = f"{_PROJECT_ID}_ingest_dag"
    entrypoint_args_fixture: Dict[str, List[str]] = {}

    @classmethod
    def setUpClass(cls) -> None:
        with open(
            os.path.join(os.path.dirname(fixtures.__file__), "./entrypoints_args.yaml"),
            "r",
            encoding="utf-8",
        ) as fixture_file:
            cls.entrypoint_args_fixture = yaml.safe_load(fixture_file)

    def setUp(self) -> None:
        self.environment_patcher = patch(
            "os.environ",
            {
                "GCP_PROJECT": _PROJECT_ID,
            },
        )
        self.environment_patcher.start()
        self.project_environment_patcher = patch(
            "recidiviz.utils.environment.get_environment_for_project",
            return_value=GCPEnvironment.STAGING,
        )
        self.project_environment_patcher.start()

    def tearDown(self) -> None:
        self.environment_patcher.stop()
        self.project_environment_patcher.stop()

    def test_acquire_lock_task_exists(self) -> None:
        """Tests that acquire_lock triggers the proper script."""

        test_dag = _create_test_single_ingest_pipeline_group_dag(
            StateCode.US_XX, DirectIngestInstance.PRIMARY
        )

        acquire_lock_task = test_dag.get_task(_US_XX_PRIMARY_ACQUIRE_LOCK_TASK_ID)

        if not isinstance(acquire_lock_task, KubernetesPodOperator):
            raise ValueError(
                f"Expected type KubernetesPodOperator, found "
                f"[{type(acquire_lock_task)}]."
            )

    def test_acquire_lock_task(
        self,
    ) -> None:
        """Tests that refresh_bq_dataset_task triggers the proper script."""

        task = _acquire_lock(StateCode.US_XX, DirectIngestInstance.PRIMARY)

        self.assertEqual(task.task_id, "acquire_lock")
        self.assertEqual(
            task.arguments[4:],
            self.entrypoint_args_fixture["test_ingest_dag_acquire_lock_task"],
        )
