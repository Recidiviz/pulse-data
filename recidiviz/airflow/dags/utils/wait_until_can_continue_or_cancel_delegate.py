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
"""Shared implementations of WaitUntilCanContinueOrCancelDelegate"""
from typing import List

from airflow.models import DagRun

from recidiviz.airflow.dags.operators.wait_until_can_continue_or_cancel_sensor_async import (
    WaitUntilCanContinueOrCancelDelegate,
)
from recidiviz.airflow.dags.utils.config_utils import get_ingest_instance
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


class StateSpecificNonBlockingDagWaitUntilCanContinueOrCancelDelegate(
    WaitUntilCanContinueOrCancelDelegate
):
    """Delegate for the WaitUntilCanContinueOrCancelSensorAsync that determines if a
    dag run can continue or should be canceled for DAGs that have state_code and
    ingest_instance as parameters and want state-specific runs to be non-blocking for
    state-agnostic runs.
    """

    def this_dag_run_can_continue(
        self, dag_run: DagRun, all_active_dag_runs: List[DagRun]
    ) -> bool:
        """Returns True if this dag run can continue. Will continue if it is a SECONDARY
        dag run or if it is the first dag run in the queue.
        """
        ingest_instance = get_ingest_instance(dag_run)
        if not ingest_instance:
            raise ValueError("[ingest_instance] must be set in dag_run configuration")
        primary_dag_runs = self._filter_to_primary_dag_runs(all_active_dag_runs)
        return (
            ingest_instance == DirectIngestInstance.SECONDARY.value
            or primary_dag_runs[0].run_id == dag_run.run_id
        )

    def this_dag_run_should_be_canceled(
        self, dag_run: DagRun, all_active_dag_runs: List[DagRun]
    ) -> bool:
        """Returns True if this dag run should be canceled. Will cancel if it is a
        PRIMARY dag run and is not the first or last dag run in the queue.
        """
        ingest_instance = get_ingest_instance(dag_run)
        if not ingest_instance:
            raise ValueError("[ingest_instance] must be set in dag_run configuration")
        primary_dag_runs = self._filter_to_primary_dag_runs(all_active_dag_runs)
        return (
            ingest_instance == DirectIngestInstance.PRIMARY.value
            and primary_dag_runs[0].run_id != dag_run.run_id
            and primary_dag_runs[-1].run_id != dag_run.run_id
        )

    @staticmethod
    def _filter_to_primary_dag_runs(dag_runs: List[DagRun]) -> List[DagRun]:
        return [
            dag_run
            for dag_run in dag_runs
            if dag_run.conf["ingest_instance"].upper()
            == DirectIngestInstance.PRIMARY.value
        ]
