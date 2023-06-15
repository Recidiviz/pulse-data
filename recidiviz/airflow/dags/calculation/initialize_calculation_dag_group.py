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
"""Handles queueing of calculation dags."""
import logging
from datetime import timedelta
from typing import Any, List, Optional, Set

from airflow.decorators import task, task_group
from airflow.exceptions import TaskDeferred
from airflow.models import DagRun
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.utils.context import Context
from airflow.utils.state import State

from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a disable expression-not-assigned because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned


@task.short_circuit(task_id="verify_required_parameters")
def _verify_required_parameters(dag_run: Optional[DagRun] = None) -> bool:
    """Verifies that the required parameters are set in the dag_run configuration."""
    if not dag_run:
        logging.error(
            "Dag run not passed to task. Should be automatically set due to function being a task."
        )
        return False
    ingest_instance = dag_run.conf.get("ingest_instance")
    if not ingest_instance:
        logging.error("[ingest_instance] must be set in dag_run configuration")
        return False

    if ingest_instance not in {instance.value for instance in DirectIngestInstance}:
        logging.error("[ingest_instance] not valid DirectIngestInstance.")
        return False

    state_code_filter = dag_run.conf.get("state_code_filter")
    if ingest_instance.upper() == "SECONDARY" and not state_code_filter:
        logging.error(
            "[state_code_filter] must be set in dag_run configuration for SECONDARY ingest_instance"
        )
        return False
    return True


def _get_all_active_primary_dag_runs(dag_id: str) -> List[DagRun]:
    """Returns all active primary dag runs for a given dag_id."""
    running_dags: Set[DagRun] = DagRun.find(dag_id=dag_id, state=State.RUNNING)
    primary_dag_runs = [
        dag_run
        for dag_run in running_dags
        if dag_run.conf["ingest_instance"].upper() == "PRIMARY"
    ]
    return sorted(primary_dag_runs, key=lambda dag_run: dag_run.execution_date)


def _this_dag_run_should_be_canceled(
    ingest_instance: str, run_id: str, primary_dag_runs: List[DagRun]
) -> bool:
    """Returns True if this dag run should be canceled. Will cancel if it is a PRIMARY dag run and
    is not the first or last dag run in the queue."""
    return (
        ingest_instance == "PRIMARY"
        and primary_dag_runs[0].run_id != run_id
        and primary_dag_runs[-1].run_id != run_id
    )


def _this_dag_run_can_continue(
    ingest_instance: str, run_id: str, primary_dag_runs: List[DagRun]
) -> bool:
    """Returns True if this dag run can continue. Will continue if it is a SECONDARY dag run or
    if it is the first dag run in the queue."""
    return ingest_instance == "SECONDARY" or primary_dag_runs[0].run_id == run_id


class WaitUntilCanContinueOrCancelSensorAsync(BaseSensorOperator):
    """Sensor that waits until a dag run can either continue or needs to be canceled."""

    def execute(
        self, context: Context, event: Any = None  # pylint: disable=unused-argument
    ) -> str:
        """Acts as a queue until the dag run can either continue or needs to be canceled."""
        dag_run = context["dag_run"]
        if not dag_run:
            raise ValueError(
                "Dag run not passed to task. Should be automatically set due to function being a task."
            )

        ingest_instance = dag_run.conf.get("ingest_instance")
        primary_dag_runs = _get_all_active_primary_dag_runs(dag_run.dag_id)

        if _this_dag_run_should_be_canceled(
            ingest_instance=ingest_instance,
            run_id=dag_run.run_id,
            primary_dag_runs=primary_dag_runs,
        ):
            return "CANCEL"

        if _this_dag_run_can_continue(
            ingest_instance=ingest_instance,
            run_id=dag_run.run_id,
            primary_dag_runs=primary_dag_runs,
        ):
            return "CONTINUE"

        self.defer(
            trigger=TimeDeltaTrigger(timedelta(seconds=60)), method_name="execute"
        )
        # Code passed this point should not be called as self.defer will raise an exception
        raise TaskDeferred(trigger=(timedelta(seconds=60)), method_name="execute")


@task.short_circuit(task_id="handle_short_circuiting")
def _handle_short_circuiting(action_type: str) -> bool:
    """Returns True if the DAG should continue, otherwise short circuits."""
    return action_type == "CONTINUE"


@task_group(group_id="initialize_dag")
def initialize_calculation_dag_group() -> Any:
    wait_to_continue_or_cancel = WaitUntilCanContinueOrCancelSensorAsync(
        task_id="wait_to_continue_or_cancel"
    )
    (
        _verify_required_parameters()
        >> wait_to_continue_or_cancel
        >> _handle_short_circuiting(wait_to_continue_or_cancel.output)
    )
