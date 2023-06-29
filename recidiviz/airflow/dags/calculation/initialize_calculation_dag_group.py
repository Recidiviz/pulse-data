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
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Union

from airflow.decorators import task, task_group
from airflow.exceptions import TaskDeferred
from airflow.models import DagRun, TaskInstance
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.utils.context import Context
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a disable expression-not-assigned because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned


INITIALIZE_DAG_GROUP_ID = "initialize_dag"
UPDATE_PARAMETERS_TASK_ID = "verify_and_update_parameters"
UPDATE_PARAMETERS_FULL_TASK_ID = (
    f"{INITIALIZE_DAG_GROUP_ID}.{UPDATE_PARAMETERS_TASK_ID}"
)


@task(task_id=UPDATE_PARAMETERS_TASK_ID)
def verify_and_update_parameters(
    dag_run: Optional[DagRun] = None,
) -> Dict[str, Optional[Union[str, bool]]]:
    """Verifies that the required parameters are set in the dag_run configuration and
    returns additional parameters that can be referenced throughout the DAG run.
    """
    if not dag_run:
        raise ValueError(
            "Dag run not passed to task. Should be automatically set due to function "
            "being a task."
        )
    ingest_instance = get_ingest_instance(dag_run)
    if not ingest_instance:
        raise ValueError("[ingest_instance] must be set in dag_run configuration")

    if ingest_instance not in {instance.value for instance in DirectIngestInstance}:
        raise ValueError(
            f"[ingest_instance] not valid DirectIngestInstance: {ingest_instance}."
        )

    sandbox_prefix: Optional[str] = None
    if ingest_instance == "SECONDARY":
        state_code_filter = get_state_code_filter(dag_run)
        if not state_code_filter:
            raise ValueError(
                "[state_code_filter] must be set in dag_run configuration for SECONDARY "
                "ingest_instance"
            )
        sandbox_prefix = f"{state_code_filter.lower()}_secondary_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}"
        logging.info("Setting sandbox prefix to %s", sandbox_prefix)
    return {
        "sandbox_prefix": sandbox_prefix,
        # TODO(#21079): We need this because we want positive confirmation that the
        #  parameters have been set and can't yet test this in unittests. Remove the
        #  "parameters_set" checks once we can confirm the parameters get set in the
        #  correct task via a unittest.
        "parameters_set": True,
    }


def get_sandbox_prefix(task_instance: TaskInstance) -> Optional[str]:
    parameters_set = task_instance.xcom_pull(
        task_ids=UPDATE_PARAMETERS_FULL_TASK_ID, key="parameters_set"
    )
    if not parameters_set:
        raise ValueError(
            f"Parameters not set in task id {UPDATE_PARAMETERS_FULL_TASK_ID}."
        )

    return task_instance.xcom_pull(
        task_ids=UPDATE_PARAMETERS_FULL_TASK_ID, key="sandbox_prefix"
    )


def get_ingest_instance(dag_run: DagRun) -> str:
    return dag_run.conf.get("ingest_instance").upper()


def get_state_code_filter(dag_run: DagRun) -> Optional[str]:
    return dag_run.conf.get("state_code_filter")


@task.short_circuit(trigger_rule=TriggerRule.ALL_DONE)
def handle_params_check(
    variables: Optional[Dict[str, Optional[Union[str, bool]]]],
) -> bool:
    """Returns True if the DAG should continue, otherwise short circuits."""
    if variables is None:
        logging.info(
            "Found null has_valid_params, indicating that the params check task sensor "
            "failed (crashed) - do not continue."
        )
        return False
    logging.info("Found properly set variables [%s]", variables)
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
    """Returns True if this dag run should be canceled. Will cancel if it is a PRIMARY
    dag run and is not the first or last dag run in the queue.
    """
    return (
        ingest_instance == "PRIMARY"
        and primary_dag_runs[0].run_id != run_id
        and primary_dag_runs[-1].run_id != run_id
    )


def _this_dag_run_can_continue(
    ingest_instance: str, run_id: str, primary_dag_runs: List[DagRun]
) -> bool:
    """Returns True if this dag run can continue. Will continue if it is a SECONDARY dag
    run or if it is the first dag run in the queue.
    """
    return ingest_instance == "SECONDARY" or primary_dag_runs[0].run_id == run_id


class WaitUntilCanContinueOrCancelSensorAsync(BaseSensorOperator):
    """Sensor that waits until a dag run can either continue or needs to be canceled."""

    DEFER_WAIT_TIME_SECONDS = 60

    def execute(
        self, context: Context, event: Any = None  # pylint: disable=unused-argument
    ) -> str:
        """Acts as a queue until the dag run can either continue or needs to be
        canceled.
        """
        dag_run = context["dag_run"]
        if not dag_run:
            raise ValueError(
                "Dag run not passed to task. Should be automatically set due to "
                "function being a task."
            )

        logging.info(
            "Checking if DAG can continue with configuration: %s", dag_run.conf
        )

        ingest_instance = get_ingest_instance(dag_run)
        primary_dag_runs = _get_all_active_primary_dag_runs(dag_run.dag_id)

        logging.info(
            "Found current active primary dag runs: %s",
            [d.run_id for d in primary_dag_runs],
        )

        if _this_dag_run_should_be_canceled(
            ingest_instance=ingest_instance,
            run_id=dag_run.run_id,
            primary_dag_runs=primary_dag_runs,
        ):
            logging.info("Cancelling DAG...")
            return "CANCEL"

        if _this_dag_run_can_continue(
            ingest_instance=ingest_instance,
            run_id=dag_run.run_id,
            primary_dag_runs=primary_dag_runs,
        ):
            logging.info("Continuing DAG...")
            return "CONTINUE"

        logging.info(
            "Cannot continue or cancel this DAG - deferring for %s seconds",
            self.DEFER_WAIT_TIME_SECONDS,
        )
        raise TaskDeferred(
            trigger=TimeDeltaTrigger(timedelta(seconds=self.DEFER_WAIT_TIME_SECONDS)),
            method_name="execute",
        )


@task.short_circuit
def handle_queueing_result(action_type: Optional[str]) -> bool:
    """Returns True if the DAG should continue, otherwise short circuits."""
    if action_type is None:
        logging.info(
            "Found null action_type, indicating that the queueing sensor failed "
            "(crashed) failed - do not continue."
        )
        return False

    logging.info("Found action_type [%s]", action_type)
    return action_type == "CONTINUE"


@task_group(group_id=INITIALIZE_DAG_GROUP_ID)
def initialize_calculation_dag_group() -> Any:
    wait_to_continue_or_cancel = WaitUntilCanContinueOrCancelSensorAsync(
        task_id="wait_to_continue_or_cancel"
    )
    (
        handle_params_check(verify_and_update_parameters())
        >> wait_to_continue_or_cancel
        >> handle_queueing_result(wait_to_continue_or_cancel.output)
    )
