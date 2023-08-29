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
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.airflow.dags.monitoring.task_failure_alerts import (
    KNOWN_CONFIGURATION_PARAMETERS,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a disable expression-not-assigned because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned


INITIALIZE_DAG_GROUP_ID = "initialize_dag"


INGEST_INSTANCE = "ingest_instance"
SANDBOX_PREFIX = "sandbox_prefix"
STATE_CODE_FILTER = "state_code_filter"


def templated_argument_from_conf(
    conf_key: str,
    entrypoint_argument: Optional[str] = None,
    jinja_filter: Optional[str] = None,
) -> str:
    """
    Returns a Jinja-templated entrypoint argument
    example:
    {% if 'ingest_instance' in dag_run.conf %}--ingest_instance={{dag_run.conf["ingest_instance"]}}{% endif %}
    """
    if entrypoint_argument is None:
        entrypoint_argument = conf_key

    jinja_filter_str = ""
    if jinja_filter:
        jinja_filter_str = f"| {jinja_filter}"

    return (
        f"{{% if '{conf_key}' in dag_run.conf %}}"
        f'--{entrypoint_argument}={{{{ dag_run.conf["{entrypoint_argument}"] {jinja_filter_str} }}}}'
        "{% endif %}"
    )


INGEST_INSTANCE_JINJA_ARG = templated_argument_from_conf(
    INGEST_INSTANCE, jinja_filter="upper"
)
SANDBOX_PREFIX_JINJA_ARG = templated_argument_from_conf(SANDBOX_PREFIX)
STATE_CODE_FILTER_JINJA_ARG = templated_argument_from_conf(
    STATE_CODE_FILTER, jinja_filter="upper"
)


@task
def verify_parameters(dag_run: Optional[DagRun] = None) -> bool:
    """Verifies that the required parameters are set in the dag_run configuration and
    returns additional parameters that can be referenced throughout the DAG run.
    """
    if not dag_run:
        raise ValueError(
            "Dag run not passed to task. Should be automatically set due to function "
            "being a task."
        )

    unknown_parameters = {
        parameter
        for parameter in dag_run.conf.keys()
        if parameter not in KNOWN_CONFIGURATION_PARAMETERS[dag_run.dag_id]
    }

    if unknown_parameters:
        raise ValueError(
            f"Unknown configuration parameters supplied: {unknown_parameters}"
        )

    ingest_instance = get_ingest_instance(dag_run)
    if not ingest_instance:
        raise ValueError("[ingest_instance] must be set in dag_run configuration")

    if ingest_instance not in {instance.value for instance in DirectIngestInstance}:
        raise ValueError(
            f"[ingest_instance] not valid DirectIngestInstance: {ingest_instance}."
        )

    sandbox_prefix: Optional[str] = get_sandbox_prefix(dag_run)
    if ingest_instance == "SECONDARY":
        state_code_filter = get_state_code_filter(dag_run)
        if not state_code_filter:
            raise ValueError(
                "[state_code_filter] must be set in dag_run configuration for SECONDARY "
                "ingest_instance"
            )
        if not sandbox_prefix:
            raise ValueError(
                "[sandbox_prefix] must be set in dag_run configuration for SECONDARY "
                "ingest_instance"
            )
    return True


def get_sandbox_prefix(dag_run: DagRun) -> Optional[str]:
    return dag_run.conf.get(SANDBOX_PREFIX)


def get_ingest_instance(dag_run: DagRun) -> str:
    return dag_run.conf.get(INGEST_INSTANCE).upper()


def get_state_code_filter(dag_run: DagRun) -> Optional[str]:
    state_code_filter = dag_run.conf.get(STATE_CODE_FILTER)

    if state_code_filter:
        return state_code_filter.upper()

    return None


@task.short_circuit(trigger_rule=TriggerRule.ALL_DONE)
def handle_params_check(
    variables_verified: bool,
) -> bool:
    """Returns True if the DAG should continue, otherwise short circuits."""
    if not variables_verified:
        logging.info(
            "variables_verified did not return true, indicating that the params check task sensor "
            "failed (crashed) - do not continue."
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
        handle_params_check(verify_parameters())
        >> wait_to_continue_or_cancel
        >> handle_queueing_result(wait_to_continue_or_cancel.output)
    )
