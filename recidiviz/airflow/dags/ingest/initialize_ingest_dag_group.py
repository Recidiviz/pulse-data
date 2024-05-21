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
"""Handles queueing of ingest dags."""
from typing import List, Optional

from airflow.decorators import task, task_group
from airflow.models import DagRun

from recidiviz.airflow.dags.monitoring.dag_registry import (
    get_known_configuration_parameters,
)
from recidiviz.airflow.dags.operators.wait_until_can_continue_or_cancel_sensor_async import (
    WaitUntilCanContinueOrCancelDelegate,
    WaitUntilCanContinueOrCancelSensorAsync,
)
from recidiviz.airflow.dags.utils.config_utils import (
    get_ingest_instance,
    get_state_code_filter,
    handle_params_check,
    handle_queueing_result,
)
from recidiviz.airflow.dags.utils.dag_orchestration_utils import (
    get_ingest_pipeline_enabled_state_and_instance_pairs,
)
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a disable expression-not-assigned because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned


@task
def verify_parameters(dag_run: Optional[DagRun] = None) -> bool:
    """Verifies that the required parameters are set in the dag_run configuration."""
    if not dag_run:
        raise ValueError(
            "Dag run not passed to task. Should be automatically set due to function "
            "being a task."
        )

    unknown_parameters = {
        parameter
        for parameter in dag_run.conf.keys()
        if parameter
        not in get_known_configuration_parameters(
            project_id=get_project_id(), dag_id=dag_run.dag_id
        )
    }

    if unknown_parameters:
        raise ValueError(
            f"Unknown configuration parameters supplied: {unknown_parameters}"
        )

    ingest_instance = get_ingest_instance(dag_run)
    state_code_filter = get_state_code_filter(dag_run)

    if (ingest_instance and not state_code_filter) or (
        state_code_filter and not ingest_instance
    ):
        raise ValueError(
            "[ingest_instance] and [state_code_filter] must both be set or both be unset."
        )

    if (state_code_filter and ingest_instance) and (
        StateCode(state_code_filter),
        DirectIngestInstance(ingest_instance),
    ) not in get_ingest_pipeline_enabled_state_and_instance_pairs():
        raise ValueError(
            f"{state_code_filter} in {ingest_instance} must be a enabled for ingest. "
            f"Valid pairs are: {get_ingest_pipeline_enabled_state_and_instance_pairs()}"
        )

    return True


def _dag_run_is_state_agnostic(dag_run: DagRun) -> bool:
    """Returns true if the dag run is state agnostic."""

    state_code_filter = get_state_code_filter(dag_run)
    ingest_instance = get_ingest_instance(dag_run)

    if (not state_code_filter and ingest_instance) or (
        state_code_filter and not ingest_instance
    ):
        raise ValueError(
            f"Unexpected combination of parameters: [{ingest_instance=}], "
            f"[{state_code_filter=}]. Expected either both to be set or both to be "
            f"unset."
        )

    return not state_code_filter and not ingest_instance


def _dag_runs_have_same_filter(dag_run_1: DagRun, dag_run_2: DagRun) -> bool:
    """Returns true if the dag runs have the same filter."""
    return get_state_code_filter(dag_run_1) == get_state_code_filter(
        dag_run_2
    ) and get_ingest_instance(dag_run_1) == get_ingest_instance(dag_run_2)


def _get_state_agnostic_active_runs(active_dag_runs: List[DagRun]) -> List[DagRun]:
    """Returns all state-agnostic DAG runs in the provided list"""
    return [
        dag_run for dag_run in active_dag_runs if _dag_run_is_state_agnostic(dag_run)
    ]


def _is_first(dag_runs: List[DagRun], dag_run: DagRun) -> bool:
    """If True, the |dag_run| is the first DAG in the |dag_runs| list."""
    return dag_runs[0].run_id == dag_run.run_id


def _is_last(dag_runs: List[DagRun], dag_run: DagRun) -> bool:
    """If True, the |dag_run| is the last DAG in the |dag_runs| list."""
    return dag_runs[-1].run_id == dag_run.run_id


def _dag_run_2_is_superset_of_1(dag_run_1: DagRun, dag_run_2: DagRun) -> bool:
    """If True, |dag_run_2| will execute a superset of pipelines run in |dag_run_1|."""
    return _dag_run_is_state_agnostic(dag_run_2) or _dag_runs_have_same_filter(
        dag_run_1, dag_run_2
    )


class IngestDagWaitUntilCanContinueOrCancelDelegate(
    WaitUntilCanContinueOrCancelDelegate
):
    """
    Delegate for the WaitUntilCanContinueOrCancelSensorAsync that determines if a
    dag run can continue or should be canceled.
    """

    def this_dag_run_can_continue(
        self, dag_run: DagRun, all_active_dag_runs: List[DagRun]
    ) -> bool:
        """Returns True if this dag run can continue. Will continue if it is a SECONDARY
        dag run or if it is the first dag run in the queue.
        """

        if _dag_run_is_state_agnostic(dag_run):
            return _is_first(all_active_dag_runs, dag_run)

        state_agnostic_or_same_filter_dag_runs = [
            other_dag_run
            for other_dag_run in all_active_dag_runs
            if _dag_run_2_is_superset_of_1(dag_run, other_dag_run)
        ]

        return _is_first(state_agnostic_or_same_filter_dag_runs, dag_run)

    def this_dag_run_should_be_canceled(
        self, dag_run: DagRun, all_active_dag_runs: List[DagRun]
    ) -> bool:
        """Returns True if this dag run should be canceled. Will cancel if it is a
        PRIMARY dag run and is not the first or last dag run in the queue.
        """

        if _dag_run_is_state_agnostic(dag_run):
            agnostic_dag_runs = _get_state_agnostic_active_runs(all_active_dag_runs)
            return not _is_last(agnostic_dag_runs, dag_run)

        state_agnostic_or_same_filter_dag_runs = [
            other_dag_run
            for other_dag_run in all_active_dag_runs
            if _dag_run_2_is_superset_of_1(dag_run, other_dag_run)
        ]

        # Should be cancelled if this is not the most recently queued version of the
        # DAG that runs this state.
        return not _is_last(state_agnostic_or_same_filter_dag_runs, dag_run)


@task_group(group_id="initialize_ingest_dag")
def create_initialize_ingest_dag() -> None:
    """
    Creates the initialize ingest dag.
    """
    wait_to_continue_or_cancel = WaitUntilCanContinueOrCancelSensorAsync(
        task_id="wait_to_continue_or_cancel",
        delegate=IngestDagWaitUntilCanContinueOrCancelDelegate(),
    )
    (
        handle_params_check(verify_parameters())
        >> wait_to_continue_or_cancel
        >> handle_queueing_result(wait_to_continue_or_cancel.output)
    )
