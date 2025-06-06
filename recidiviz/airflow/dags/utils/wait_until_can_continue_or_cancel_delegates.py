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
from recidiviz.airflow.dags.utils.config_utils import (
    get_ingest_instance,
    get_state_code_filter,
)


class StateSpecificNonBlockingDagWaitUntilCanContinueOrCancelDelegate(
    WaitUntilCanContinueOrCancelDelegate
):
    """Delegate for the WaitUntilCanContinueOrCancelSensorAsync that determines if a
    dag run can continue or should be canceled for DAGs that have state_code_filter and
    ingest_instance as parameters and want state-specific runs to be non-blocking for
    state-agnostic runs.

    n.b. this delegate should only be used for DAGs that are not worried about a
    state-specific and state-agnostic running concurrently.
    """

    def this_dag_run_can_continue(
        self, dag_run: DagRun, all_active_dag_runs: List[DagRun]
    ) -> bool:
        """Returns True if this dag run can continue. If |dag_run| is a
        state-agnostic DagRun, it will continue iff it is the first DagRun in the queue
        of matching state-agnostic DagRuns with the same ingest_instances. If
        |dag_run| is a state-specific DagRun, it will continue iff it is the first DagRun
        in the queue of state-agnostic and matching state-specific DagRuns with the sane
        ingest_instance.
        """
        ingest_instance = get_ingest_instance(dag_run)
        if not ingest_instance:
            raise ValueError("[ingest_instance] must be set in dag_run configuration")

        state_code_filter = get_state_code_filter(dag_run)
        is_state_agnostic_dag = not bool(state_code_filter)

        if is_state_agnostic_dag:
            state_agnostic_dag_runs = self._filter_to_state_agnostic_dag_runs(
                all_active_dag_runs, ingest_instance=ingest_instance
            )
            return state_agnostic_dag_runs[0].run_id == dag_run.run_id

        # if this is a state agnostic DAG run, we will only continue if it the first
        # state agnostic DAG in the queue w/ a matching ingest_instance
        state_agnostic_and_matching_state_specific_dag_runs = (
            self._filter_to_state_agnostic_or_matching_state_specific_dag_runs(
                all_active_dag_runs,
                ingest_instance=ingest_instance,
                state_code_filter=state_code_filter,
            )
        )

        # if this is a state specific DAG run, we will only continue if it the first in
        # the queue of state agnostic and matching state specific DAG runs
        return (
            state_agnostic_and_matching_state_specific_dag_runs[0].run_id
            == dag_run.run_id
        )

    def this_dag_run_should_be_canceled(
        self, dag_run: DagRun, all_active_dag_runs: List[DagRun]
    ) -> bool:
        """Returns True if this dag run should be canceled. If |dag_run| is a
        state-agnostic DagRun, it will cancel iff it is not the first or last DagRun
        in the queue of matching state-agnostic DagRuns with the same ingest_instances. If
        |dag_run| is a state-specific DagRun, it will cancel iff it is not the first or
        last DagRun in the queue of state-agnostic and matching state-specific DagRuns
        with the sane ingest_instance.
        """

        ingest_instance = get_ingest_instance(dag_run)
        if not ingest_instance:
            raise ValueError("[ingest_instance] must be set in dag_run configuration")

        state_code_filter = get_state_code_filter(dag_run)
        is_state_agnostic_dag = not bool(state_code_filter)

        # if this is a state agnostic DAG run, we will only cancel if it is not the
        # first or last state agnostic DAG in the queue w/ a matching ingest_instance
        if is_state_agnostic_dag:
            state_agnostic_dag_runs = self._filter_to_state_agnostic_dag_runs(
                all_active_dag_runs, ingest_instance=ingest_instance
            )

            return dag_run.run_id not in {
                state_agnostic_dag_runs[0].run_id,
                state_agnostic_dag_runs[-1].run_id,
            }

        state_agnostic_and_matching_state_specific_dag_runs = (
            self._filter_to_state_agnostic_or_matching_state_specific_dag_runs(
                all_active_dag_runs,
                ingest_instance=ingest_instance,
                state_code_filter=state_code_filter,
            )
        )

        # if this is a state specific DAG run, we will only cancel if it is not the
        # first or last DAG in the queue of state agnostic and matching state specific
        # DAG runs
        return dag_run.run_id not in {
            state_agnostic_and_matching_state_specific_dag_runs[0].run_id,
            state_agnostic_and_matching_state_specific_dag_runs[-1].run_id,
        }

    @staticmethod
    def _filter_to_state_agnostic_dag_runs(
        dag_runs: List[DagRun],
        ingest_instance: str,
    ) -> List[DagRun]:
        return [
            dag_run
            for dag_run in dag_runs
            if (
                dag_run.conf["ingest_instance"].upper() == ingest_instance.upper()
                and not dag_run.conf.get("state_code_filter")
            )
        ]

    @staticmethod
    def _filter_to_state_agnostic_or_matching_state_specific_dag_runs(
        dag_runs: List[DagRun],
        ingest_instance: str,
        state_code_filter: str | None,
    ) -> List[DagRun]:
        return [
            dag_run
            for dag_run in dag_runs
            if (
                _dag_run_matches_instance(dag_run, ingest_instance)
                and not dag_run.conf.get("state_code_filter")
            )
            or (
                _dag_run_matches_instance(dag_run, ingest_instance)
                and dag_run.conf.get("state_code_filter") == state_code_filter
            )
        ]


def _dag_run_matches_instance(dag_run: DagRun, ingest_instance: str) -> bool:
    """Helper for determining if a |dag_run| has |ingest_instance| in it's run
    configuration.
    """
    return dag_run.conf["ingest_instance"].upper() == ingest_instance.upper()


class NoConcurrentDagsWaitUntilCanContinueOrCancelDelegate(
    WaitUntilCanContinueOrCancelDelegate
):
    """Delegate for the WaitUntilCanContinueOrCancelSensorAsync that determines if a
    dag run can continue or should be canceled for DAGs that should only ever have one
    instance of that DAG running at once.
    """

    def this_dag_run_can_continue(
        self, dag_run: DagRun, all_active_dag_runs: List[DagRun]
    ) -> bool:
        """Returns True if this dag run can continue. Will continue if it is the first
        dag run in the queue.
        """
        return all_active_dag_runs[0].run_id == dag_run.run_id

    def this_dag_run_should_be_canceled(
        self, dag_run: DagRun, all_active_dag_runs: List[DagRun]
    ) -> bool:
        """Returns True if this dag run should be canceled. Will cancel if it is a
        is not the first or last dag run in the queue.
        """
        return dag_run.run_id not in {
            all_active_dag_runs[0].run_id,
            all_active_dag_runs[-1].run_id,
        }
