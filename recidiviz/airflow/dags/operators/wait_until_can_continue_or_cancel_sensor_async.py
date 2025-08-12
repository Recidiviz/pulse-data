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
"""Manages queueing logic of DAG runs."""
import abc
import logging
from datetime import timedelta
from typing import Any, List

from airflow.exceptions import TaskDeferred
from airflow.models import DagRun
from airflow.sensors.base import BaseSensorOperator
from airflow.serialization.pydantic.dag_run import DagRunPydantic
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.utils.context import Context
from airflow.utils.state import DagRunState

from recidiviz.airflow.dags.utils.config_utils import QueuingActionType


class WaitUntilCanContinueOrCancelDelegate:
    @abc.abstractmethod
    def this_dag_run_can_continue(
        self, dag_run: DagRun | DagRunPydantic, all_active_dag_runs: List[DagRun]
    ) -> bool:
        """Returns True if this dag run can continue."""

    @abc.abstractmethod
    def this_dag_run_should_be_canceled(
        self, dag_run: DagRun | DagRunPydantic, all_active_dag_runs: List[DagRun]
    ) -> bool:
        """Returns True if this dag run should be canceled."""


def _get_all_active_dag_runs(dag_id: str) -> List[DagRun]:
    """Returns all active dag runs for the given dag run."""
    running_dags: list[DagRun] = DagRun.find(dag_id=dag_id, state=DagRunState.RUNNING)
    return sorted(running_dags, key=lambda dag_run: dag_run.execution_date)


def _get_pretty_dag_run_identifier(dag_run: DagRun | DagRunPydantic) -> str:
    """Returns a pretty string representation of a dag run."""
    return f"<{dag_run.run_id} {dag_run.conf}>"


class WaitUntilCanContinueOrCancelSensorAsync(BaseSensorOperator):
    """Sensor that waits until a dag run can either continue or needs to be canceled."""

    DEFER_WAIT_TIME_SECONDS = 60

    def __init__(
        self, delegate: WaitUntilCanContinueOrCancelDelegate, *args: Any, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self.delegate = delegate

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

        all_active_dag_runs = _get_all_active_dag_runs(dag_run.dag_id)

        logging.info(
            "Found current active dag runs: %s",
            [d.run_id for d in all_active_dag_runs],
        )

        if self.delegate.this_dag_run_should_be_canceled(
            dag_run=dag_run, all_active_dag_runs=all_active_dag_runs
        ):
            logging.info(
                "Cancelling DAG %s. Current running DAGs: %s",
                _get_pretty_dag_run_identifier(dag_run),
                [_get_pretty_dag_run_identifier(d) for d in all_active_dag_runs],
            )
            return QueuingActionType.CANCEL.value

        if self.delegate.this_dag_run_can_continue(
            dag_run=dag_run, all_active_dag_runs=all_active_dag_runs
        ):
            logging.info(
                "Continuing DAG %s. Current running DAGs: %s",
                _get_pretty_dag_run_identifier(dag_run),
                [_get_pretty_dag_run_identifier(d) for d in all_active_dag_runs],
            )
            return QueuingActionType.CONTINUE.value

        logging.info(
            "Cannot continue or cancel this DAG - deferring for %s seconds",
            self.DEFER_WAIT_TIME_SECONDS,
        )
        raise TaskDeferred(
            trigger=TimeDeltaTrigger(timedelta(seconds=self.DEFER_WAIT_TIME_SECONDS)),
            method_name="execute",
        )
