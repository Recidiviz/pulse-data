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
"""
Helper functions containing tasks that are shared by mutliple dags.
"""
import logging
from enum import Enum
from typing import Optional

from airflow.decorators import task
from airflow.models import DagRun
from airflow.utils.trigger_rule import TriggerRule


class QueuingActionType(Enum):
    """Possible queuing action return types from WaitUntilCanContinueOrCancelSensorAsync."""

    # CONTINUE means that the queued DagRun will proceed normally, starting with tasks
    # directly downstream from `handle_queueing_result`
    CONTINUE = "CONTINUE"
    # CANCEL means that all tasks in the queued DagRun will be skipped by `handle_queueing_result`
    # effectively canceling the current DagRun.
    CANCEL = "CANCEL"


INGEST_INSTANCE = "ingest_instance"
SANDBOX_PREFIX = "sandbox_prefix"
STATE_CODE_FILTER = "state_code_filter"


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


@task.short_circuit
def handle_queueing_result(action_type: Optional[str]) -> bool:
    """Returns True if the DAG should continue, otherwise short circuits."""
    if action_type is None:
        logging.error(
            "Found null action_type, indicating that the queueing sensor failed "
            "(crashed) failed - do not continue."
        )
        return False

    try:
        action_type_enum = QueuingActionType(action_type)
    except Exception:
        logging.error(
            "Found unrecognized action_type [%s] -- do not continue.", action_type
        )
        return False

    logging.info("Found action_type [%s]", action_type_enum)
    return action_type_enum == QueuingActionType.CONTINUE


def get_ingest_instance(dag_run: DagRun) -> Optional[str]:
    ingest_instance = dag_run.conf.get(INGEST_INSTANCE)
    return ingest_instance.upper() if ingest_instance else None


def get_state_code_filter(dag_run: DagRun) -> Optional[str]:
    state_code_filter = dag_run.conf.get(STATE_CODE_FILTER)
    return state_code_filter.upper() if state_code_filter else None
