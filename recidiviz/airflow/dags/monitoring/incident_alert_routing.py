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
"""Helpers for routing Airflow task failures to the correct service."""

import re
from typing import Optional

from more_itertools import one

from recidiviz.airflow.dags.calculation.constants import (
    STATE_SPECIFIC_METRIC_EXPORTS_GROUP_ID,
)
from recidiviz.airflow.dags.monitoring.airflow_alerting_incident import (
    AirflowAlertingIncident,
)
from recidiviz.airflow.dags.monitoring.dag_registry import get_sftp_dag_id
from recidiviz.airflow.dags.utils.branching_by_key import (
    BRANCH_END_TASK_NAME,
    BRANCH_START_TASK_NAME,
)
from recidiviz.airflow.dags.utils.constants import (
    DATAFLOW_OPERATOR_TASK_ID,
    SHOULD_RUN_BASED_ON_WATERMARKS_TASK_ID,
)
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.airflow.dags.utils.recidiviz_pagerduty_service import (
    RecidivizPagerDutyService,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.types import assert_type

_STATE_CODE_BEGINNING_REGEX = re.compile(r"^(?P<state_code>US_[A-Z]{2})(_|$)")
_STATE_CODE_END_REGEX = re.compile(r".*_(?P<state_code>US_[A-Z]{2})$")
_STATE_CODE_MIDDLE_REGEX = re.compile(r"_(?P<state_code>US_[A-Z]{2})_")


def _state_code_from_task_id_part(task_id_part: str) -> Optional[StateCode]:
    normalized_part = task_id_part.upper().replace("-", "_")
    matches = [
        re.match(_STATE_CODE_BEGINNING_REGEX, normalized_part),
        re.match(_STATE_CODE_END_REGEX, normalized_part),
        *list(re.finditer(_STATE_CODE_MIDDLE_REGEX, normalized_part)),
    ]
    state_codes = {m.group("state_code") for m in matches if m is not None}
    if not state_codes:
        return None
    if len(state_codes) > 1:
        raise ValueError(
            f"Task id part [{task_id_part}] references more than one state code: "
            f"{sorted(state_codes)}. Each task should only reference at most one state code."
        )
    state_code_str = one(state_codes)
    return StateCode(state_code_str)


def _state_code_from_task_id(task_id: str) -> Optional[StateCode]:
    """Returns the single state code that the provided task_id should be attributed to.
    Throws if more than one state code is referenced in the task_id.
    """
    state_code: Optional[StateCode] = None
    for part in task_id.split("."):
        if part_state_code := _state_code_from_task_id_part(part):
            if state_code and state_code != part_state_code:
                raise ValueError(
                    f"Found task_id [{task_id}] referencing more than one state code. "
                    f"References [{state_code.value}] and [{part_state_code.value}]"
                )
            state_code = part_state_code
    return state_code


def _task_id_part_matches(*, task_id: str, regex: str) -> bool:
    """Returns True if any part of a task id matches the regex. Task id parts are
    separated by periods.
    """
    return any(re.match(regex, part) for part in task_id.split("."))


def _task_is_in_group(*, task_id: str, group_id: str) -> bool:
    """Returns True if the task with the given task_id is a component task or group in
    the provided group_id.
    """
    all_but_last_task_parts = task_id.split(".")[:-1]
    return any(re.match(group_id, part) for part in all_but_last_task_parts)


def _task_is_branching_start_or_end(task_id: str) -> bool:
    """Returns True if this task is the state-agnostic start or end node of a set of
    state-specific branches.
    """
    return task_id.endswith(f".{BRANCH_START_TASK_NAME}") or task_id.endswith(
        f".{BRANCH_END_TASK_NAME}"
    )


def get_alerting_service_for_incident(
    incident: AirflowAlertingIncident,
) -> RecidivizPagerDutyService:
    """Returns the service that the given Airflow task should be attributed to. A
    PagerDuty alert will be triggered for the given service when that task fails.
    """
    project_id = get_project_id()
    dag_id = incident.dag_id
    task_id = incident.task_id

    if dag_id == get_sftp_dag_id(project_id):
        if not (state_code := _state_code_from_task_id(task_id)):
            return RecidivizPagerDutyService.data_platform_airflow_service(
                project_id=project_id
            )
        return RecidivizPagerDutyService.airflow_service_for_state_code(
            project_id=project_id, state_code=state_code
        )

    if _task_id_part_matches(task_id=task_id, regex=DATAFLOW_OPERATOR_TASK_ID):
        state_code = assert_type(_state_code_from_task_id(task_id), StateCode)
        return RecidivizPagerDutyService.airflow_service_for_state_code(
            project_id=project_id, state_code=state_code
        )

    # Failures in this task indicate that raw data has been removed or operations tables
    # haven't been properly managed, so route the failure to state-specific on-calls.
    if _task_id_part_matches(
        task_id=task_id, regex=SHOULD_RUN_BASED_ON_WATERMARKS_TASK_ID
    ):
        state_code = assert_type(_state_code_from_task_id(task_id), StateCode)
        return RecidivizPagerDutyService.airflow_service_for_state_code(
            project_id=project_id, state_code=state_code
        )

    if _task_is_in_group(
        task_id=task_id, group_id=STATE_SPECIFIC_METRIC_EXPORTS_GROUP_ID
    ) and not _task_is_branching_start_or_end(task_id):
        state_code = assert_type(_state_code_from_task_id(task_id), StateCode)
        return RecidivizPagerDutyService.airflow_service_for_state_code(
            project_id=project_id, state_code=state_code
        )

    return RecidivizPagerDutyService.data_platform_airflow_service(
        project_id=project_id
    )
