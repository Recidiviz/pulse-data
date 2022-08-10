# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Helper SQL fragments that do standard queries against tables in the
normalized_state dataset.
"""
from recidiviz.common.constants.state.state_task_deadline import StateTaskType


def task_deadline_critical_date_update_datetimes_cte(
    task_type: StateTaskType, critical_date_column: str
) -> str:
    """Returns a CTE that selects all StateTaskDeadline rows with the provided
    |task_type] and renames the |critical_date_column| to `critical_date` for standard
    processing.
    """
    if critical_date_column not in {"eligible_date", "due_date"}:
        raise ValueError(f"Unsupported critical date column {critical_date_column}")
    return f"""critical_date_update_datetimes AS (
        SELECT
            state_code,
            person_id,
            {critical_date_column} AS critical_date,
            update_datetime
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_task_deadline`
        WHERE task_type = '{task_type.value}'
    )"""
