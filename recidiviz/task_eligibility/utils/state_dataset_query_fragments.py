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


def state_task_deadline_eligible_date_updates_cte(task_type: StateTaskType) -> str:
    """Returns a CTE that, for each StateTaskDeadline with the provided |task_type|,
    returns all rows when the eligible_date changed for a given person.
    """
    return f"""task_deadlines AS (
    SELECT * EXCEPT(new_span)
    FROM (
        SELECT
            state_code,
            person_id,
            eligible_date,
            update_datetime,
            COALESCE(
                COALESCE(eligible_date, "9999-12-31") != LAG(COALESCE(eligible_date, "9999-12-31")) OVER (
                    PARTITION BY person_id ORDER BY update_datetime ASC
                ), 
                TRUE
            ) AS new_span,
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_task_deadline`
        WHERE task_type = '{task_type.value}'
        QUALIFY new_span
    )
)"""
