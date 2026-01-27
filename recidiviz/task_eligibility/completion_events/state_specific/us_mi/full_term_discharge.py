# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Identifies (at the person level) when clients in MI have been discharged from
supervision after serving the full term (ie. not an early discharge).
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_QUERY_TEMPLATE = """
SELECT
    sessions.state_code,
    sessions.person_id,
    sessions.end_date_exclusive AS completion_event_date,
FROM
    `{project_id}.sessions.compartment_sessions_materialized` sessions
LEFT JOIN
    `{project_id}.task_eligibility_completion_events_us_mi.early_discharge_materialized` eds
ON
    eds.state_code = sessions.state_code
    AND eds.person_id = sessions.person_id
    AND eds.completion_event_date = sessions.end_date_exclusive
WHERE
    sessions.compartment_level_1 IN ("SUPERVISION", "SUPERVISION_OUT_OF_STATE")
    AND sessions.outflow_to_level_1 = "LIBERTY"
    -- Only count as full term if it was not counted as an early discharge
    AND eds.person_id IS NULL
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=StateCode.US_MI,
        completion_event_type=TaskCompletionEventType.FULL_TERM_DISCHARGE,
        description=__doc__,
        completion_event_query_template=_QUERY_TEMPLATE,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
