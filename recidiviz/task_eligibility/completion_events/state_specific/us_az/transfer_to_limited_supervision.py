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
"""Identifies people on supervision who have been transferred from a higher level of supervision
to LIMITED in AZ"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_QUERY_TEMPLATE = """SELECT
  state_code,
  person_id,
  start_date AS completion_event_date,
FROM `{project_id}.sessions.supervision_level_raw_text_sessions_materialized`
WHERE state_code = 'US_AZ'
  AND supervision_level = 'LIMITED'
  -- This extracts the string within the first and second '@@', which will tell us
  -- if the person was transferred from a higher level of supervision
  AND REGEXP_EXTRACT(supervision_level_raw_text, r'@@([^@]+)@@') = 'NONE'
  -- Only count transfers where the previous level was not limited
  AND IFNULL(previous_supervision_level, "UNKNOWN") != "LIMITED"
"""
VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=StateCode.US_AZ,
        completion_event_type=TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION,
        description=__doc__,
        completion_event_query_template=_QUERY_TEMPLATE,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
