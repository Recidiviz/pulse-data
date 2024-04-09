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
"""Defines a view that shows transfers to minimum security facilities (JRCC or MRCC)."""
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.task_eligibility.utils.us_nd_query_fragments import (
    MINIMUM_SECURITY_FACILITIES,
)

_DESCRIPTION = """Defines a view that shows transfers to minimum security
facilities (JRCC or MRCC)."""

_QUERY_TEMPLATE = f"""
SELECT 
  state_code,
  person_id,
  start_date AS completion_event_date,
FROM `{{project_id}}.{{sessions_dataset}}.location_sessions_materialized`
WHERE state_code = 'US_ND'
  AND facility IN {tuple(MINIMUM_SECURITY_FACILITIES)}
GROUP BY 1,2,3
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=StateCode.US_ND,
        completion_event_type=TaskCompletionEventType.TRANSFER_TO_REENTRY_CENTER,
        description=_DESCRIPTION,
        completion_event_query_template=_QUERY_TEMPLATE,
        sessions_dataset=SESSIONS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
