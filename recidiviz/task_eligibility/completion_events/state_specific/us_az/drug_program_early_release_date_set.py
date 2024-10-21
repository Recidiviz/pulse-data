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
"""Identify when the DTP (Drug Transition Program) early release date is initially set for an AZ resident."""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_QUERY_TEMPLATE = """
SELECT
    state_code,
    person_id,
    CAST(update_datetime AS DATE) AS completion_event_date,
FROM `{project_id}.normalized_state.state_task_deadline`
WHERE
    state_code = "US_AZ"
    AND task_type = "DISCHARGE_FROM_INCARCERATION"
    AND task_subtype = "DRUG TRANSITION RELEASE"
    AND eligible_date IS NOT NULL
    AND eligible_date > "1900-01-01"
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY state_code, person_id, task_metadata
    ORDER BY update_datetime
) = 1
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = StateSpecificTaskCompletionEventBigQueryViewBuilder(
    state_code=StateCode.US_AZ,
    completion_event_type=TaskCompletionEventType.DRUG_PROGRAM_EARLY_RELEASE_DATE_SET,
    description=__doc__,
    completion_event_query_template=_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
