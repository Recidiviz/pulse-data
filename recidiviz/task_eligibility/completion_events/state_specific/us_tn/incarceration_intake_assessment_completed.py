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
"""Defines a view that shows when intake classification hearings have occurred"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_QUERY_TEMPLATE = """
    SELECT c.state_code,
           c.person_id,
           c.classification_decision_date AS completion_event_date, 
    FROM
        `{project_id}.analyst_data.custody_classification_assessment_dates_materialized` c
    -- This criteria only exists for spans where someone has received their first classification after starting
    -- or re-starting state-prison custody, so joining here limits to only intake completion events
    INNER JOIN 
        `{project_id}.task_eligibility_criteria_general.has_initial_classification_in_state_prison_custody_materialized` i
    ON c.person_id = i.person_id
    AND c.state_code = i.state_code
    AND c.classification_decision_date = i.start_date  

"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = StateSpecificTaskCompletionEventBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    completion_event_type=TaskCompletionEventType.INCARCERATION_INTAKE_ASSESSMENT_COMPLETED,
    description=__doc__,
    completion_event_query_template=_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
