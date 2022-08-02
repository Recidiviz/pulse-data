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
"""Defines a criteria span view that shows spans of time during which
someone in ND has a valid supervision level to qualify them for supervision early
termination, as inferred by the presence of a set early termination date in
docstars_offenders.
"""
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    state_task_deadline_eligible_date_updates_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ND_IMPLIED_VALID_EARLY_TERMINATION_SUPERVISION_LEVEL"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone in ND has a valid supervision level to qualify them for supervision early
termination, as inferred by the presence of a set early termination date in
docstars_offenders."""

_QUERY_TEMPLATE = f"""
/*{{description}}*/
WITH 
{state_task_deadline_eligible_date_updates_cte(StateTaskType.DISCHARGE_EARLY_FROM_SUPERVISION)}
-- TODO(#14317): Actually build criteria spans here.
SELECT
    state_code,
    person_id,
    CAST(update_datetime AS DATE) AS start_date,
    NULL AS end_date,
    False AS meets_criteria,
    NULL AS reason
FROM task_deadlines;
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_ND,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
