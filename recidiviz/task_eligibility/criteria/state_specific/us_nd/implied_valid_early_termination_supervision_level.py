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
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_exists_spans_cte,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    task_deadline_critical_date_update_datetimes_cte,
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
{task_deadline_critical_date_update_datetimes_cte(
    task_type=StateTaskType.DISCHARGE_EARLY_FROM_SUPERVISION,
    critical_date_column='eligible_date')
},
{critical_date_exists_spans_cte()}
SELECT
    et_criteria.state_code,
    et_criteria.person_id,
    et_criteria.start_date,
    et_criteria.end_date,
    -- Mark this span as meeting the criteria if the eligible date is set
    critical_date_exists AS meets_criteria,
    TO_JSON(
        STRUCT(supervision_level AS supervision_level)
    ) AS reason,
FROM critical_date_exists_spans et_criteria
-- Join all the overlapping supervision level sessions
LEFT JOIN `{{project_id}}.{{sessions_data}}.supervision_level_sessions_materialized` sup_level
    ON sup_level.state_code = et_criteria.state_code
    AND sup_level.person_id = et_criteria.person_id
    AND sup_level.start_date < {nonnull_end_date_clause('et_criteria.end_date')}
    AND et_criteria.start_date < {nonnull_end_date_clause('sup_level.end_date')}
-- Prioritize the latest non-null supervision level
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY state_code, person_id, start_date
    ORDER BY
        supervision_level IS NOT NULL DESC,
        {nonnull_end_date_clause('sup_level.end_date')} DESC
) = 1
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_ND,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        sessions_data=SESSIONS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
