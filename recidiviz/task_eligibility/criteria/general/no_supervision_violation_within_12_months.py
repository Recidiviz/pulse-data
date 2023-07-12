# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Defines a criteria span view that shows spans of time during which there
is no violations within 12 months on supervision."""

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    VIOLATIONS_FOUND_WHERE_CLAUSE,
    violations_within_time_interval_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "NO_SUPERVISION_VIOLATION_WITHIN_12_MONTHS"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which there
is no violations within 12 months on supervision."""

_QUERY_TEMPLATE = f"""
WITH supervision_violations AS (
    /*
    This CTE identifies relevant violations and sets a 12 month 
    window where meets_criteria is FALSE.
    */
    {violations_within_time_interval_cte(
        date_interval = 12, 
        where_clause = VIOLATIONS_FOUND_WHERE_CLAUSE)}
    ),
{create_sub_sessions_with_attributes('supervision_violations')}
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    LOGICAL_AND(meets_criteria) AS meets_criteria,
    TO_JSON(STRUCT(ARRAY_AGG(violation_date IGNORE NULLS) AS latest_convictions)) AS reason,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
