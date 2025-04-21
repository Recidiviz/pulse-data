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
# ============================================================================
"""This criteria view builder defines spans of time where residents have not had any time on the
maximum or high supervision levels in the past year
"""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "SUPERVISION_LEVEL_IS_NOT_HIGH_OR_MAX_FOR_ONE_YEAR"

_DESCRIPTION = __doc__

_REASON_QUERY = f"""
WITH max_high_sessions AS (
    SELECT 
        state_code,
        person_id,
        start_date,
        {nonnull_end_date_clause('DATE_ADD(end_date, INTERVAL 1 YEAR)')} AS end_date,
        {nonnull_end_date_clause('DATE_ADD(end_date, INTERVAL 1 YEAR)')} AS eligible_date,
    FROM `{{project_id}}.{{sessions_dataset}}.supervision_level_sessions_materialized`
    WHERE supervision_level IN ('MAXIMUM', 'HIGH')
),{create_sub_sessions_with_attributes('max_high_sessions')}
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    False AS meets_criteria,
    TO_JSON(STRUCT({revert_nonnull_end_date_clause('MAX(eligible_date)')} AS eligible_date)) AS reason,
    {revert_nonnull_end_date_clause('MAX(eligible_date)')} AS eligible_date,
FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_REASON_QUERY,
    sessions_dataset=SESSIONS_DATASET,
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="eligible_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date where client will have had 12 months with no maximum or high supervision",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
