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
# ============================================================================
"""Describes the spans of time when a resident is in a custody level that is higher than the one recommended."""
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "CUSTODY_LEVEL_HIGHER_THAN_RECOMMENDED"

_DESCRIPTION = """Describes the spans of time when a resident is in a custody level that is higher than the one recommended."""

_QUERY_TEMPLATE = f"""
    WITH custody_level_and_recommended_level AS (
      SELECT 
        state_code, 
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        custody_level,
        NULL AS recommended_custody_level,
      FROM `{{project_id}}.{{sessions_dataset}}.custody_level_sessions_materialized`

      UNION ALL

      SELECT 
        state_code, 
        person_id, 
        start_date,
        end_date,
        NULL AS custody_level,
        recommended_custody_level
      FROM `{{project_id}}.{{analyst_dataset}}.recommended_custody_level_spans_materialized`
    ),
    {create_sub_sessions_with_attributes('custody_level_and_recommended_level')}
    , 
    dedup_cte AS (
        SELECT
            person_id,
            state_code,
            start_date,
            end_date,
            -- Take non-null values if there are any
            MAX(custody_level) AS custody_level,
            MAX(recommended_custody_level) AS recommended_custody_level,
        FROM
            sub_sessions_with_attributes
        GROUP BY
            1,2,3,4
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        -- Lower levels are prioritized lower for deduplication, so they have a higher priority "number"
        recommended_cl.custody_level_priority > current_cl.custody_level_priority AS meets_criteria,
        TO_JSON(STRUCT(
            recommended_custody_level AS recommended_custody_level,
            dedup_cte.custody_level AS custody_level
        )) AS reason,
    FROM dedup_cte
    LEFT JOIN `{{project_id}}.{{sessions_dataset}}.custody_level_dedup_priority` current_cl
        ON dedup_cte.custody_level = current_cl.custody_level
    LEFT JOIN `{{project_id}}.{{sessions_dataset}}.custody_level_dedup_priority` recommended_cl
        ON recommended_custody_level = recommended_cl.custody_level
"""


VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        sessions_dataset=SESSIONS_DATASET,
        analyst_dataset=ANALYST_VIEWS_DATASET,
        meets_criteria_default=False,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
