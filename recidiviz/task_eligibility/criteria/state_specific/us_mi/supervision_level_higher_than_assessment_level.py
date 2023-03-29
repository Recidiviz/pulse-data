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
# ============================================================================
"""Describes the spans of time during which someone in MI
is supervised at a stricter level than the risk assessment policy recommends.
"""
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MI_SUPERVISION_LEVEL_HIGHER_THAN_ASSESSMENT_LEVEL"

_DESCRIPTION = """Describes the spans of time during which someone in MI
is supervised at a stricter level than the risk assessment policy recommends.
"""

_QUERY_TEMPLATE = f"""
    WITH supervision_and_assessments AS (
      SELECT 
        state_code, 
        person_id, 
        assessment_date AS start_date, 
        DATE_ADD(score_end_date, INTERVAL 1 DAY) AS end_date, 
        NULL AS supervision_level, 
        assessment_level_raw_text AS assessment_level,
        assessment_date,
      FROM `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized`
      WHERE state_code = "US_MI"
    
      UNION ALL
    
      SELECT 
        state_code, 
        person_id, 
        start_date, 
        DATE_ADD(end_date, INTERVAL 1 DAY) AS end_date, 
        supervision_level,
        NULL AS assessment_level,
        NULL AS assessment_date,
      FROM `{{project_id}}.{{sessions_dataset}}.supervision_level_sessions_materialized`
      WHERE state_code = "US_MI"
    ),
    {create_sub_sessions_with_attributes('supervision_and_assessments')}
    , 
    priority_levels AS (
        SELECT  DISTINCT
                person_id, 
                state_code, 
                start_date, 
                end_date, 
                --if there is no COMPAS level, default to MEDIUM
                COALESCE(FIRST_VALUE(assessment_level) OVER (assessment_window), 'MEDIUM') AS assessment_level,
                FIRST_VALUE(assessment_date) OVER (assessment_window) AS assessment_date,
                FIRST_VALUE(supervision_level) OVER (supervision_level_window) AS supervision_level,
        FROM sub_sessions_with_attributes
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.supervision_level_dedup_priority`
            ON supervision_level = correctional_level
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.assessment_level_dedup_priority`
            USING(assessment_level)
        WINDOW assessment_window AS (
            PARTITION BY state_code, person_id, start_date, end_date
            ORDER BY COALESCE(assessment_level_priority, 999)
        ),
        supervision_level_window AS (
            PARTITION BY state_code, person_id, start_date, end_date
            ORDER BY COALESCE(correctional_level_priority, 999)        
        )
    ),
    state_specific_mapping AS (
        SELECT *,
            CASE 
                WHEN assessment_level = 'LOW/LOW' 
                    AND supervision_level NOT IN ('MINIMUM', 'LIMITED', 'UNSUPERVISED')
                    AND supervision_level IS NOT NULL 
                    THEN TRUE
                WHEN assessment_level IN ('LOW/MEDIUM', 'MEDIUM/LOW', 'MEDIUM/MEDIUM', 'HIGH/MEDIUM', 'MEDIUM/HIGH', 'HIGH/LOW', 'LOW/HIGH')
                    AND supervision_level NOT IN ('MEDIUM', 'MINIMUM', 'LIMITED', 'UNSUPERVISED') 
                    AND supervision_level IS NOT NULL 
                    THEN TRUE
                ELSE FALSE 
                END as meets_criteria
        FROM priority_levels
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(
            supervision_level AS supervision_level,
            assessment_level AS assessment_level,
            assessment_date AS latest_assessment_date
        )) AS reason,
    FROM state_specific_mapping
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_MI,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
