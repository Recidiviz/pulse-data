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
"""Describes the spans of time during which someone in TN
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

_CRITERIA_NAME = "US_TN_SUPERVISION_LEVEL_HIGHER_THAN_ASSESSMENT_LEVEL"

_DESCRIPTION = """Describes the spans of time during which someone in TN
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
        NULL AS supervision_level_raw_text, 
        assessment_level,
        assessment_level_raw_text,
        assessment_date,
      FROM `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized`
    
      UNION ALL
    
      SELECT 
        state_code, 
        person_id, 
        start_date, 
        DATE_ADD(end_date, INTERVAL 1 DAY) AS end_date, 
        supervision_level,
        supervision_level_raw_text, 
        NULL AS assessment_level,
        NULL AS assessment_level_raw_text,
        NULL AS assessment_date,
      FROM `{{project_id}}.{{sessions_dataset}}.supervision_level_raw_text_sessions_materialized`
    ),
    {create_sub_sessions_with_attributes('supervision_and_assessments')}
    , 
    priority_levels AS (
        SELECT  DISTINCT
                person_id, 
                state_code, 
                start_date, 
                end_date, 
                FIRST_VALUE(assessment_level) OVER (assessment_window) AS assessment_level,
                FIRST_VALUE(assessment_level_raw_text) OVER (assessment_window) AS assessment_level_raw_text,
                FIRST_VALUE(assessment_date) OVER (assessment_window) AS assessment_date,
                FIRST_VALUE(supervision_level) OVER (supervision_level_window) AS supervision_level,
                FIRST_VALUE(supervision_level_raw_text) OVER (supervision_level_window) AS supervision_level_raw_text,
        #TODO(#20588) Potentially simplify to remove priority level dedups and just choose non null value
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
                WHEN assessment_level = 'LOW' 
                    AND supervision_level NOT IN ('MINIMUM', 'LIMITED', 'UNSUPERVISED') 
                    /* 6P3 maps to PSU (Programmed Supervision Unit, for people with sex offenses)
                       which Recidiviz maps to MEDIUM supervision level. However, the PSU designation is
                       determined by courts/judges and thus people in that unit are not eligible for a downgrade 
                       regardless of their assessment score
                    */
                    AND supervision_level_raw_text != '6P3' 
                    THEN TRUE
                WHEN assessment_level = 'MODERATE' 
                    AND supervision_level NOT IN ('MEDIUM', 'MINIMUM', 'LIMITED', 'UNSUPERVISED') 
                    THEN TRUE
                WHEN assessment_level_raw_text = 'HP' 
                    AND Supervision_Level NOT IN ('HIGH','MEDIUM', 'MINIMUM', 'LIMITED', 'UNSUPERVISED') 
                    /* These levels map to various "special" supervision levels for people with sex or violent offenses.                    
                       Recidiviz maps these to MAXIMUM supervision level. However, these designations are likely
                       determined by courts/judges and thus people in those levels are not eligible for a downgrade 
                       regardless of their assessment score
                    */
                    AND supervision_level_raw_text NOT IN ('KGE','KNE','QGE','QNE','VGE','VNE','XEN','XGE','XNE')  
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
        state_code=StateCode.US_TN,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
