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
"""Describes the spans of time after the first 45 days of supervision, during which someone in ID
is supervised at a stricter level than the risk assessment policy recommends.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_intersection_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_SUPERVISION_LEVEL_HIGHER_THAN_ASSESSMENT_LEVEL"

_DESCRIPTION = """Describes the spans of time during which someone in ID
is supervised at a stricter level than the risk assessment policy recommends.
"""

_QUERY_TEMPLATE = f"""
    WITH 
    supervision_level_sessions AS (
    /* We use the raw text view here because it dedups in the way we expect for this view unlike supervision_level_sessions*/
        SELECT 
            state_code, 
            person_id, 
            start_date AS supervision_level_start_date, 
            end_date_exclusive,
            supervision_level,
        FROM `{{project_id}}.{{sessions_dataset}}.supervision_level_raw_text_sessions_materialized`
        WHERE state_code = "US_IX"
            AND case_type = "GENERAL"
    ),
    sss_sessions AS (
    /* This cte associates assessments done within the first 45 days of supervision and prioritizes the earliest one */
    #TODO(#38916) Replace call to supervision_super_sessions with prioritized_supervision_super_sessions
        SELECT
            sss.state_code,
            sss.person_id,
            sss.start_date,
            sss.end_date_exclusive,
            sap.assessment_date
        FROM `{{project_id}}.{{sessions_dataset}}.supervision_super_sessions_materialized` sss
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` sap
            ON sss.state_code = sap.state_code
            AND sss.person_id = sap.person_id 
            --join all assessments done within the first 45 days of supervision start
            AND sap.assessment_date BETWEEN sss.start_date AND DATE_ADD(sss.start_date, INTERVAL 45 DAY)
        WHERE sss.state_code = "US_IX"
        --only choose the first assessment score within a supervision super session
        QUALIFY ROW_NUMBER() OVER(PARTITION BY sss.state_code, sss.person_id, sss.start_date, sss.end_date ORDER BY sap.assessment_date)=1
    ),
    sss_sessions_all AS (
    /* This cte unions sessions of time where a client is awaiting assessment with the rest of the supervision super
    session and sets a boolean flag to indicate whether a client is awaiting assessment or not during that span */
        SELECT
            sss.state_code,
            sss.person_id,
            sss.start_date,
            COALESCE(assessment_date, DATE_ADD(sss.start_date, INTERVAL 45 DAY)) AS end_date_exclusive,
            TRUE AS awaiting_assessment
        FROM sss_sessions sss
        
        UNION ALL 
            
        SELECT 
            sss.state_code,
            sss.person_id,
            COALESCE(assessment_date, DATE_ADD(sss.start_date, INTERVAL 45 DAY)) AS sss_start_date,
            sss.end_date_exclusive,
            FALSE AS awaiting_assessment
        FROM sss_sessions sss
    ),
    supervision_level_sessions_preprocessed AS (
    /* This cte takes the intersection of the supervision level sessions and sss_sessions_all to output
     spans of time on a specific supervision level and awaiting_assessment status.   */
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        supervision_level,
        awaiting_assessment
    FROM 
    ({create_intersection_spans(table_1_name='supervision_level_sessions',
                                         table_2_name='sss_sessions_all',
                                         index_columns=['state_code', 'person_id'],
                                         table_1_columns=['supervision_level'],
                                         table_2_columns=['awaiting_assessment'],
                                         table_1_start_date_field_name='supervision_level_start_date')})
    ),
    supervision_and_assessments AS (
    /* This cte unions all assessment_score_sessions as well as supervision_level_sessions_preprocessed so we can
    create_sub_sessions_with_attributes in the next cte */
      SELECT 
        state_code, 
        person_id, 
        assessment_date AS start_date, 
        score_end_date_exclusive AS end_date,
        NULL AS supervision_level, 
        assessment_score_bucket AS assessment_level,
        assessment_date,
        NULL AS awaiting_assessment
      FROM `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized`
      WHERE state_code = "US_IX"
    
      UNION ALL
    
      SELECT 
        state_code, 
        person_id, 
        start_date, 
        end_date_exclusive AS end_date,
        supervision_level,
        NULL AS assessment_level,
        NULL AS assessment_date,
        awaiting_assessment
      FROM supervision_level_sessions_preprocessed
    ),
    {create_sub_sessions_with_attributes('supervision_and_assessments')}
    , 
    priority_levels AS (
        SELECT  DISTINCT
                person_id, 
                state_code, 
                start_date, 
                end_date,  
                -- since assessment and supervision levels are non-overlapping, MAX() choose the non null 
                -- value for each span
                MAX(assessment_level) AS assessment_level,
                MAX(assessment_date) AS assessment_date,
                MAX(supervision_level) AS supervision_level,
                LOGICAL_OR(awaiting_assessment) AS awaiting_assessment
        FROM sub_sessions_with_attributes
        GROUP BY 1,2,3,4
    ),
    state_specific_mapping AS (
        SELECT *,
            CASE 
            --if someone does not have an assessment score and is awaiting_assessment the should by default be supervised at MEDIUM
                WHEN supervision_level = "MEDIUM"
                    AND awaiting_assessment
                    THEN FALSE
                WHEN (assessment_level IN ('LOW','LEVEL_1','LEVEL_2'))
                    AND supervision_level NOT IN ('MINIMUM', 'LIMITED', 'UNSUPERVISED', 'DIVERSION')
                    AND supervision_level IS NOT NULL 
                    THEN TRUE
                WHEN (assessment_level IN ('MODERATE', 'LEVEL_3') OR assessment_level IS NULL)
                    AND supervision_level NOT IN ('MEDIUM', 'MINIMUM', 'LIMITED', 'UNSUPERVISED', 'DIVERSION') 
                    AND supervision_level IS NOT NULL 
                    THEN TRUE
                WHEN (assessment_level IN ('HIGH', 'LEVEL_4'))
                    AND supervision_level NOT IN ('HIGH','MEDIUM', 'MINIMUM', 'LIMITED', 'UNSUPERVISED', 'DIVERSION') 
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
            assessment_date AS latest_assessment_date,
            awaiting_assessment AS awaiting_assessment
        )) AS reason,
        supervision_level,
        assessment_level,
        assessment_date AS latest_assessment_date,
        awaiting_assessment,
    FROM state_specific_mapping
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    reasons_fields=[
        ReasonsField(
            name="supervision_level",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Superior level of supervision",
        ),
        ReasonsField(
            name="assessment_level",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Assessment level",
        ),
        ReasonsField(
            name="latest_assessment_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Latest assessment date",
        ),
        ReasonsField(
            name="awaiting_assessment",
            type=bigquery.enums.StandardSqlTypeNames.BOOL,
            description="A boolean indicating whether the client is in the first 45 of supervision and has not had an assessment",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
