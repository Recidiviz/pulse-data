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
"""Describes the spans of time during which someone in ID
is supervised at a stricter level than the risk assessment policy recommends.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
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
    WITH supervision_and_assessments AS (
      SELECT 
        state_code, 
        person_id, 
        assessment_date AS start_date, 
        score_end_date_exclusive AS end_date,
        NULL AS supervision_level, 
        assessment_score_bucket AS assessment_level,
        assessment_date,
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
      FROM `{{project_id}}.{{sessions_dataset}}.supervision_level_raw_text_sessions_materialized`
      WHERE state_code = "US_IX"
      AND case_type = "GENERAL"
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
                MAX(supervision_level) AS supervision_level
        FROM sub_sessions_with_attributes
        GROUP BY 1,2,3,4
    ),
    state_specific_mapping AS (
        SELECT *,
            CASE 
                WHEN assessment_level = 'LOW' 
                    AND supervision_level NOT IN ('MINIMUM', 'LIMITED', 'UNSUPERVISED', 'DIVERSION')
                    AND supervision_level IS NOT NULL 
                    THEN TRUE
                WHEN assessment_level = 'MODERATE' 
                    AND supervision_level NOT IN ('MEDIUM', 'MINIMUM', 'LIMITED', 'UNSUPERVISED', 'DIVERSION') 
                    AND supervision_level IS NOT NULL 
                    THEN TRUE
                WHEN assessment_level = 'HIGH'
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
            assessment_date AS latest_assessment_date
        )) AS reason,
        supervision_level,
        assessment_level,
        assessment_date AS latest_assessment_date,
    FROM state_specific_mapping
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
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
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
