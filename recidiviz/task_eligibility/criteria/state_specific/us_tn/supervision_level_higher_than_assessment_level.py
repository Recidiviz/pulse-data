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
"""Describes the spans of time during which someone in TN is supervised at a stricter
level than the risk-assessment policy recommends.
"""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_tn_query_fragments import (
    DRC_SUPERVISION_LEVELS_RAW_TEXT,
    PSU_SUPERVISION_LEVELS_RAW_TEXT,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_SUPERVISION_LEVEL_HIGHER_THAN_ASSESSMENT_LEVEL"

_QUERY_TEMPLATE = f"""
    WITH strong_r_assessments_prioritized AS (
        /* Though `assessment_score_sessions_materialized` is already sessionized, there
        can be overlapping sessions there if multiple assessment types have been used to
        assess risk in a state. To work around this, we'll take the assessment data from
        that view but end up constructing criterion-specific spans later in this query,
        since we want to avoid having overlapping spans. */
        SELECT
            state_code,
            person_id,
            assessment_type,
            assessment_date,
            assessment_level,
        FROM `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized`
        WHERE state_code='US_TN'
            AND assessment_type IN ('STRONG_R', 'STRONG_R2')
        /* If someone happened to have both STRONG-R and STRONG-R 2.0 assessments on the
        same day, prioritize the 2.0 assessment, since it's intended to replace the
        original assessment. (This probably shouldn't happen, but this QUALIFY statement
        is here as a safeguard.) */
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY state_code, person_id, assessment_date
            ORDER BY 
                CASE
                    WHEN assessment_type='STRONG_R2' THEN 1
                    ELSE 2
                    END
        ) = 1
    ),
    supervision_and_assessments AS (
        /* Create STRONG-R (1.0/2.0) spans. We create non-overlapping spans such that if
        a person has a previous STRONG-R assessment of either type, that span will end
        when a subsequent STRONG-R assessment is completed, regardless of the type of
        the subsequent assessment. */
        SELECT 
            state_code,
            person_id,
            assessment_date AS start_date,
            LEAD(assessment_date) OVER (
                PARTITION BY state_code, person_id
                ORDER BY assessment_date
            ) AS end_date,
            CAST(NULL AS STRING) AS supervision_level,
            CAST(NULL AS STRING) AS supervision_level_raw_text,
            assessment_type,
            assessment_level,
            assessment_date,
        FROM strong_r_assessments_prioritized
    
        UNION ALL
    
        /* Pull supervision spans for TN clients. */
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive AS end_date,
            correctional_level AS supervision_level,
            correctional_level_raw_text AS supervision_level_raw_text,
            CAST(NULL AS STRING) AS assessment_type,
            CAST(NULL AS STRING) AS assessment_level,
            CAST(NULL AS DATE) AS assessment_date,
            --TODO(#20035): Use supervision_level_raw_text_sessions when deduping is consistent across views 
        FROM `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized`
        WHERE state_code='US_TN'
            AND compartment_level_1='SUPERVISION'
    ),
    {create_sub_sessions_with_attributes('supervision_and_assessments')}, 
    priority_levels AS (
        SELECT DISTINCT
            person_id, 
            state_code, 
            start_date, 
            end_date, 
            -- since assessment and supervision levels are non-overlapping, MAX() choose the non null 
            -- value for each span
            MAX(supervision_level) AS supervision_level,
            MAX(supervision_level_raw_text) AS supervision_level_raw_text,
            MAX(assessment_type) AS assessment_type,
            MAX(assessment_level) AS assessment_level,
            MAX(assessment_date) AS assessment_date,
        FROM sub_sessions_with_attributes
        GROUP BY 1,2,3,4
    ),
    state_specific_mapping AS (
        SELECT
            *,
            CASE
                /* Clients on DRC or PSU levels are never eligible for downgrades,
                regardless of their assessment levels. PSU designation is determined by
                courts/judges. */
                WHEN (
                    supervision_level_raw_text IN ({list_to_query_string(DRC_SUPERVISION_LEVELS_RAW_TEXT, quoted=True)})
                    OR supervision_level_raw_text IN ({list_to_query_string(PSU_SUPERVISION_LEVELS_RAW_TEXT, quoted=True)})
                    OR supervision_level_raw_text IS NULL
                ) THEN FALSE
                WHEN assessment_level = 'LOW'
                    AND supervision_level NOT IN ('MINIMUM', 'LIMITED', 'UNSUPERVISED')
                    THEN TRUE
                WHEN assessment_level = 'MODERATE'
                    AND supervision_level NOT IN ('MEDIUM', 'MINIMUM', 'LIMITED', 'UNSUPERVISED')
                    THEN TRUE
                ELSE FALSE
                END AS meets_criteria,
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
            assessment_type AS assessment_type,
            assessment_level AS assessment_level,
            assessment_date AS latest_assessment_date
        )) AS reason,
        supervision_level,
        assessment_type,
        assessment_level,
        assessment_date AS latest_assessment_date,
    FROM state_specific_mapping
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_TN,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=__doc__,
        sessions_dataset=SESSIONS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="supervision_level",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Supervision level",
            ),
            ReasonsField(
                name="assessment_type",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Type of latest risk assessment",
            ),
            ReasonsField(
                name="assessment_level",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Assessed risk level",
            ),
            ReasonsField(
                name="latest_assessment_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of latest risk assessment",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
