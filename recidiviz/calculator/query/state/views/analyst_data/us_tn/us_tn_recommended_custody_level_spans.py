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
"""TN-specific recommended custody level spans using a combination of TN-computed and Recidiviz-computed scores"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_NAME = (
    "us_tn_recommended_custody_level_spans"
)

US_TN_RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_DESCRIPTION = """TN-specific recommended custody level spans using a combination of TN-computed and Recidiviz-computed scores"""

US_TN_RECOMMENDED_CUSTODY_LEVEL_SPANS_QUERY_TEMPLATE = f"""
    /* We are using TN's computed scores for Q3, Q4 (schedule A) and Q5, Q9 (schedule B). If someone scores
    above a 10 on Schedule A, schedule B does not get scored. So this CTE unions all CAF scores (from which we pull
    Q3 and Q4) with a subset of CAF scores where Schedule A score is < 10 so that we have the most complete / accurate
    information for Q5 and Q9. 
    
    There's a very, very small number of people (~30) who have a CAF score but no CAF scores where Schedule A < 10
    */
    WITH CAF_scores AS (
        SELECT
            state_code,
            person_id,
            assessment_date AS start_date,
            score_end_date_exclusive AS end_date_exclusive,
            assessment_score,
            CAST(JSON_EXTRACT_SCALAR(assessment_metadata,'$.QUESTION1') AS INT64) AS q1_score,
            CAST(JSON_EXTRACT_SCALAR(assessment_metadata,'$.QUESTION2') AS INT64) AS q2_score,
            CAST(JSON_EXTRACT_SCALAR(assessment_metadata,'$.QUESTION3') AS INT64) AS q3_score,
            CAST(JSON_EXTRACT_SCALAR(assessment_metadata,'$.QUESTION4') AS INT64) AS q4_score,
            CAST(JSON_EXTRACT_SCALAR(assessment_metadata,'$.QUESTION5') AS INT64) AS q5_score,
            CAST(JSON_EXTRACT_SCALAR(assessment_metadata,'$.QUESTION6') AS INT64) AS q6_score,
            CAST(JSON_EXTRACT_SCALAR(assessment_metadata,'$.QUESTION7') AS INT64) AS q7_score,
            CAST(JSON_EXTRACT_SCALAR(assessment_metadata,'$.QUESTION8') AS INT64) AS q8_score,
            CAST(JSON_EXTRACT_SCALAR(assessment_metadata,'$.QUESTION9') AS INT64) AS q9_score,
            CAST(JSON_EXTRACT_SCALAR(assessment_metadata,'$.SCHEDULEASCORE') AS INT64) AS schedule_a_score,
            CAST(JSON_EXTRACT_SCALAR(assessment_metadata,'$.SCHEDULEBSCORE') AS INT64) AS schedule_b_score
        FROM
            `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` asmt
        WHERE
            assessment_type = 'CAF'
            AND state_code = 'US_TN'
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, assessment_date ORDER BY assessment_score DESC) = 1
            
    ), union_caf_scores AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
            NULL AS q1_score,
            NULL AS q2_score,
            q3_score,
            q4_score,
            NULL AS q5_score,
            NULL AS q6_score,
            NULL AS q7_score,
            NULL AS q8_score,
            NULL AS q9_score,
        FROM 
            CAF_scores caf
        
        UNION ALL
        
        SELECT
            state_code,
            person_id,
            start_date,
            -- Since we're taking a subset of scores here, recreating end date
            LEAD(start_date) OVER(
                            PARTITION BY person_id 
                            ORDER BY start_date ASC
                            ) AS end_date_exclusive,
            NULL AS q1_score,
            NULL AS q2_score,
            NULL AS q3_score,
            NULL AS q4_score,
            q5_score,
            NULL AS q6_score,
            NULL AS q7_score,
            NULL AS q8_score,
            q9_score,
        FROM 
            CAF_scores caf
        WHERE
            schedule_a_score < 10
    ),
    -- This CTE combines CAF Scores from TN with our self-computed scores
    union_scores AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
            MAX(q1_score) AS q1_score,
            MAX(q2_score) AS q2_score,
            MAX(q3_score) AS q3_score,
            MAX(q4_score) AS q4_score,
            MAX(q5_score) AS q5_score,
            MAX(q6_score) AS q6_score,
            MAX(q7_score) AS q7_score,
            MAX(q8_score) AS q8_score,
            MAX(q9_score) AS q9_score,
        FROM 
            union_caf_scores
        GROUP BY
            1,2,3,4    
        
        UNION ALL
        
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
            q1_score,
            NULL AS q2_score,
            NULL AS q3_score,
            NULL AS q4_score,
            NULL AS q5_score,
            NULL AS q6_score,
            NULL AS q7_score,
            NULL AS q8_score,
            NULL AS q9_score,
        FROM 
            `{{project_id}}.{{analyst_dataset}}.us_tn_caf_q1_materialized` caf
        
        UNION ALL
        
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
            NULL AS q1_score,
            q2_score,
            NULL AS q3_score,
            NULL AS q4_score,
            NULL AS q5_score,
            NULL AS q6_score,
            NULL AS q7_score,
            NULL AS q8_score,
            NULL AS q9_score,
        FROM 
            `{{project_id}}.{{analyst_dataset}}.us_tn_caf_q2_materialized` caf
        
        UNION ALL
        
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
            NULL AS q1_score,
            NULL AS q2_score,
            NULL AS q3_score,
            NULL AS q4_score,
            NULL AS q5_score,
            q6_score,
            NULL AS q7_score,
            NULL AS q8_score,
            NULL AS q9_score,
        FROM 
            `{{project_id}}.{{analyst_dataset}}.us_tn_caf_q6_materialized` caf
        
        UNION ALL
        
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
            NULL AS q1_score,
            NULL AS q2_score,
            NULL AS q3_score,
            NULL AS q4_score,
            NULL AS q5_score,
            NULL AS q6_score,
            q7_score,
            NULL AS q8_score,
            NULL AS q9_score,
        FROM 
            `{{project_id}}.{{analyst_dataset}}.us_tn_caf_q7_materialized` caf
            
        UNION ALL
        
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
            NULL AS q1_score,
            NULL AS q2_score,
            NULL AS q3_score,
            NULL AS q4_score,
            NULL AS q5_score,
            NULL AS q6_score,
            NULL AS q7_score,
            q8_score,
            NULL AS q9_score,
        FROM 
            `{{project_id}}.{{analyst_dataset}}.us_tn_caf_q8_materialized` caf
        
        UNION ALL
        
        SELECT 
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
            0 AS q1_score,
            0 AS q2_score,
            NULL AS q3_score,
            NULL AS q4_score,
            NULL AS q5_score,
            NULL AS q6_score,
            0 AS q7_score,
            0 AS q8_score,
            NULL AS q9_score,
        FROM
            `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` cs
        WHERE
            state_code = 'US_TN'
        
    )
    ,
    {create_sub_sessions_with_attributes(table_name='union_scores',end_date_field_name="end_date_exclusive")},
    -- This CTE computes the relevant sub-scores needed for computing a recommended custody level
    dedup_cte AS (
        SELECT *,
            q1_score + q2_score + q3_score + q4_score AS calculated_schedule_a_score,
            q5_score + q6_score + q7_score + q8_score + q9_score AS calculated_schedule_b_score,
            q1_score + q2_score + q3_score + q4_score + q5_score + q6_score + q7_score + q8_score + q9_score AS calculated_total_score,
        FROM (
            SELECT
                person_id,
                state_code,
                start_date,
                end_date_exclusive,
                MAX(q1_score) AS q1_score,
                MAX(q2_score) AS q2_score,
                MAX(q3_score) AS q3_score,
                MAX(q4_score) AS q4_score,
                MAX(q5_score) AS q5_score,
                MAX(q6_score) AS q6_score,
                MAX(q7_score) AS q7_score,
                MAX(q8_score) AS q8_score,
                MAX(q9_score) AS q9_score,
            FROM
                sub_sessions_with_attributes
            GROUP BY
                1,2,3,4
        )
    ), scoring AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
            CASE WHEN calculated_schedule_a_score BETWEEN 10 AND 14 THEN 'CLOSE'
                 WHEN calculated_schedule_a_score >= 15 THEN 'MAXIMUM'
                 WHEN calculated_total_score >= 17 THEN 'CLOSE'
                 WHEN calculated_total_score BETWEEN 7 AND 16 THEN 'MEDIUM'
                 WHEN calculated_total_score <= 6 THEN 'MINIMUM'
            END AS recommended_custody_level,
            TO_JSON_STRING(STRUCT(
                q1_score AS q1_score,
                q2_score AS q2_score,
                q3_score AS q3_score,
                q4_score AS q4_score,
                q5_score AS q5_score,
                q6_score AS q6_score,
                q7_score AS q7_score,
                q8_score AS q8_score,
                q9_score AS q9_score,
                calculated_schedule_a_score AS calculated_schedule_a_score,
                calculated_schedule_b_score AS calculated_schedule_b_score,
                calculated_total_score AS calculated_total_score
            )) AS score_metadata
        FROM
            dedup_cte
    )
    SELECT *,
    FROM ({aggregate_adjacent_spans(table_name='scoring',
                                    attribute=['recommended_custody_level','score_metadata'],
                                    end_date_field_name="end_date_exclusive")})
"""

US_TN_RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_NAME,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    description=US_TN_RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    view_query_template=US_TN_RECOMMENDED_CUSTODY_LEVEL_SPANS_QUERY_TEMPLATE,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_BUILDER.build_and_print()
