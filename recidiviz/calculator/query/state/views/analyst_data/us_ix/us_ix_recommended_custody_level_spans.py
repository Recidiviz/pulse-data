# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""IX-specific recommended custody level spans using Recidiviz-computed scores"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_NAME = (
    "us_ix_recommended_custody_level_spans"
)

US_IX_RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_DESCRIPTION = (
    """IX-specific recommended custody level spans using Recidiviz-computed scores"""
)

US_IX_RECOMMENDED_CUSTODY_LEVEL_SPANS_QUERY_TEMPLATE = f"""
WITH union_scores AS (       
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            q1_score,
            NULL AS q2_score,
            NULL AS q3_score,
            NULL AS q4_score,
            NULL AS q5_score
        FROM 
            `{{project_id}}.{{analyst_dataset}}.us_ix_sls_q1_materialized` sls
        
        UNION ALL
        
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            NULL AS q1_score,
            q2_score,
            NULL AS q3_score,
            NULL AS q4_score,
            NULL AS q5_score
        FROM 
            `{{project_id}}.{{analyst_dataset}}.us_ix_sls_q2_materialized` sls
        
        UNION ALL
        
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            NULL AS q1_score,
            NULL AS q2_score,
            q3_score,
            NULL AS q4_score,
            NULL AS q5_score
        FROM 
            `{{project_id}}.{{analyst_dataset}}.us_ix_sls_q3_materialized` sls
        
        UNION ALL
        
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            NULL AS q1_score,
            NULL AS q2_score,
            NULL AS q3_score,
            q4_score,
            NULL AS q5_score
        FROM 
            `{{project_id}}.{{analyst_dataset}}.us_ix_sls_q4_materialized` sls
            
        UNION ALL
        
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            NULL AS q1_score,
            NULL AS q2_score,
            NULL AS q3_score,
            NULL AS q4_score,
            q5_score
        FROM 
            `{{project_id}}.{{analyst_dataset}}.us_ix_sls_q5_materialized` sls
    )
    ,
    {create_sub_sessions_with_attributes(table_name='union_scores')},
    -- This CTE computes the relevant sub-scores needed for computing a recommended custody level
    dedup_cte AS (
        SELECT *,
            q1_score + q2_score + q3_score + q4_score + q5_score AS calculated_total_score,
        FROM (
            SELECT
                person_id,
                state_code,
                start_date,
                end_date,
                --if q1_score (current sentence severity) is NULL, overall score should be NULL
                MAX(q1_score) AS q1_score,
                --if q2_score is NULL, resident had no escape history, q2_score = 0
                COALESCE(MAX(q2_score),0) AS q2_score,
                --if q3_score is NULL, resident had no previous high severity felonies and q3_score = 0
                COALESCE(MAX(q3_score),0) AS q3_score,
                --if q4_score (age) is NULL overall score should be NULL
                MAX(q4_score) AS q4_score,
                --if q5_score is NULL, resident had no DORs in relevant time frame, q5_score = -1
                COALESCE(MAX(q5_score),-1) AS q5_score
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
            end_date,
            CASE 
                 WHEN calculated_total_score >= 20 THEN 'CLOSE'
                 WHEN calculated_total_score BETWEEN 7 AND 19 THEN 'MEDIUM'
                 WHEN calculated_total_score <= 6 THEN 'MINIMUM'
            END AS recommended_custody_level,
            TO_JSON_STRING(STRUCT(
                q1_score AS q1_score,
                q2_score AS q2_score,
                q3_score AS q3_score,
                q4_score AS q4_score,
                q5_score AS q5_score,
                calculated_total_score AS calculated_total_score
            )) AS score_metadata
        FROM
            dedup_cte
    )
    SELECT *
    FROM ({aggregate_adjacent_spans(table_name='scoring',
                                    attribute=['recommended_custody_level','score_metadata'])})
"""

US_IX_RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_IX_RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_NAME,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    description=US_IX_RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_DESCRIPTION,
    view_query_template=US_IX_RECOMMENDED_CUSTODY_LEVEL_SPANS_QUERY_TEMPLATE,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_BUILDER.build_and_print()
