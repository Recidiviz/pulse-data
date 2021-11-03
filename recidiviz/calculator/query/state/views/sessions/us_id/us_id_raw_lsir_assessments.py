# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Individual questions and calculated subscale components of the LSI-R assessment in ID, derived from raw tables"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_RAW_LSIR_ASSESSMENTS_VIEW_NAME = "us_id_raw_lsir_assessments"

US_ID_RAW_LSIR_ASSESSMENTS_VIEW_DESCRIPTION = """Individual questions and components of the LSI-R assessment in ID, derived from raw tables"""

US_ID_RAW_LSIR_ASSESSMENTS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH raw_assessments_clean AS (
        SELECT 
            'US_ID' as state_code,
            person_id,
            SAFE_CAST(assess_qstn_num AS INT64) as assessment_question, 
            /* Converts YES to 1, NO to 0, all other non-numerics to NULL. For questions 
            with numeric scoring (usually a scale from 0-3), convert these to integer values. */
            CASE LEFT(NULLIF(qstn_choice_desc, 'LEFT UNANSWERED'), 1) 
                WHEN 'Y' THEN 1
                WHEN 'N' THEN 0
                ELSE SAFE_CAST(LEFT(NULLIF(qstn_choice_desc, 'LEFT UNANSWERED'), 1) AS INT64) END
            AS assessment_response, 
            SAFE.PARSE_DATE("%F", SPLIT(ofndr_tst.updt_dt, ' ')[OFFSET(0)]) as assessment_date,
        FROM `{project_id}.us_id_raw_data_up_to_date_views.tst_qstn_rspns_latest`
        INNER JOIN `{project_id}.us_id_raw_data_up_to_date_views.assess_qstn_choice_latest`
            USING (qstn_choice_num, assess_qstn_num, assess_tst_id)
        INNER JOIN `{project_id}.us_id_raw_data_up_to_date_views.ofndr_tst_latest` ofndr_tst
            USING (ofndr_tst_id, assess_tst_id)
        INNER JOIN `{project_id}.{base_dataset}.state_person_external_id` person
            ON person.external_id = ofndr_num
        INNER JOIN `{project_id}.{sessions_dataset}.assessment_score_sessions_materialized` sessions
            USING (person_id)
        -- `assess_tst_id` enum 2 indicates LSI-R assessment type
        WHERE assess_tst_id = '2'
            AND person.state_code = 'US_ID'
            AND SAFE.PARSE_DATE("%F", SPLIT(ofndr_tst.updt_dt, ' ')[OFFSET(0)]) = sessions.assessment_date
        -- Get the row of the largest test id from a given assessment date, as a proxy for the most recent assessment 
        -- in cases where there are duplicate assessments on the same day.
        QUALIFY RANK() OVER(
            PARTITION BY sessions.person_id, sessions.assessment_date
            ORDER BY ofndr_tst_id DESC
        )  = 1
    ),
    raw_assessments_clean_subscales AS (
        SELECT 
            state_code, 
            person_id, 
            assessment_date,
            SUM(IF(subscale = 'criminal_history', risk_score, NULL)) as criminal_history_total,
            SUM(IF(subscale = 'education_employment', risk_score, NULL)) as education_employment_total,
            SUM(IF(subscale = 'financial', risk_score, NULL)) as financial_total,
            SUM(IF(subscale = 'family_marital', risk_score, NULL)) as family_marital_total,
            SUM(IF(subscale = 'accommodation', risk_score, NULL)) as accommodation_total,
            SUM(IF(subscale = 'leisure_recreation', risk_score, NULL)) as leisure_recreation_total,
            SUM(IF(subscale = 'companions', risk_score, NULL)) as companions_total,
            SUM(IF(subscale = 'alcohol_drug', risk_score, NULL)) as alcohol_drug_total,
            SUM(IF(subscale = 'emotional_personal', risk_score, NULL)) as emotional_personal_total,
            SUM(IF(subscale = 'attitudes_orientation', risk_score, NULL)) as attitudes_orientation_total,
            SUM(protective_factors_score) as protective_factors_score_total,
        FROM raw_assessments_clean
        INNER JOIN `{project_id}.{sessions_dataset}.assessment_lsir_scoring_key`
        USING (assessment_question, assessment_response)
        GROUP BY 1,2,3
    ),
    raw_assessments_pivot AS (
        /* Turn assessment questions and responses from "long" to "wide" format, with columns for each question*/
            SELECT * FROM raw_assessments_clean
        PIVOT(
            MAX(assessment_response) AS lsir_response
            FOR assessment_question IN ({lsir_question_array})
        )
    ) 
    /* Join individual questions with aggregate subscale scores for each person and assessment date row */
    SELECT *
    FROM raw_assessments_pivot
    INNER JOIN raw_assessments_clean_subscales
    USING (state_code, person_id, assessment_date)
    """

US_ID_RAW_LSIR_ASSESSMENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_ID_RAW_LSIR_ASSESSMENTS_VIEW_NAME,
    view_query_template=US_ID_RAW_LSIR_ASSESSMENTS_QUERY_TEMPLATE,
    description=US_ID_RAW_LSIR_ASSESSMENTS_VIEW_DESCRIPTION,
    base_dataset=STATE_BASE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    lsir_question_array=",".join([str(x) for x in range(1, 55)]),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_RAW_LSIR_ASSESSMENTS_VIEW_BUILDER.build_and_print()
