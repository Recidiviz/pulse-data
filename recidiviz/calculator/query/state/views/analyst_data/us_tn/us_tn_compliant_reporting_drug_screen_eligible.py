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
# =============================================================================
"""Creates a view that surfaces sessions during which clients are eligible or ineligible for
Compliant Reporting due to drug screening results."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_COMPLIANT_REPORTING_DRUG_SCREEN_ELIGIBLE_VIEW_NAME = (
    "us_tn_compliant_reporting_drug_screen_eligible"
)


US_TN_COMPLIANT_REPORTING_DRUG_SCREEN_ELIGIBLE_VIEW_DESCRIPTION = "Creates a view that surfaces sessions during which clients are eligible or ineligible for Compliant Reporting due to drug screening results."

US_TN_COMPLIANT_REPORTING_DRUG_SCREEN_ELIGIBLE_QUERY_TEMPLATE = """
    WITH date_array AS (
        SELECT date FROM UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 5 YEAR), CURRENT_DATE('US/Eastern'))) AS date
    )
    ,
    contacts_cte AS (
        /* Identify all drug screens */
        SELECT 
            person_id,
            drug_screen_date,
            CASE WHEN result_raw_text_primary IN ('DRUN','DRUM','DRUX') THEN 1 ELSE 0 END AS is_negative_result,
            is_positive_result,
            CASE WHEN is_positive_result = true THEN drug_screen_date END AS positive_screen_date,
            1 as drug_screen
        FROM `{project_id}.{sessions_dataset}.drug_screens_preprocessed_materialized`
        WHERE state_code = 'US_TN'
    )
    ,
    flag_high_ad AS (
        /* Identifies spans of time when someone is flagged as a "drug offender" because the Alcohol/Drug component
        of their assessment was MOD or HIGH */
        SELECT *,
          IFNULL(IF(
                    LAG(high_ad_client) OVER(PARTITION BY person_id ORDER BY assessment_date) = high_ad_client,
                    0, 1
                ), 1) AS high_ad_client_changed,
          DATE_SUB(LEAD(assessment_date) OVER(PARTITION BY person_id ORDER BY assessment_date), INTERVAL 1 DAY) AS end_date,
          FROM (
              /* In a small number of cases, there can be multiple assessments for the same person/day. In these cases
              we take the maximum of whether or not a person's alcohol/drug need was flagged as moderate or high */
              SELECT person_id,
                assessment_date, 
                MAX(CASE WHEN COALESCE(REPLACE(JSON_EXTRACT(assessment_metadata, "$.ALCOHOL_DRUG_NEED_LEVEL"), '"',''), 'MISSING') IN ('MOD','HIGH') THEN 1 ELSE 0 END) AS high_ad_client
              FROM `{project_id}.{base_dataset}.state_assessment`
              WHERE state_code = 'US_TN'
              GROUP BY 1,2
          )
    ), 
    sessionized_high_ad AS (
        /* Sessionizes periods of time when someone is or isn't a "drug offender" */
        SELECT 
        person_id,
        high_ad_client_session_id,
        high_ad_client,
        MIN(assessment_date) AS high_ad_start_date,
        IF(
            LOGICAL_AND(end_date IS NOT NULL),
            MAX(end_date),
            NULL
        ) AS high_ad_end_date
      FROM (
        SELECT *,
            SUM(high_ad_client_changed) OVER(PARTITION BY person_id ORDER BY assessment_date) AS high_ad_client_session_id
        FROM flag_high_ad
      )
      GROUP BY 1,2,3
    )
    ,
    drug_tests_unnested_cte AS (
    /* Get person-day stats about past drug screens. Calculated with a lookback of 365 days */
        SELECT 
            person_id,
            date,
            COUNTIF(drug_screen_date BETWEEN DATE_SUB(date, INTERVAL 365 DAY) AND date) OVER (PARTITION BY person_id, date) AS num_screens_past_year,
            COUNTIF(is_negative_result = 1 AND drug_screen_date BETWEEN DATE_SUB(date, INTERVAL 365 DAY) AND date) OVER (PARTITION BY person_id, date) AS num_negative_screens_past_year,
            FIRST_VALUE(is_negative_result) OVER (PARTITION BY person_id, date ORDER BY drug_screen_date DESC) AS negative_most_recent_screen_result,
            FIRST_VALUE(positive_screen_date) OVER (PARTITION BY person_id, date ORDER BY positive_screen_date desc) AS most_recent_positive_test_date,
        FROM date_array
        INNER JOIN contacts_cte
        ON drug_screen_date <= date
        QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, date) = 1
    )
    ,
    drug_screen_eligibility_unnested_cte AS (
        /* Unnests eligibility based on offense type at the person-day level */
        SELECT
            person_id,
            date,
            /* For a given person/day, flag if someone is a high Alcohol/Drug needs client.  */
            MAX(high_ad_client) AS high_ad_client,        
        FROM date_array a
        INNER JOIN sessionized_high_ad
            ON date BETWEEN high_ad_start_date AND COALESCE(high_ad_end_date, CURRENT_DATE('US/Eastern'))
        GROUP BY 1,2
    )
    ,   
    all_unnested_cte AS (
        SELECT
            person_id,
            date,
            /* If this flag is missing, meaning there were no assessments for a given period, then assume they don't have high Alcohol/Drug needs. 
             This is unlikely to affect compliant reporting eligibility because lack of assessments suggests this person is currently in the intake process */
            CASE WHEN COALESCE(high_ad_client,0) = 1 THEN 
                CASE WHEN negative_most_recent_screen_result = 1
                        AND num_negative_screens_past_year >= 2
                        AND DATE_DIFF(date, COALESCE(most_recent_positive_test_date, '1900-01-01'), DAY) >= 365
                THEN 1
                ELSE 0 END
            ELSE 
                CASE WHEN negative_most_recent_screen_result = 1
                        AND num_negative_screens_past_year >= 1
                        AND DATE_DIFF(date, COALESCE(most_recent_positive_test_date, '1900-01-01'), DAY) >= 180
                THEN 1
                ELSE 0 END
            END AS drug_screen_eligible    
        FROM drug_tests_unnested_cte 
        /* Some people are missing tests, some are missing assessments. This keeps the full universe of both */ 
        FULL OUTER JOIN drug_screen_eligibility_unnested_cte
        USING (person_id, date)
    )
    SELECT
        person_id,
        "US_TN" AS state_code,
        session_id,
        MIN(date) AS start_date,
        IF(
            LOGICAL_AND(date != CURRENT_DATE('US/Eastern')),
            MAX(date),
            NULL
        ) AS end_date,
        ANY_VALUE(drug_screen_eligible) AS drug_screen_eligible, 
    FROM 
        (
        SELECT 
            *,
            SUM(IF(new_session,1,0)) OVER(PARTITION BY person_id ORDER BY date) AS session_id
        FROM 
            (
            SELECT 
                *,
                LAG(drug_screen_eligible)
                    OVER (PARTITION BY person_id ORDER BY date) != drug_screen_eligible AS new_session,
            FROM all_unnested_cte
            )
        )
    GROUP BY 1,2,3
"""

US_TN_COMPLIANT_REPORTING_DRUG_SCREEN_ELIGIBLE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    base_dataset=STATE_BASE_DATASET,
    view_id=US_TN_COMPLIANT_REPORTING_DRUG_SCREEN_ELIGIBLE_VIEW_NAME,
    description=US_TN_COMPLIANT_REPORTING_DRUG_SCREEN_ELIGIBLE_VIEW_DESCRIPTION,
    view_query_template=US_TN_COMPLIANT_REPORTING_DRUG_SCREEN_ELIGIBLE_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_COMPLIANT_REPORTING_DRUG_SCREEN_ELIGIBLE_VIEW_BUILDER.build_and_print()
