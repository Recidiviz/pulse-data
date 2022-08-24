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
Compliant Reporting due to offense types, drug screening results, and other sentence-based exclusions."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
    US_TN_RAW_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_COMPLIANT_REPORTING_OFFENSE_ELIGIBLE_VIEW_NAME = (
    "us_tn_compliant_reporting_offense_eligible"
)

US_TN_COMPLIANT_REPORTING_OFFENSE_ELIGIBLE_VIEW_DESCRIPTION = "Creates a view that surfaces sessions during which clients are eligible for Compliant Reporting due to offense types and other sentence-based exclusions."

US_TN_COMPLIANT_REPORTING_OFFENSE_ELIGIBLE_QUERY_TEMPLATE = """
    WITH date_array AS (
        SELECT date FROM UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 5 YEAR), CURRENT_DATE('US/Eastern'))) AS date
    )
    ,
    zero_tolerance_codes AS (
        /* Identify spans of time between issued zero tolerance codes */
        SELECT DISTINCT 
            person_id, 
            CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) AS zero_tolerance_sanction_date,
            LEAD(CAST(CAST(ContactNoteDateTime AS datetime) AS DATE)) 
                OVER(PARTITION BY OffenderID ORDER BY CAST(CAST(ContactNoteDateTime AS datetime) AS DATE)) AS zero_tolerance_sanction_end_date
        FROM `{project_id}.{raw_dataset}.ContactNoteType_latest` a
        INNER JOIN `{project_id}.{base_dataset}.state_person_external_id` pei
            ON a.OffenderID = pei.external_id
            AND pei.state_code = 'US_TN' 
        WHERE ContactNoteType IN ('VWAR','PWAR','ZTVR','COHC')
    )
    ,
    sent_start_sessions AS (
        /* Get spans of time between sentence starts (to use in join to get the closest sentence start) */
        SELECT DISTINCT
            person_id,
            sentence_effective_date,
            drug_offense,
            COALESCE(expiration_date, full_expiration_date) AS expiration_date,
            LEAD(sentence_effective_date) OVER (PARTITION BY person_id ORDER BY sentence_effective_date) AS next_sentence_effective_date
        FROM `{project_id}.{analyst_dataset}.us_tn_cr_raw_sentence_preprocessing_materialized`
        WHERE sentence_source != 'PriorRecord'
    )
    ,
    sentence_serving_sessions AS (
        SELECT
            person_id,
            missing_offense,
            drug_offense,
            sentence_effective_date AS start_date,
            COALESCE(expiration_date, full_expiration_date) AS end_date,
        FROM `{project_id}.{analyst_dataset}.us_tn_cr_raw_sentence_preprocessing_materialized`
        WHERE sentence_source != 'PriorRecord'
    )
    ,
    offense_serving_eligibility_sessions AS (
    /* Defines periods of ineligibility due to sentence type based on start date to expiration date */
        SELECT 
            person_id,
            docket_number,
            sentence_effective_date AS start_date,
            sentence_source,
            # Null expiration dates if sentence is a prior record, or 
            # if expiration date occurs far in the future (to avoid overflow errors with adding dates).
            # If DUI, then set end date for eligibility period to five years after offense date or full expiration date, whichever is greater
            CASE 
                WHEN sentence_source = 'PriorRecord' THEN NULL
                # WHEN has_active_sentence = 1 THEN NULL
                WHEN COALESCE(expiration_date, full_expiration_date) > DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 100 YEAR) THEN NULL
                WHEN dui_flag = 1 THEN GREATEST(COALESCE(expiration_date, full_expiration_date), DATE_ADD(offense_date, INTERVAL 5 YEAR))
                ELSE COALESCE(expiration_date, full_expiration_date)
            END AS end_date,
            
            CASE 
                WHEN sentence_source = 'PriorRecord' THEN NULL
                WHEN has_active_sentence = 1 THEN NULL
                WHEN COALESCE(expiration_date, full_expiration_date) > DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 100 YEAR) THEN NULL
                WHEN dui_flag = 1 THEN NULL
                WHEN homicide_flag = 1 THEN NULL
                ELSE DATE_ADD(COALESCE(expiration_date, full_expiration_date), INTERVAL 10 YEAR)
            END AS discretionary_end_date,
            
            COALESCE(expiration_date, full_expiration_date) AS expiration_date,
            
            # Only count as missing offense or drug offense if not in prior record
            IF(sentence_source = 'PriorRecord', 0, missing_offense) AS missing_offense,
            IF(sentence_source = 'PriorRecord', 0, drug_offense) AS drug_offense,
            unknown_offense_flag,
            
            # If excluded offense is in prior record sentence, count as discretionary but not eligible
            CASE 
                WHEN greatest(domestic_flag, sex_offense_flag, assaultive_offense_flag, young_victim_flag) = 1 AND sentence_source != 'PriorRecord'
                    THEN 0
                WHEN homicide_flag = 1 
                    THEN 0 
                ELSE 1 END AS eligible,
            
            CASE
                WHEN greatest(IF(sentence_source = 'PriorRecord', 0, missing_offense),maybe_assaultive_flag,unknown_offense_flag) = 1 THEN 0 
                WHEN greatest(domestic_flag, sex_offense_flag, assaultive_offense_flag, young_victim_flag) = 1 AND sentence_source = 'PriorRecord' THEN 0
                ELSE 1 END AS eligible_discretionary,
            
            no_lifetime_flag,
            conviction_county,
                        
        FROM `{project_id}.{analyst_dataset}.us_tn_cr_raw_sentence_preprocessing_materialized`
    )
    ,
    offense_eligibility_unnested_cte AS (
    /* Unnests eligibility based on offense type at the person-day level */
    SELECT
        b.person_id,
        date,
        compartment_level_0 = 'SUPERVISION' AS is_on_supervision,
        MAX(c1.missing_offense) AS missing_offense,
        
        /* If someone is not currently serving excluded offense but is within 10 years of the expiration of 
        excluded offense, we get MIN(c1.eligible) = 0 and MIN(c2.eligible) = 1 -- so person is not c1 eligible
        but is c2 and c3 eligible. */
        CASE 
            WHEN MIN(c1.eligible_discretionary) = 0 THEN 0
            WHEN MIN(c2.eligible_discretionary) = 0 THEN 0
            WHEN MIN(c1.eligible) = 0 THEN 0
            WHEN MIN(c2.eligible) = 0 THEN 0
            ELSE 1 END AS c1_eligible,

        CASE 
            WHEN MIN(c1.eligible) = 0 THEN 0
            ELSE 1 END AS c2_eligible,

        CASE 
            WHEN MIN(c1.eligible) = 0 THEN 0
            ELSE 1 END AS c3_eligible,
        
        # Check if a zero tolerance sanction has occurred after the earliest active sentence effective date (c1), 
        # or the latest non-active sentence effective date (s)
        CASE WHEN COUNTIF(z.zero_tolerance_sanction_date > COALESCE(c1.start_date, s.sentence_effective_date)) > 0
            THEN 1 ELSE 0 END AS has_zero_tolerance_sanction,
            
        # Check if person has no currently serving sentences, or if the maximum expiration date is within 90 days
        # of the date - mark as overdue for discharge or impending overdue
        CASE 
            # Can't be overdue for discharge if person has zero tolerance sanction.
            WHEN COUNTIF(z.zero_tolerance_sanction_date > COALESCE(c1.start_date, s.sentence_effective_date)) > 0 THEN 0
            
            # Can't be overdue for discharge if serving a lifetime supervision sentence
            WHEN COALESCE(MIN(c1.no_lifetime_flag), 1) = 0 THEN 0
            
            # # If no active sentences, then person is overdue
            # WHEN COUNTIF(c1.start_date IS NOT NULL) = 0 THEN 1
            
            # If the latest active sentence expiration date is within 90 days of current date, then person is overdue
            WHEN MAX(c1.expiration_date) <= DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 90 DAY) THEN 1 
            ELSE 0 END overdue_for_discharge_90_days,
        
        CASE WHEN COUNT(c1.expiration_date) = 0 THEN 1 ELSE 0 END AS missing_sent,
        
        # If any sentence currently being served is a lifetime supervision sentence, no_lifetime_flag is set to 0
        COALESCE(MIN(c1.no_lifetime_flag), 1) AS no_lifetime_flag,
        CASE WHEN COUNTIF(compartment_level_2 IN ("PAROLE","DUAL")) > 0 THEN 1 ELSE 0 END AS parole_flag,
        CASE WHEN COUNTIF(compartment_level_2 IN ("PROBATION","DUAL")) > 0 THEN 1 ELSE 0 END AS probation_flag,
        
    FROM date_array a
    INNER JOIN `{project_id}.{sessions_dataset}.compartment_level_2_super_sessions_materialized` b
        ON date BETWEEN start_date AND COALESCE(end_date, CURRENT_DATE('US/Eastern'))
        AND state_code = 'US_TN'
    LEFT JOIN offense_serving_eligibility_sessions c1
        ON b.person_id = c1.person_id
        AND date BETWEEN c1.start_date AND COALESCE(c1.end_date, '9999-01-01')
    LEFT JOIN offense_serving_eligibility_sessions c2
        ON b.person_id = c2.person_id
        AND date BETWEEN COALESCE(c2.end_date, CURRENT_DATE('US/Eastern')) AND COALESCE(c2.discretionary_end_date, '9999-01-01')
    -- helps us find the most recent sentence start date on a given date, even if it's not being actively served
    LEFT JOIN sent_start_sessions s
        ON b.person_id = s.person_id
        AND date BETWEEN s.sentence_effective_date AND COALESCE(s.next_sentence_effective_date, '9999-01-01')
    LEFT JOIN zero_tolerance_codes z
        ON b.person_id = z.person_id
        AND date BETWEEN z.zero_tolerance_sanction_date AND COALESCE(zero_tolerance_sanction_end_date, '9999-01-01')
        AND z.zero_tolerance_sanction_date > CASE WHEN COALESCE(c1.sentence_source,'PriorRecord') != "PriorRecord" THEN c1.start_date
                                             ELSE s.sentence_effective_date END
        # AND c1.sentence_source != "PriorRecord"
    GROUP BY 1,2,3
    )
    ,
    all_unnested_cte AS (
        SELECT
            person_id,
            date,
            is_on_supervision,
            overdue_for_discharge_90_days,
            no_lifetime_flag,
            
            CASE 
                WHEN has_zero_tolerance_sanction = 1 AND parole_flag = 0 THEN 3
                WHEN missing_sent = 1 THEN 3
                ELSE 1 END AS zt_discretion_eligibility,
            
            CASE 
                WHEN c1_eligible = 1 THEN 1
                WHEN c2_eligible = 1 THEN 2
                ELSE 0 END AS offense_type_eligibility,
            
            # C1
            CASE 
                WHEN has_zero_tolerance_sanction = 1 AND parole_flag = 0 THEN 0
                WHEN overdue_for_discharge_90_days = 1 THEN 0
                WHEN missing_sent = 1 THEN 0
                WHEN no_lifetime_flag = 0 THEN 0
                ELSE c1_eligible END AS c1_eligible,
    
            # C2
            CASE 
                WHEN has_zero_tolerance_sanction = 1 AND parole_flag = 0 THEN 0
                WHEN overdue_for_discharge_90_days = 1 THEN 0
                WHEN missing_sent = 1 THEN 0
                WHEN no_lifetime_flag = 0 THEN 0
                ELSE c2_eligible END AS c2_eligible,

            # C3
            -- Don't exclude someone for overdue for discharge if they are missing sentencing info
            CASE 
                WHEN overdue_for_discharge_90_days = 1 AND missing_sent = 0 THEN 0
                WHEN no_lifetime_flag = 0 THEN 0
                ELSE c3_eligible END AS c3_eligible

    
        FROM offense_eligibility_unnested_cte
    ),
    determine_eligibility AS (
        SELECT 
            person_id,
            date,
            is_on_supervision,
            c1_eligible,
            c2_eligible,
            c3_eligible,
            CASE WHEN 
                LEAST(
                        1 - overdue_for_discharge_90_days,
                        no_lifetime_flag,
                        zt_discretion_eligibility,
                        offense_type_eligibility
                    ) > 0 
                THEN 
                    GREATEST(
                        1 - overdue_for_discharge_90_days,
                        no_lifetime_flag,
                        zt_discretion_eligibility,
                        offense_type_eligibility
                    )
                ELSE 0 END AS sentencing_eligibility
        FROM all_unnested_cte
                    
    ),
    sessionized_cte AS (
    SELECT
        person_id,
        session_id,
        "US_TN" AS state_code,
        MIN(date) AS start_date,
        MAX(date) AS end_date,
        ANY_VALUE(is_on_supervision) AS is_on_supervision,
        ANY_VALUE(sentencing_eligibility) AS sentencing_eligibility,
        ANY_VALUE(c1_eligible) AS c1_eligible,
        ANY_VALUE(c2_eligible) AS c2_eligible,
        ANY_VALUE(c3_eligible) AS c3_eligible,
    FROM 
        (
        SELECT 
            *,
            SUM(IF(new_session,1,0)) OVER(PARTITION BY person_id ORDER BY date) AS session_id
        FROM 
            (
            SELECT 
                *,
                COALESCE(LAG(CONCAT(sentencing_eligibility,is_on_supervision))
                    OVER (PARTITION BY person_id ORDER BY date),'999') != CONCAT(sentencing_eligibility,is_on_supervision) AS new_session,
            FROM determine_eligibility
            )
        )
    GROUP BY 1,2,3
    )
    SELECT * EXCEPT (is_on_supervision) FROM sessionized_cte
    WHERE is_on_supervision
"""

US_TN_COMPLIANT_REPORTING_OFFENSE_ELIGIBLE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    raw_dataset=US_TN_RAW_DATASET,
    base_dataset=STATE_BASE_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    view_id=US_TN_COMPLIANT_REPORTING_OFFENSE_ELIGIBLE_VIEW_NAME,
    description=US_TN_COMPLIANT_REPORTING_OFFENSE_ELIGIBLE_VIEW_DESCRIPTION,
    view_query_template=US_TN_COMPLIANT_REPORTING_OFFENSE_ELIGIBLE_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_COMPLIANT_REPORTING_OFFENSE_ELIGIBLE_VIEW_BUILDER.build_and_print()
