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
"""Creates a view to identify individuals eligible for Compliant Reporting"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_COMPLIANT_REPORTING_LOGIC_VIEW_NAME = "us_tn_compliant_reporting_logic"

US_TN_COMPLIANT_REPORTING_LOGIC_VIEW_DESCRIPTION = (
    """Creates a view to identify individuals eligible for Compliant Reporting"""
)

US_TN_COMPLIANT_REPORTING_LOGIC_QUERY_TEMPLATE = """
    -- First CTE uses standards list and creates flags to apply supervision level and time spent criteria
    WITH standards AS (
        SELECT * EXCEPT(Offender_ID),
                LPAD(CAST(Offender_ID AS string), 8, '0') AS Offender_ID,
                -- Criteria: 1 year on Minimum, 18 months on Medium
                CASE WHEN Supervision_Level LIKE '%MINIMUM%' AND DATE_DIFF(current_date('US/Eastern'),Plan_Start_Date,MONTH)>=12 THEN 1
                     WHEN Supervision_Level LIKE '%MEDIUM%' AND DATE_DIFF(current_date('US/Eastern'),Plan_Start_Date,MONTH)>=18 THEN 1
                     ELSE 0 END AS time_on_level_flag,
                -- Criteria: Currently on Minimum or Medium
                CASE WHEN (Supervision_Level LIKE '%MEDIUM%' OR Supervision_Level LIKE '%MINIMUM%') THEN 1 ELSE 0 END AS eligible_supervision_level,
                -- Calculate date when someone became eligible based on supervision level and time on level
                CASE WHEN Supervision_Level LIKE "%MINIMUM%" THEN DATE_ADD(Plan_Start_Date, INTERVAL 12 MONTH)
                     WHEN Supervision_Level LIKE "%MEDIUM%" THEN DATE_ADD(Plan_Start_Date, INTERVAL 18 MONTH)
                    ELSE '9999-01-01' END AS date_sup_level_eligible,
            FROM `{project_id}.{static_reference_dataset}.us_tn_standards_due`
            WHERE date_of_standards = (
                SELECT MAX(date_of_standards)
                FROM `{project_id}.{static_reference_dataset}.us_tn_standards_due`
            )
    ), 
    -- This CTE pulls past supervision plan information to catch edge cases where if someone moved from minimum -> medium less than 18 months ago,
    -- but collectively has been on medium or less for 18 months, that still counts
    supervision_plan_2 AS (
        SELECT *
        FROM `{project_id}.{analyst_dataset}.us_tn_supervision_plan_logic_materialized`
        -- Logic: If someone has an open plan, is on Medium, was on Minimum, and time on medium is < 18 months but time since previous minimum
        -- is > 18 months, we include them
        WHERE SupervisionLevelClean = 'MEDIUM' 
        AND prev_level = 'MINIMUM' 
        AND time_since_prev_start >= 18 
        AND time_since_current_start < 18
        AND PlanEndDate IS NULL
        -- After applying these filters, there should be no duplicates left, but just in case there are multiple open plan periods, this line dedups
        QUALIFY ROW_NUMBER() OVER(PARTITION BY Offender_ID ORDER BY PlanStartDate DESC, PlanType) = 1
    ),
    -- This CTE pulls past supervision plan information to catch edge cases where if someone moved from medium -> minimum less than 12 months ago,
    -- but collectively has been on medium or less for 18 months, that still counts
    supervision_plan_3 AS (
        SELECT *
        FROM `{project_id}.{analyst_dataset}.us_tn_supervision_plan_logic_materialized`
        -- Logic: If someone has an open plan, is on Minimum, was on Medium, and time on Minimum is < 12 months but time since previous Medium
        -- is >= 18 months, we include them
        WHERE SupervisionLevelClean = 'MINIMUM' 
        AND prev_level = 'MEDIUM' 
        AND time_since_prev_start >= 18 
        AND time_since_current_start < 12
        AND PlanEndDate IS NULL
        -- After applying these filters, there should be no duplicates left, but just in case there are multiple open plan periods, this line dedups
        QUALIFY ROW_NUMBER() OVER(PARTITION BY Offender_ID ORDER BY PlanStartDate DESC, PlanType) = 1
    ),
    -- This CTE combines the people with edge case situations in the 2nd CTE and 3rd CTE to the main list in the first CTE
    -- It also creates a flag for "no arrests in past year" which is implemented as "no ARRP in past year" which could be true even if Last_ARR_note is over 12 months old and the last check *was* positive. 
    -- However, VERY few people have arrest checks that are more than 12 months old, so effectively this means we're just checking that the last ARR check is negative
    -- If someone had a positive arrest check in the past year, but their last one is negative, we're still considering them eligible
    -- We don't have full arrest history so we can't check this another way
    sup_plan_standards AS (
        SELECT standards.*,
            CASE WHEN standards.supervision_level LIKE '%MEDIUM%' AND supervision_plan_2.Offender_ID IS NOT NULL THEN 1
                WHEN standards.supervision_level LIKE '%MINIMUM%' AND supervision_plan_3.Offender_ID IS NOT NULL THEN 1
                ELSE time_on_level_flag
                END AS time_on_level_flag_adj,
            CASE WHEN standards.supervision_level LIKE '%MEDIUM%' AND supervision_plan_2.Offender_ID IS NOT NULL THEN supervision_plan_2.prev_start
                WHEN standards.supervision_level LIKE '%MINIMUM%' AND supervision_plan_3.Offender_ID IS NOT NULL THEN supervision_plan_3.prev_start
                ELSE Plan_Start_Date 
                END AS eligible_level_start,
            -- Criteria: No Arrests in past year
            CASE WHEN Last_ARR_Type = 'ARRP' AND DATE_DIFF(current_date('US/Eastern'),Last_ARR_Note,MONTH)<12 THEN 0
                 ELSE 1 END AS no_arrests_flag,
            CASE WHEN DATE_DIFF(current_date('US/Eastern'),Last_ARR_Note,MONTH)<12 THEN Last_ARR_Type
                ELSE NULL END AS arrests_past_year,
            -- Date when someone became eligible for this criteria
            -- If last ARR type is ARRP (positive), add 1 year to Last_ARR_Note, i.e. they became eligible 1 year since that check
            -- Else, pick some *very early date* since someone could have met the criteria of "no arrests in past year" at any time in the past, and it's likely
            -- that another eligibility date will be the "limiting" date
            CASE WHEN Last_ARR_Type = 'ARRP' THEN DATE_ADD(Last_ARR_Note, INTERVAL 12 MONTH)
                 ELSE '1900-01-01'
                 END AS date_arrest_check_eligible
        FROM standards
        LEFT JOIN supervision_plan_2 
            USING(Offender_ID)
        LEFT JOIN supervision_plan_3 
            USING(Offender_ID)
    ),
    -- Joins together sentences and standards sheet
    sentences_join AS (
        SELECT *, 
            CASE WHEN sentence_put_together.Offender_ID IS NULL THEN 1 ELSE 0 END AS missing_sent_info
        FROM sup_plan_standards
        LEFT JOIN `{project_id}.{analyst_dataset}.us_tn_sentence_logic_materialized` sentence_put_together
            USING(Offender_ID)
    ),
    -- This CTE pulls information on drug contacts to understand who satisfies the criteria for drug screens
    contacts_cte AS (
        SELECT OffenderID AS Offender_ID, 
                CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) AS contact_date, 
                ContactNoteType,
                CASE WHEN ContactNoteType IN ('DRUN','DRUX','DRUM') THEN 1 ELSE 0 END AS negative_drug_test,
                1 AS drug_screen,
        FROM `{project_id}.us_tn_raw_data_up_to_date_views.ContactNoteType_latest`
        -- Limit to DRU% (DRUN,DRUM, etc) type contacts in the last 12 months 
        WHERE DATE_DIFF(CURRENT_DATE,CAST(CAST(ContactNoteDateTime AS datetime) AS DATE),MONTH)<=12
        AND ContactNoteType LIKE '%DRU%'
    ),
    -- This CTE calculates total drug screens and total negative screens in the past year
    dru_contacts AS (
        SELECT Offender_ID, 
                sum(drug_screen) AS total_screens_in_past_year,
                sum(negative_drug_test) as sum_negative_tests_in_past_year, 
                max(most_recent_test_negative) as most_recent_test_negative
        FROM (
            SELECT *,
                -- Since we've already limited to just drug screen contacts, this looks at whether the latest screen was a negative one 
                CASE WHEN ROW_NUMBER() OVER(PARTITION BY Offender_ID ORDER BY CAST(CAST(contact_date AS datetime) AS DATE) DESC) = 1
                     AND ContactNoteType IN ('DRUN','DRUX','DRUM') THEN 1 ELSE 0 END AS most_recent_test_negative
            FROM contacts_cte 
        )
        GROUP BY 1    
        ORDER BY 2 desc
    ), 
    -- This CTE keeps all negative screens in the last year as an array to be displayed
    contacts_cte_arrays AS (
        SELECT Offender_ID, ARRAY_AGG(STRUCT(ContactNoteType,contact_date)) as DRUN_array
        FROM contacts_cte 
        WHERE ContactNoteType IN ('DRUN','DRUX','DRUM')
        GROUP BY 1
    ),
    -- Earliest DRUN for date eligibility - for each person, keeps the earliest date they received a DRUN
    -- This will be useful for determining date of eligibility for people without drug offenses
    drun_contacts AS (
        SELECT OffenderID AS Offender_ID, MIN(CAST(CAST(ContactNoteDateTime AS datetime) AS DATE)) AS earliest_DRUN_date
        FROM `{project_id}.us_tn_raw_data_up_to_date_views.ContactNoteType_latest`
        WHERE ContactNoteType IN ('DRUN','DRUX','DRUM')
        GROUP BY 1
    ),
    -- This CTE excludes people who have a flag indicating Lifetime Supervision or Life Sentence - both are ineligible for CR or overdue
    CSL AS (
        SELECT OffenderID as Offender_ID, LifetimeSupervision, LifeDeathHabitual 
        FROM `{project_id}.us_tn_raw_data_up_to_date_views.JOSentence_latest`
        WHERE TRUE
        QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderID ORDER BY CASE WHEN LifetimeSupervision = 'Y' THEN 0 ELSE 1 END,
                                                                    CASE WHEN LifeDeathHabitual IS NOT NULL THEN 0 ELSE 1 END) = 1
    ),
    life_sentence_ISC AS (
        SELECT DISTINCT OffenderID AS Offender_ID,
        FROM `{project_id}.us_tn_raw_data_up_to_date_views.ISCSentence_latest`
        WHERE sentence LIKE '%LIFE%'
    ),
    -- This CTE excludes people who have been rejected from CR because of criminal record or court order
    CR_rejection_codes AS (
        SELECT DISTINCT OffenderID as Offender_ID
        FROM `{project_id}.us_tn_raw_data_up_to_date_views.ContactNoteType_latest`
        WHERE ContactNoteType IN ('DEIJ','DECR')
    ),
    -- This CTE excludes people who have been rejected from CR for other reasons, that are not static
    -- For example, people can be denied for failure to pay fines/fees, for having compliance problems, etc
    -- But those things could change over time, so perhaps we will exclude people if they have any of these rejection
    -- Codes in the last X months, but not older than that
    -- Rejection codes not included:
        -- DECT: DENIED COMPLIANT REPORTING, INSUFFICENT TIME IN SUPERVISON LEVEL - since we're checking for that
    CR_rejection_codes_dynamic AS (
        SELECT DISTINCT OffenderID as Offender_ID
        FROM `{project_id}.us_tn_raw_data_up_to_date_views.ContactNoteType_latest`
        WHERE ContactNoteType IN 
                                -- DENIED, NO EFFORT TO PAY FINE AND COSTS
                                ('DECF',
                                -- DENIED, NO EFFORT TO PAY FEES
                                'DEDF',
                                -- DENIED, SERIOUS COMPLIANCE PROBLEMS
                                'DEDU',
                                -- DENIED FOR IOT
                                'DEIO',
                                -- DENIED FOR FAILURE TO REPORT AS INSTRUCTD
                                'DEIR'
                                )
        AND CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) >= DATE_SUB(current_date('US/Eastern'), INTERVAL 3 MONTH)
    ),
    rejection_codes_max_date AS (
        SELECT OffenderID as Offender_ID, MAX(CAST(CAST(ContactNoteDateTime AS datetime) AS DATE)) AS latest_cr_rejection_code_date
        FROM `{project_id}.us_tn_raw_data_up_to_date_views.ContactNoteType_latest`
        WHERE ContactNoteType IN ('DECF', 'DEDF','DEDU','DEIO','DEIR')
        GROUP BY 1
    ),
    -- serious sanctions criteria
    sanctions AS (
        SELECT DISTINCT OffenderID as Offender_ID,
        FROM `{project_id}.us_tn_raw_data_up_to_date_views.Violations_latest`
        JOIN `{project_id}.us_tn_raw_data_up_to_date_views.Sanctions_latest` 
            USING(TriggerNumber)
        WHERE CAST(SanctionLevel AS INT) > 1
            AND CAST(CAST(ProposedDate AS datetime) AS DATE) >= DATE_SUB(CURRENT_DATE('US/Eastern'),INTERVAL 12 MONTH)
    ),
    sanctions_array AS (
        SELECT OffenderID as Offender_ID, ARRAY_AGG(ProposedSanction IGNORE NULLS) AS sanctions_in_last_year
        FROM `{project_id}.us_tn_raw_data_up_to_date_views.Violations_latest`
        JOIN `{project_id}.us_tn_raw_data_up_to_date_views.Sanctions_latest` 
            USING(TriggerNumber)
        WHERE CAST(CAST(ProposedDate AS datetime) AS DATE) >= DATE_SUB(CURRENT_DATE('US/Eastern'),INTERVAL 12 MONTH)
        GROUP BY 1
    ),
    sanction_eligible_date AS (
        SELECT OffenderID as Offender_ID, 
              MAX(CASE WHEN CAST(SanctionLevel AS INT) > 1 THEN DATE_ADD(CAST(CAST(ProposedDate AS datetime) AS DATE), INTERVAL 12 MONTH)
                  ELSE '1900-01-01' END) AS date_sanction_eligible
        FROM `{project_id}.us_tn_raw_data_up_to_date_views.Violations_latest`
        JOIN `{project_id}.us_tn_raw_data_up_to_date_views.Sanctions_latest` 
            USING(TriggerNumber)
        GROUP BY 1
    ),
    -- we conservatively use these codes 
    zero_tolerance_codes AS (
        SELECT OffenderID as Offender_ID, 
                ARRAY_AGG(STRUCT(ContactNoteType,CAST(CAST(ContactNoteDateTime AS datetime) AS DATE))) AS zt_codes,
                MAX(CAST(CAST(ContactNoteDateTime AS datetime) AS DATE)) AS latest_zero_tolerance_sanction_date
        FROM `{project_id}.us_tn_raw_data_up_to_date_views.ContactNoteType_latest`
        WHERE ContactNoteType IN ('VWAR','PWAR','ZTVR','COHC')
        GROUP BY OffenderID
    ),
    person_status_cte AS (
            SELECT OffenderID as Offender_ID, OffenderStatus AS person_status
            FROM `{project_id}.us_tn_raw_data_up_to_date_views.OffenderName_latest`
            WHERE TRUE
            QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderID 
                                    ORDER BY SequenceNumber DESC) = 1
    ),
    -- FINES AND FEES
    -- Calculate unpaid amount from invoices
    exemptions AS (
        SELECT CAST(SPLIT(StartDate,' ')[OFFSET(0)] AS DATE) as StartDate,
                CAST(SPLIT(EndDate,' ')[OFFSET(0)] AS DATE) as EndDate,
                FeeItemID,
                AccountSAK,
                ReasonCode,
                CAST(ExemptAmount AS FLOAT64) AS ExemptAmount
        FROM `{project_id}.us_tn_raw_data_up_to_date_views.OffenderExemptions_latest`
    ),
    inv AS (
        SELECT * EXCEPT(InvoiceDate, UnPaidAmount, InvoiceAmount),
                        CAST(SPLIT(InvoiceDate,' ')[OFFSET(0)] AS DATE) as InvoiceDate,
                        ROUND(CAST(UnPaidAmount AS FLOAT64)) AS UnPaidAmount,
                        CAST(InvoiceAmount AS FLOAT64 ) AS InvoiceAmount,
        FROM `{project_id}.us_tn_raw_data_up_to_date_views.OffenderInvoices_latest`
    ),
    -- Returns all accounts with permanent exemption types
    permanent_exemptions AS (
        SELECT DISTINCT exemptions.AccountSAK
              FROM exemptions 
              WHERE ReasonCode IN ('SSDB','JORD','CORD','SISS')               
        ),
    current_exemptions AS (
        SELECT AccountSAK, ARRAY_AGG(ReasonCode IGNORE NULLS) AS current_exemption_type
              FROM exemptions 
              WHERE EndDate IS NULL
        GROUP BY 1
        ),
    -- This calculates unpaid total after subtracting any exemption amounts, since often an invoice is run on the same date
    -- that an exemption goes into effect
    arrearages AS (
            SELECT 
                inv.AccountSAK,
                SUM(UnPaidAmount - COALESCE(ExemptAmount,0)) as unpaid_total, 
            FROM inv
            LEFT JOIN exemptions
                ON inv.AccountSAK = exemptions.AccountSAK
                AND inv.FeeItemID = exemptions.FeeItemID
                AND inv.InvoiceDate BETWEEN exemptions.StartDate AND COALESCE(exemptions.EndDate,'9999-01-01')
            GROUP BY 1
    ),
    -- Join onto main OFS table that links accounts and people
    -- If someone has an account but nothing in invoices, this could be because they have an exemption, so no invoices are generated
    -- Therefore assume unpaid amount is 0
    all_accounts AS (
        SELECT AccountSAK, OffenderID as Offender_ID, COALESCE(unpaid_total,0) AS unpaid_total
        FROM `{project_id}.us_tn_raw_data_up_to_date_views.OffenderAccounts_latest` 
        LEFT JOIN arrearages 
            USING(AccountSAK)
    ),
    payments AS (
        SELECT AccountSAK, 
            CAST(SPLIT(PaymentDate,' ')[OFFSET(0)] AS DATE) as PaymentDate,
            CAST(PaidAmount AS FLOAT64) - CAST(UnAppliedAmount AS FLOAT64) AS PaidAmount,
        FROM `{project_id}.us_tn_raw_data_up_to_date_views.OffenderPayments_latest`
    ),
    sum_payments AS (
        SELECT AccountSAK, PaymentDate, SUM(PaidAmount) as last_paid_amount
        FROM payments 
        GROUP BY 1,2
    ),
    account_flags AS (
        SELECT 
            AccountSAK, 
            MAX(PaymentDate) as most_recent_payment,
            MAX(CASE WHEN DATE_TRUNC(PaymentDate,MONTH) = DATE_TRUNC(current_date('US/Eastern'),MONTH) THEN 1 ELSE 0 END) as have_curr_month_payment,
            MAX(CASE WHEN DATE_TRUNC(PaymentDate,MONTH) = DATE_SUB(DATE_TRUNC(current_date('US/Eastern'),MONTH), INTERVAL 1 MONTH) THEN 1 ELSE 0 END) as have_past_1_month_payment,
            MAX(CASE WHEN DATE_TRUNC(PaymentDate,MONTH) = DATE_SUB(DATE_TRUNC(current_date('US/Eastern'),MONTH), INTERVAL 2 MONTH) THEN 1 ELSE 0 END) as have_past_2_month_payment,
            MAX(CASE WHEN DATE_TRUNC(PaymentDate,MONTH) = DATE_SUB(DATE_TRUNC(current_date('US/Eastern'),MONTH), INTERVAL 3 MONTH) THEN 1 ELSE 0 END ) as have_past_3_month_payment,
            MAX(CASE WHEN DATE_TRUNC(PaymentDate,MONTH) >= DATE_SUB(DATE_TRUNC(current_date('US/Eastern'),MONTH), INTERVAL 6 MONTH) THEN 1 ELSE 0 END ) as have_at_least_1_past_6_mo_payment,
            MAX(CASE WHEN DATE_TRUNC(PaymentDate,MONTH) >= DATE_SUB(DATE_TRUNC(current_date('US/Eastern'),MONTH), INTERVAL 12 MONTH) THEN 1 ELSE 0 END ) as have_at_least_1_past_12_mo_payment,
        FROM payments
        GROUP BY 1
    ),
    -- When pulling people overdue for discharge, we also want to be able to include people who may be overdue, and the process of closing out the case
    -- may have started (i.e. a TEPE contact note entered by PO, or a ZZZZ/C entered by a supervisor) but who are still showing up on supervision
    latest_tepe AS (
        SELECT OffenderID as Offender_ID,
                CAST(SPLIT(ContactNoteDateTime,' ')[OFFSET(0)] AS DATE) as latest_tepe_date
        FROM `{project_id}.us_tn_raw_data_up_to_date_views.ContactNoteType_latest`
        WHERE ContactNoteType = 'TEPE'
        QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderID ORDER BY ContactNoteDateTime DESC) = 1
    ),
    latest_zzz AS (
        SELECT OffenderID as Offender_ID, 
                CAST(SPLIT(ContactNoteDateTime,' ')[OFFSET(0)] AS DATE) as latest_zzz_date,
                ContactNoteType as latest_zzz_code,
        FROM `{project_id}.us_tn_raw_data_up_to_date_views.ContactNoteType_latest`
        WHERE ContactNoteType IN ('ZZZZ','ZZZC')
        QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderID ORDER BY ContactNoteDateTime DESC) = 1
    ), 
    latest_off_mvmt AS (
    SELECT OffenderID as Offender_ID,
            CAST(SPLIT(LastUpdateDate,' ')[OFFSET(0)] AS DATE) as latest_mvmt_date,
            MovementType as latest_mvmt_code,
    FROM `{project_id}.us_tn_raw_data_up_to_date_views.OffenderMovement_latest`
    WHERE MovementType LIKE '%DI%'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderID ORDER BY MovementDateTime DESC) = 1
    ),
    special_conditions AS (
        SELECT person_id, ARRAY_AGG(conditions IGNORE NULLS) AS special_conditions_on_current_sentences
        FROM (
            SELECT person_id, conditions
            FROM `{project_id}.{base_dataset}.state_supervision_sentence`
            WHERE state_code ='US_TN'
            AND completion_date >= CURRENT_DATE('US/Eastern')
            
            UNION ALL
            
            SELECT person_id, conditions
            FROM `{project_id}.{base_dataset}.state_incarceration_sentence`
            WHERE state_code ='US_TN'
            AND completion_date >= CURRENT_DATE('US/Eastern')
        )
        GROUP BY 1
    ),
    add_more_flags_1 AS (
        SELECT sentences_join.* ,
                dru_contacts.* EXCEPT(total_screens_in_past_year,Offender_ID),
                account_flags.*,
                sum_payments.last_paid_amount,
                all_accounts.* EXCEPT(Offender_ID),
                zt_codes,
            -- General pattern:
            -- a) if dont have the flag now, didnt have it in past sentences or prior record, eligible
            -- b) if don't have the flag now, didnt have it in prior record, DID have it in past sentences but those sentences expired over 10 years ago, eligible
            -- c) if dont have flag now, had it for inactive sentences, and all past sentences not expired over 10 years ago, discretion
            -- d) if dont have flag now, had it for prior record, discretion
            CASE WHEN COALESCE(domestic_flag,0) = 0 AND COALESCE(domestic_flag_ever,0) = 0 AND COALESCE(domestic_flag_prior,0) = 0 THEN 'Eligible'
                 WHEN COALESCE(domestic_flag,0) = 0 AND COALESCE(domestic_flag_prior,0) = 0 AND COALESCE(domestic_flag_ever,0) = 1 AND COALESCE(all_domestic_offenses_expired,0) = 1 THEN 'Eligible - Expired'
                 WHEN COALESCE(domestic_flag,0) = 0 AND COALESCE(domestic_flag_ever,0) = 1 AND COALESCE(all_domestic_offenses_expired,0) = 0 THEN 'Discretion'
                 WHEN COALESCE(domestic_flag,0) = 0 AND COALESCE(domestic_flag_prior,0) = 1 THEN 'Discretion'
                 ELSE 'Ineligible' END AS domestic_flag_eligibility,
            CASE WHEN COALESCE(sex_offense_flag,0) = 0 AND COALESCE(sex_offense_flag_ever,0) = 0 AND COALESCE(sex_offense_flag_prior,0) = 0  THEN 'Eligible'
                 WHEN COALESCE(sex_offense_flag,0) = 0 AND COALESCE(sex_offense_flag_prior,0) = 0 AND COALESCE(sex_offense_flag_ever,0) = 1 AND COALESCE(all_sex_offenses_expired,0) = 1 THEN 'Eligible - Expired'
                 WHEN COALESCE(sex_offense_flag,0) = 0 AND COALESCE(sex_offense_flag_ever,0) = 1 AND COALESCE(all_sex_offenses_expired,0) = 0 THEN 'Discretion'
                 WHEN COALESCE(sex_offense_flag,0) = 0 AND COALESCE(sex_offense_flag_prior,0) = 1 THEN 'Discretion'
                 ELSE 'Ineligible' END AS sex_offense_flag_eligibility,
            CASE WHEN COALESCE(assaultive_offense_flag,0) = 0 AND COALESCE(assaultive_offense_flag_ever,0) = 0 AND COALESCE(assaultive_offense_flag_prior,0) = 0 THEN 'Eligible'
                 WHEN COALESCE(assaultive_offense_flag,0) = 0 AND COALESCE(assaultive_offense_flag_prior,0) = 0 AND COALESCE(assaultive_offense_flag_ever,0) = 1 AND COALESCE(all_assaultive_offenses_expired,0) = 1 THEN 'Eligible - Expired'
                 WHEN COALESCE(assaultive_offense_flag,0) = 0 AND COALESCE(assaultive_offense_flag_ever,0) = 1 AND COALESCE(all_assaultive_offenses_expired,0) = 0 THEN 'Discretion'
                 WHEN COALESCE(assaultive_offense_flag,0) = 0 AND COALESCE(assaultive_offense_flag_prior,0) = 1 THEN 'Discretion'
                 ELSE 'Ineligible' END AS assaultive_offense_flag_eligibility,
            CASE WHEN COALESCE(maybe_assaultive_flag,0) = 0 AND COALESCE(maybe_assaultive_flag_ever,0) = 0 AND COALESCE(maybe_assaultive_flag_prior,0) = 0 THEN 'Eligible'
                 WHEN COALESCE(maybe_assaultive_flag,0) = 0 AND COALESCE(maybe_assaultive_flag_prior,0) = 0 AND COALESCE(maybe_assaultive_flag_ever,0) = 1 AND COALESCE(all_maybe_assaultive_offenses_expired,0) = 1 THEN 'Eligible - Expired'
                 WHEN COALESCE(maybe_assaultive_flag,0) = 0 AND COALESCE(maybe_assaultive_flag_ever,0) = 1 AND COALESCE(all_maybe_assaultive_offenses_expired,0) = 0 THEN 'Discretion'
                 WHEN COALESCE(maybe_assaultive_flag,0) = 0 AND COALESCE(maybe_assaultive_flag_prior,0) = 1 THEN 'Discretion'
                 WHEN COALESCE(maybe_assaultive_flag,0) = 1 THEN 'Discretion'
                 ELSE 'Ineligible' END AS maybe_assaultive_flag_eligibility,
            CASE WHEN COALESCE(unknown_offense_flag,0) = 0 AND COALESCE(unknown_offense_flag_ever,0) = 0 AND COALESCE(unknown_offense_flag_prior,0) = 0 THEN 'Eligible'
                 WHEN COALESCE(unknown_offense_flag,0) = 0 AND COALESCE(unknown_offense_flag_prior,0) = 0 AND COALESCE(unknown_offense_flag_ever,0) = 1 AND COALESCE(all_unknown_offenses_expired,0) = 1 THEN 'Eligible - Expired'
                 WHEN COALESCE(unknown_offense_flag,0) = 0 AND COALESCE(unknown_offense_flag_ever,0) = 1 AND COALESCE(all_unknown_offenses_expired,0) = 0 THEN 'Discretion'
                 WHEN COALESCE(unknown_offense_flag,0) = 0 AND COALESCE(unknown_offense_flag_prior,0) = 1 THEN 'Discretion'
                 WHEN COALESCE(unknown_offense_flag,0) = 1 THEN 'Discretion'
                 ELSE 'Ineligible' END AS unknown_offense_flag_eligibility,
            CASE WHEN COALESCE(missing_offense,0) = 0 AND COALESCE(missing_offense_ever,0) = 0 AND COALESCE(missing_offense_prior,0) = 0 THEN 'Eligible'
                 -- If only missing offense is in prior record, I don't think that will / should exclude
                 WHEN COALESCE(missing_offense,0) = 0 AND COALESCE(missing_offense_ever,0) = 0 AND COALESCE(missing_offense_prior,0) = 1 THEN 'Eligible'
                 WHEN COALESCE(missing_offense,0) = 0 AND COALESCE(missing_offense_prior,0) = 0 AND COALESCE(missing_offense_ever,0) = 1 AND COALESCE(all_missing_offenses_expired,0) = 1 THEN 'Eligible - Expired'
                 WHEN COALESCE(missing_offense,0) = 0 AND COALESCE(missing_offense_ever,0) = 1 AND COALESCE(all_missing_offenses_expired,0) = 0 THEN 'Discretion'
                 ELSE 'Discretion' END AS missing_offense_flag_eligibility,
            CASE WHEN COALESCE(young_victim_flag,0) = 0 AND COALESCE(young_victim_flag_ever,0) = 0 AND COALESCE(young_victim_flag_prior,0) = 0 THEN 'Eligible'
                 WHEN COALESCE(young_victim_flag,0) = 0 AND COALESCE(young_victim_flag_prior,0) = 0 AND COALESCE(young_victim_flag_ever,0) = 1 AND COALESCE(all_young_victim_offenses_expired,0) = 1 THEN 'Eligible - Expired'
                 WHEN COALESCE(young_victim_flag,0) = 0 AND COALESCE(young_victim_flag_ever,0) = 1 AND COALESCE(all_young_victim_offenses_expired,0) = 0 THEN 'Discretion'
                 WHEN COALESCE(young_victim_flag,0) = 0 AND COALESCE(young_victim_flag_prior,0) = 1 THEN 'Discretion'
                 ELSE 'Ineligible' END AS young_victim_flag_eligibility,
            CASE WHEN COALESCE(dui_last_5_years_flag,0) = 0 THEN 'Eligible' ELSE 'Ineligible' END AS dui_last_5_years_flag_eligibility,
                 
            CASE WHEN total_screens_in_past_year IS NULL THEN 0 
                ELSE total_screens_in_past_year END AS total_screens_in_past_year,
        -- Logic here: for non-drug-offense:
        -- If there is no latest screen (i.e. no screen in the last ~16 months) they're eligible
        -- If the latest screen is over a year old, they're eligible
        -- If they latest screen is negative, they're eligible
        -- If they have 1 negative test in the past year, they're eligible (this might catch people who have 1 negative test and therefore
        -- meet the requirement, but their latest DRU type is positive)
        CASE 
            -- Potentially comment out
            WHEN COALESCE(drug_offense,drug_offense_ever) = 0 AND Last_DRU_Note IS NULL THEN 1
             -- Potentially comment out
             WHEN COALESCE(drug_offense,drug_offense_ever) = 0 AND DATE_DIFF(CURRENT_DATE,Last_DRU_Note,MONTH)>= 12 THEN 1
             WHEN COALESCE(drug_offense,drug_offense_ever) = 0 and Last_DRU_Type IN ('DRUN','DRUX','DRUM') THEN 1
             -- Potentially comment out
             WHEN COALESCE(drug_offense,drug_offense_ever) = 0 and sum_negative_tests_in_past_year >= 1 THEN 1
             -- These two conditions ensure that selecting this flag doesnt automatically eliminate people missing sentencing info
             -- or where drug offense = 1 since there are separate flags for that
             WHEN COALESCE(drug_offense,drug_offense_ever) IS NULL THEN NULL
             WHEN COALESCE(drug_offense,drug_offense_ever) = 1 THEN NULL
             ELSE 0 END AS drug_screen_pass_flag_non_drug_offense,
        -- Logic here: for drug offenses
        -- If there are 2 negative tests in the past year and the most recent test is negative, eligible
        CASE WHEN COALESCE(drug_offense,drug_offense_ever) = 1 and sum_negative_tests_in_past_year >= 2 and most_recent_test_negative = 1 THEN 1
             -- These two conditions ensure that selecting this flag doesnt automatically eliminate people missing sentencing info
             -- or where drug offense = 0 since there are separate flags for that
             WHEN COALESCE(drug_offense,drug_offense_ever) = 0 THEN NULL
             WHEN COALESCE(drug_offense,drug_offense_ever) IS NULL THEN NULL
             ELSE 0 END AS drug_screen_pass_flag_drug_offense,
        -- Criteria: Special Conditions are up to date
        CASE WHEN SPE_Note_Due <= current_date('US/Eastern') THEN 0 ELSE 1 END AS spe_conditions_not_overdue_flag,
        -- Counties in JD 17 don't allow CR for probation cases
        CASE WHEN judicial_district = '17' AND Case_Type LIKE '%PROBATION%' THEN 0 ELSE 1 END AS eligible_counties,
        -- Exclude anyone on lifetime supervision or with a life sentence
        CASE WHEN COALESCE(LifetimeSupervision,'N') != 'Y' AND COALESCE(LifeDeathHabitual,'N') = 'N' AND life_sentence_ISC.Offender_ID IS NULL THEN 1 
            ELSE 0 END AS no_lifetime_flag,
        -- Exclude anyone from CR who was previous rejected
        CASE WHEN 
            CR_rejection_codes.Offender_ID IS NULL THEN 1 ELSE 0 END AS cr_not_previous_rejected_flag,
        CASE WHEN 
            CR_rejection_codes_dynamic.Offender_ID IS NULL THEN 1 ELSE 0 END AS cr_not_rejected_x_months_flag,
        CASE WHEN zero_tolerance_codes.Offender_ID IS NULL THEN 1 ELSE 0 END AS no_zero_tolerance_codes,
        #CASE WHEN ((unpaid_total <= 2000 AND have_past_1_month_payment = 1 AND have_past_2_month_payment = 1 AND have_past_3_month_payment=1) OR unpaid_total = 0 OR permanent_exemptions.AccountSAK IS NOT NULL) THEN 'Option 1 - Eligible' 
         #    WHEN ((unpaid_total <= 2000 AND have_at_least_1_past_6_mo_payment=1) OR unpaid_total = 0 OR permanent_exemptions.AccountSAK IS NOT NULL) THEN 'Option 2 - Eligible' 
         CASE   WHEN ((unpaid_total <= 500) OR unpaid_total = 0 OR permanent_exemptions.AccountSAK IS NOT NULL) THEN 'Option 3 - Eligible'
            WHEN ((unpaid_total <= 2000) OR unpaid_total = 0 OR permanent_exemptions.AccountSAK IS NOT NULL) THEN 'Option 4 - Eligible' 
            ELSE 'Ineligible' END AS fines_fees_eligible,        
        CASE WHEN sanctions.Offender_ID IS NULL THEN 1 ELSE 0 END as no_serious_sanctions_flag,
        CASE WHEN permanent_exemptions.AccountSAK IS NOT NULL THEN 'Fees Waived' 
             END AS exemption_notes,
        current_exemption_type,
        FROM sentences_join
        LEFT JOIN dru_contacts 
            USING(Offender_ID)
        LEFT JOIN CSL
            USING(Offender_ID)
        LEFT JOIN life_sentence_ISC
            USING(Offender_ID)
        LEFT JOIN CR_rejection_codes 
            USING(Offender_ID)
        LEFT JOIN CR_rejection_codes_dynamic 
            USING(Offender_ID)
        LEFT JOIN all_accounts 
            USING(Offender_ID)
        LEFT JOIN account_flags 
            USING(AccountSAK)
        LEFT JOIN permanent_exemptions 
            USING(AccountSAK)
        LEFT JOIN current_exemptions 
            USING(AccountSAK)
        LEFT JOIN sum_payments 
            ON account_flags.AccountSAK = sum_payments.AccountSAK
            AND account_flags.most_recent_payment = sum_payments.PaymentDate
        LEFT JOIN sanctions
            USING(Offender_ID)
        LEFT JOIN zero_tolerance_codes 
            ON sentences_join.Offender_ID = zero_tolerance_codes.Offender_ID
            AND zero_tolerance_codes.latest_zero_tolerance_sanction_date  > COALESCE(sentence_start_date,'0001-01-01')

    ),
    add_more_flags_2 AS (
        SELECT
            COALESCE(full_name,TO_JSON_STRING(STRUCT(first_name as given_names,"" as middle_names,"" as name_suffix,last_name as surname)))  AS person_name,
            pei.person_id,
            first_name,
            last_name,
            Phone_Number AS phone_number,
            -- TODO(#11790): Pull from Address table instead of Standards Sheet if both are same
            INITCAP(CONCAT(
                Address_Line1, 
                IF(Address_Line2 IS NULL, '', CONCAT(' ', Address_Line2)), 
                ', ', 
                Address_City, 
                ', ', 
                Address_State, 
                ' ', 
                Address_Zip
                )) AS address,
            conviction_county,
            fines_fees_eligible,
            Last_ARR_Note,
            Last_FEE_Note,
            FEE_Note_Due,
            CASE WHEN SPE_Note_Due IS NULL AND Last_SPE_Note IS NULL AND Last_SPET_Date IS NULL THEN 'none'
                 WHEN SPE_Note_Due IS NULL AND Last_SPE_Note IS NOT NULL AND Last_SPET_Date IS NOT NULL THEN 'terminated'
                 WHEN SPE_Note_Due > CURRENT_DATE('US/Eastern') THEN 'current'
                 END AS spe_flag,
            SPE_Note_Due AS  SPE_note_due,
            Last_SPE_Note AS last_SPE_note, 
            Last_SPET_Date AS last_SPET_date,
            EXP_Date AS sentence_expiration_date,
            missing_at_least_1_exp_date,
            COALESCE(has_TN_sentence,0) AS has_TN_sentence,
            sentence_expiration_date_internal,
            COALESCE(sentence_expiration_date_internal,EXP_Date) AS expiration_date,
            DATE_DIFF(COALESCE(sentence_expiration_date_internal,EXP_Date), current_date('US/Eastern'), DAY) AS time_to_expiration_days,
            CASE WHEN COALESCE(sentence_expiration_date_internal,EXP_Date) < current_date('US/Eastern') 
                AND no_lifetime_flag = 1
                AND (no_zero_tolerance_codes = 1 OR Case_Type LIKE '%PAROLE%')
                AND person_status in ("ACTV","PEND")
                AND missing_at_least_1_exp_date = 0
                AND latest_tepe_date IS NOT NULL
                THEN 1 ELSE 0 END AS overdue_for_discharge_tepe,
            CASE WHEN COALESCE(sentence_expiration_date_internal,EXP_Date) < current_date('US/Eastern') 
                AND no_lifetime_flag = 1
                AND (no_zero_tolerance_codes = 1 OR Case_Type LIKE '%PAROLE%')
                AND person_status in ("ACTV","PEND")
                AND missing_at_least_1_exp_date = 0
                AND latest_tepe_date IS NULL
                THEN 1 ELSE 0 END AS overdue_for_discharge_no_case_closure,
            CASE WHEN COALESCE(sentence_expiration_date_internal,EXP_Date) <= DATE_ADD(current_date('US/Eastern'), INTERVAL 90 DAY) 
                AND no_lifetime_flag = 1
                AND (no_zero_tolerance_codes = 1 OR Case_Type LIKE '%PAROLE%')
                AND missing_at_least_1_exp_date = 0
                AND person_status in ("ACTV","PEND")
                THEN 1 ELSE 0 END AS overdue_for_discharge_within_90,
            CASE WHEN COALESCE(sentence_expiration_date_internal,EXP_Date) <= DATE_ADD(current_date('US/Eastern'), INTERVAL 180 DAY) 
                AND no_lifetime_flag = 1
                AND (no_zero_tolerance_codes = 1 OR Case_Type LIKE '%PAROLE%')
                AND missing_at_least_1_exp_date = 0
                AND person_status in ("ACTV","PEND")
                THEN 1 ELSE 0 END AS overdue_for_discharge_within_180,
            sentence_start_date,
            sessions_supervision.start_date AS latest_supervision_start_date,
            DATE_DIFF(COALESCE(EXP_Date,sentence_expiration_date_internal),sentence_start_date,DAY) AS sentence_length_days,
            DATE_DIFF(COALESCE(EXP_Date,sentence_expiration_date_internal),sentence_start_date,YEAR) AS sentence_length_years,
            add_more_flags_1.Offender_ID AS person_external_id,
            person_status,
            special_conditions_on_current_sentences,
            unpaid_total AS current_balance,
            current_exemption_type,
            most_recent_payment AS last_payment_date,
            last_paid_amount AS last_payment_amount,
            exemption_notes,
            latest_tepe_date,
            latest_zzz_date,
            latest_zzz_code,
            latest_mvmt_code,
            latest_mvmt_date,
            Staff_ID AS officer_id,
            Staff_First_Name AS staff_first_name,
            Staff_Last_Name AS staff_last_name,
            Case_Type AS supervision_type,
            judicial_district,
            Supervision_Level AS supervision_level,
            eligible_level_start,
            Plan_Start_Date AS supervision_level_start,
            -- Current offenses is just active offenses. If someone is missing an active sentence, this is null
            active_offenses AS current_offenses,
            -- These are prior offenses that make someone ineligible, but that expired 10+ years ago
            CASE WHEN domestic_flag_eligibility in ('Eligible - Expired')
                OR sex_offense_flag_eligibility in ('Eligible - Expired')
                OR assaultive_offense_flag_eligibility in ('Eligible - Expired')
                OR maybe_assaultive_flag_eligibility in ('Eligible - Expired')
                OR unknown_offense_flag_eligibility in ('Eligible - Expired')
                OR missing_offense_flag_eligibility in ('Eligible - Expired')
                OR young_victim_flag_eligibility in ('Eligible - Expired')
                THEN lifetime_offenses
                END AS lifetime_offenses_expired,
            ARRAY_CONCAT(lifetime_offenses, prior_offenses) AS past_offenses,
            prior_offenses,
            lifetime_offenses AS lifetime_offenses_all,
            docket_numbers,
            has_active_sentence,
            DRUN_array AS last_DRUN,
            sanctions_in_last_year,
            arrests_past_year AS last_arr_check_past_year,
            zt_codes,
            Last_Sanctions_Type AS last_sanction,
            Last_DRU_Note AS last_drug_screen_date,
            Region as district,
            time_on_level_flag_adj,
            no_serious_sanctions_flag,
            no_arrests_flag,
            spe_conditions_not_overdue_flag,
            domestic_flag_eligibility,
            sex_offense_flag_eligibility,
            assaultive_offense_flag_eligibility,
            maybe_assaultive_flag_eligibility,
            unknown_offense_flag_eligibility,
            missing_offense_flag_eligibility,
            young_victim_flag_eligibility,
            dui_last_5_years_flag_eligibility,
            drug_screen_pass_flag_drug_offense,
            drug_screen_pass_flag_non_drug_offense,
            COALESCE(drug_offense,drug_offense_ever) AS drug_offense,
            sum_negative_tests_in_past_year,
            most_recent_test_negative,
            total_screens_in_past_year,
            missing_sent_info,
            eligible_counties,
            eligible_supervision_level,
            cr_not_previous_rejected_flag,
            cr_not_rejected_x_months_flag,
            no_lifetime_flag,
            no_zero_tolerance_codes,   
            earliest_DRUN_date,     
            CASE WHEN domestic_flag_eligibility = 'Eligible'
                        AND sex_offense_flag_eligibility = 'Eligible'
                        AND assaultive_offense_flag_eligibility = 'Eligible'
                        AND maybe_assaultive_flag_eligibility = 'Eligible'
                        AND unknown_offense_flag_eligibility = 'Eligible'
                        AND missing_offense_flag_eligibility = 'Eligible'
                        AND young_victim_flag_eligibility = 'Eligible'
                        AND dui_last_5_years_flag_eligibility = 'Eligible'
                        AND latest_expiration_date_for_excluded_offenses IS NOT NULL THEN DATE_ADD(latest_expiration_date_for_excluded_offenses, INTERVAL 10 YEAR)
                WHEN domestic_flag_eligibility in ('Eligible','Discretion')
                        AND sex_offense_flag_eligibility in ('Eligible','Discretion')
                        AND assaultive_offense_flag_eligibility in ('Eligible','Discretion')
                        AND maybe_assaultive_flag_eligibility in ('Eligible','Discretion')
                        AND unknown_offense_flag_eligibility in ('Eligible','Discretion')
                        AND missing_offense_flag_eligibility in ('Eligible','Discretion')
                        AND young_victim_flag_eligibility in ('Eligible','Discretion')
                        AND dui_last_5_years_flag_eligibility = 'Eligible' THEN '1900-01-01'
                END AS date_offenses_eligible,
            date_sup_level_eligible,
            date_arrest_check_eligible,
            COALESCE(date_sanction_eligible,'1900-01-01') AS date_sanction_eligible,
            COALESCE(Last_SPE_Note,'1900-01-01') AS date_spe_eligible,
            CASE WHEN COALESCE(drug_offense,drug_offense_ever) = 0 AND earliest_DRUN_date >= sessions_supervision.start_date THEN earliest_DRUN_date
                 WHEN COALESCE(drug_offense,drug_offense_ever) = 0 AND earliest_DRUN_date < COALESCE(sessions_supervision.start_date,Plan_Start_Date) THEN COALESCE(Last_DRU_Note,'1900-01-01')
                  WHEN COALESCE(drug_offense,drug_offense_ever) = 0 AND earliest_DRUN_date IS NULL THEN '1900-01-01'
                  WHEN COALESCE(drug_offense,drug_offense_ever) = 1 and sum_negative_tests_in_past_year >= 2 and most_recent_test_negative = 1 THEN Last_DRU_Note
                  ELSE '9999-01-01'
                  END as date_drug_screen_eligible,
            CASE WHEN latest_cr_rejection_code_date IS NOT NULL THEN DATE_ADD(latest_cr_rejection_code_date, INTERVAL 3 MONTH)
                 ELSE '1900-01-01' END AS date_cr_rejection_eligible,        
            CASE WHEN domestic_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND sex_offense_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND assaultive_offense_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND maybe_assaultive_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND unknown_offense_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND missing_offense_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND young_victim_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND dui_last_5_years_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                THEN 1 ELSE 0 END AS eligible_offenses,
            CASE WHEN 
                COALESCE(drug_screen_pass_flag_non_drug_offense,1) = 1
                THEN 1 ELSE 0 END AS eligible_drug_screen_non_drug_offense,
            CASE WHEN 
                COALESCE(drug_screen_pass_flag_drug_offense,1) = 1
                THEN 1 ELSE 0 END AS eligible_drug_screen_drug_offense,
            CASE WHEN eligible_supervision_level = 1
                AND time_on_level_flag_adj = 1 
                AND no_serious_sanctions_flag = 1 
                AND no_arrests_flag = 1
                AND spe_conditions_not_overdue_flag = 1
                AND domestic_flag_eligibility in ('Eligible','Eligible - Expired')
                AND sex_offense_flag_eligibility in ('Eligible','Eligible - Expired')
                AND assaultive_offense_flag_eligibility in ('Eligible','Eligible - Expired')
                AND maybe_assaultive_flag_eligibility in ('Eligible','Eligible - Expired')
                AND unknown_offense_flag_eligibility in ('Eligible','Eligible - Expired')
                AND missing_offense_flag_eligibility in ('Eligible','Eligible - Expired')
                AND young_victim_flag_eligibility in ('Eligible','Eligible - Expired')
                AND dui_last_5_years_flag_eligibility = 'Eligible'
                -- These flags can be null if sentencing info is missing or if drug_offense = 1 or drug_offense = 0, respectively
                AND COALESCE(drug_screen_pass_flag_non_drug_offense,1) = 1
                AND COALESCE(drug_screen_pass_flag_drug_offense,1) = 1
                AND missing_sent_info = 0
                AND cr_not_previous_rejected_flag = 1
                AND cr_not_rejected_x_months_flag = 1
                AND no_lifetime_flag = 1
                -- The logic here is the following: violations on diversion/probation can change someone expiration date, and we have reason to 
                -- think that there are often a lot of judgement orders/revocation orders that affect these supervision types that don't 
                -- appear in the sentencing data (or appear with a major lag). Zero tolerance codes are used as a proxy to conservatively
                -- exclude people from compliant reporting and overdue for discharge, with the assumption that there might be more to their 
                -- sentence that we don't know about. Since parole information is generally more reliable and updated, we don't apply that same
                -- filter to people on parole
                AND (no_zero_tolerance_codes = 1 OR Case_Type LIKE '%PAROLE%')
                AND eligible_counties = 1
                AND fines_fees_eligible not in ('Option 4 - Eligible','Ineligible')
            THEN 1 ELSE 0 END AS all_eligible,
            CASE WHEN eligible_supervision_level = 1
                AND time_on_level_flag_adj = 1 
                AND no_serious_sanctions_flag = 1 
                AND no_arrests_flag = 1
                AND spe_conditions_not_overdue_flag = 1
                AND domestic_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND sex_offense_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND assaultive_offense_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND maybe_assaultive_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND unknown_offense_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND missing_offense_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND young_victim_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND dui_last_5_years_flag_eligibility = 'Eligible'
                -- These flags can be null if sentencing info is missing or if drug_offense = 1 or drug_offense = 0, respectively
                AND COALESCE(drug_screen_pass_flag_non_drug_offense,1) = 1
                AND COALESCE(drug_screen_pass_flag_drug_offense,1) = 1
                AND missing_sent_info = 0
                AND cr_not_previous_rejected_flag = 1
                AND cr_not_rejected_x_months_flag = 1
                AND no_lifetime_flag = 1
                -- The logic here is the following: violations on diversion/probation can change someone expiration date, and we have reason to 
                -- think that there are often a lot of judgement orders/revocation orders that affect these supervision types that don't 
                -- appear in the sentencing data (or appear with a major lag). Zero tolerance codes are used as a proxy to conservatively
                -- exclude people from compliant reporting and overdue for discharge, with the assumption that there might be more to their 
                -- sentence that we don't know about. Since parole information is generally more reliable and updated, we don't apply that same
                -- filter to people on parole
                AND (no_zero_tolerance_codes = 1 OR Case_Type LIKE '%PAROLE%')
                AND eligible_counties = 1
                AND fines_fees_eligible not in ('Option 4 - Eligible','Ineligible')
            THEN 1 ELSE 0 END AS all_eligible_and_offense_discretion,
            CASE WHEN eligible_supervision_level = 1
                AND time_on_level_flag_adj = 1 
                AND no_serious_sanctions_flag = 1 
                AND no_arrests_flag = 1
                AND spe_conditions_not_overdue_flag = 1
                AND domestic_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND sex_offense_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND assaultive_offense_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND maybe_assaultive_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND unknown_offense_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND missing_offense_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND young_victim_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND dui_last_5_years_flag_eligibility = 'Eligible'
                -- These flags can be null if sentencing info is missing or if drug_offense = 1 or drug_offense = 0, respectively
                AND COALESCE(drug_screen_pass_flag_non_drug_offense,1) = 1
                AND COALESCE(drug_screen_pass_flag_drug_offense,1) = 1
                -- Since this flag is for offense type discretion anyway, ok to surface people missing sentence info
                # AND missing_sent_info = 0
                AND cr_not_previous_rejected_flag = 1
                AND cr_not_rejected_x_months_flag = 1
                AND no_lifetime_flag = 1
                AND eligible_counties = 1
                AND fines_fees_eligible not in ('Option 4 - Eligible','Ineligible') 
            THEN 1 ELSE 0 END AS all_eligible_and_offense_and_zt_discretion,
            CASE WHEN eligible_supervision_level = 1
                AND time_on_level_flag_adj = 1 
                AND no_serious_sanctions_flag = 1 
                AND no_arrests_flag = 1
                AND spe_conditions_not_overdue_flag = 1
                AND domestic_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND sex_offense_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND assaultive_offense_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND maybe_assaultive_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND unknown_offense_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND missing_offense_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND young_victim_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND dui_last_5_years_flag_eligibility = 'Eligible'
                -- These flags can be null if sentencing info is missing or if drug_offense = 1 or drug_offense = 0, respectively
                AND COALESCE(drug_screen_pass_flag_non_drug_offense,1) = 1
                AND COALESCE(drug_screen_pass_flag_drug_offense,1) = 1
                AND missing_sent_info = 0
                AND cr_not_previous_rejected_flag = 1
                AND cr_not_rejected_x_months_flag = 1
                AND no_lifetime_flag = 1
                AND eligible_counties = 1
                AND fines_fees_eligible != 'Ineligible'
            THEN 1 ELSE 0 END AS all_eligible_and_offense_and_zt_and_fines_discretion,
        FROM add_more_flags_1    
        LEFT JOIN contacts_cte_arrays 
            USING(Offender_ID)
        LEFT JOIN sanctions_array 
            USING(Offender_ID)
        LEFT JOIN sanction_eligible_date
            USING(Offender_ID)
        LEFT JOIN drun_contacts
            USING(Offender_ID)
        LEFT JOIN rejection_codes_max_date
            USING(Offender_ID)
        LEFT JOIN `{project_id}.{base_dataset}.state_person_external_id` pei
            ON pei.external_id = Offender_ID
        -- Use compartment level 0 to get most recent supervision start date
        LEFT JOIN (
            SELECT *
            FROM `{project_id}.{sessions_dataset}.compartment_level_0_super_sessions_materialized`
            WHERE compartment_level_0 = 'SUPERVISION' 
            QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY session_id_end DESC) = 1 
            ) sessions_supervision
            ON pei.person_id = sessions_supervision.person_id
        LEFT JOIN special_conditions
            ON pei.person_id = special_conditions.person_id
        LEFT JOIN `{project_id}.{base_dataset}.state_person` sp
            ON sp.person_id = pei.person_id
        LEFT JOIN person_status_cte
            USING(Offender_ID)
        LEFT JOIN latest_tepe
            on add_more_flags_1.Offender_ID = latest_tepe.Offender_ID
            AND latest_tepe.latest_tepe_date >= DATE_SUB(COALESCE(sentence_expiration_date_internal,EXP_Date), INTERVAL 30 DAY)
        LEFT JOIN latest_zzz
            on add_more_flags_1.Offender_ID = latest_zzz.Offender_ID
            AND latest_zzz.latest_zzz_date >= latest_tepe.latest_tepe_date
        LEFT JOIN latest_off_mvmt 
            on add_more_flags_1.Offender_ID = latest_off_mvmt.Offender_ID
            AND latest_off_mvmt.latest_mvmt_date >= latest_tepe.latest_tepe_date
    )
    SELECT *,
        GREATEST(date_offenses_eligible,
                date_sup_level_eligible,
                date_arrest_check_eligible,
                date_sanction_eligible,
                date_spe_eligible,
                date_drug_screen_eligible,
                date_cr_rejection_eligible) AS greatest_date_eligible,
        CASE -- People on ICOTS and minimum with no active TN sentences should automatically be on compliant reporting
             WHEN supervision_type LIKE '%ISC%' AND supervision_level LIKE '%MINIMUM%' AND has_TN_sentence = 0 AND no_lifetime_flag = 1 THEN 'c4'
             WHEN all_eligible = 1 AND overdue_for_discharge_no_case_closure = 0 AND overdue_for_discharge_within_90 = 0 THEN 'c1'
             WHEN all_eligible_and_offense_discretion = 1 AND overdue_for_discharge_no_case_closure = 0 AND overdue_for_discharge_within_90 = 0 THEN 'c2'
             WHEN all_eligible_and_offense_and_zt_discretion = 1 AND overdue_for_discharge_no_case_closure = 0 AND overdue_for_discharge_within_90 = 0 THEN 'c3'
             END AS compliant_reporting_eligible,
        CASE WHEN supervision_type LIKE '%ISC%' AND supervision_level LIKE '%MINIMUM%' AND has_TN_sentence = 0 AND no_lifetime_flag = 1 THEN 1 ELSE 0 END AS eligible_c4,
        CASE WHEN all_eligible = 1 AND overdue_for_discharge_no_case_closure = 0 AND overdue_for_discharge_within_90 = 0 THEN 1 ELSE 0 END AS eligible_c1,
        CASE WHEN all_eligible_and_offense_discretion = 1 AND overdue_for_discharge_no_case_closure = 0 AND overdue_for_discharge_within_90 = 0 THEN 1 ELSE 0 END AS eligible_c2,
        CASE WHEN all_eligible_and_offense_and_zt_discretion = 1 AND overdue_for_discharge_no_case_closure = 0 AND overdue_for_discharge_within_90 = 0 THEN 1 ELSE 0 END AS eligible_c3
    FROM add_more_flags_2
"""

US_TN_COMPLIANT_REPORTING_LOGIC_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_COMPLIANT_REPORTING_LOGIC_VIEW_NAME,
    description=US_TN_COMPLIANT_REPORTING_LOGIC_VIEW_DESCRIPTION,
    view_query_template=US_TN_COMPLIANT_REPORTING_LOGIC_QUERY_TEMPLATE,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    base_dataset=STATE_BASE_DATASET,
    static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_COMPLIANT_REPORTING_LOGIC_VIEW_BUILDER.build_and_print()
