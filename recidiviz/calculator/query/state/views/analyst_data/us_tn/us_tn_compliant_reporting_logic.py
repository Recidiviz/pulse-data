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
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_COMPLIANT_REPORTING_LOGIC_VIEW_NAME = "us_tn_compliant_reporting_logic"

US_TN_COMPLIANT_REPORTING_LOGIC_VIEW_DESCRIPTION = (
    """Creates a view to identify individuals eligible for Compliant Reporting"""
)

US_TN_COMPLIANT_REPORTING_LOGIC_QUERY_TEMPLATE = """
    -- This CTE calculates various supervision and system session dates
    WITH supervision_sessions_in_system AS (
        SELECT  ss.person_id,
                ss.start_date as latest_system_session_start_date,
                CASE WHEN ROW_NUMBER() OVER(PARTITION BY ss.person_id ORDER BY cs.start_date ASC) = 1 THEN cs.start_date END AS earliest_supervision_start_date,
                CASE WHEN ROW_NUMBER() OVER(PARTITION BY ss.person_id ORDER BY cs.start_date DESC) = 1 THEN cs.start_date END AS latest_supervision_start_date,
        FROM (
            SELECT *
            FROM `{project_id}.{sessions_dataset}.system_sessions_materialized`
            WHERE TRUE 
            QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY session_id_end DESC) = 1 
        ) ss
        -- Here, we're not restricting supervision super sessions to be within a system session, but since we're
        -- restricting to keeping someone's latest system session, that will happen by definition
        LEFT JOIN `{project_id}.{sessions_dataset}.compartment_level_0_super_sessions_materialized` cs
            ON ss.person_id = cs.person_id
            AND cs.start_date >= ss.start_date
            AND cs.compartment_level_0 = 'SUPERVISION'
    ), 
    calc_dates AS (
        SELECT  person_id,
                -- Since we start with everyone actively on supervision, everyone's "most recent system session" will also be 
                -- their current system session
                any_value(latest_system_session_start_date) AS latest_system_session_start_date,
                max(earliest_supervision_start_date) AS earliest_supervision_start_date_in_latest_system,
                max(latest_supervision_start_date) AS latest_supervision_start_date,
        FROM supervision_sessions_in_system
        GROUP BY 1
    ),
    -- This CTE pulls out important information about current and past supervision levels which is used to determine 
    -- eligible supervision level and time on supervision level
    sup_levels AS (
        SELECT * 
        FROM (
            SELECT sl.person_id,
                start_date,
                end_date,
                sl.supervision_level,
                supervision_level_raw_text,
                previous_supervision_level,
                previous_supervision_level_raw_text,
                LAG(start_date) OVER(PARTITION BY sl.person_id ORDER BY start_date) AS previous_start_date,
                MAX(CASE WHEN sl.supervision_level IN ("IN_CUSTODY", "MAXIMUM", "HIGH", "MEDIUM") THEN start_date END) OVER(PARTITION BY sl.person_id) AS latest_start_higher_sup_level,
            FROM `{project_id}.{sessions_dataset}.supervision_level_raw_text_sessions_materialized` sl
            WHERE sl.state_code = 'US_TN'
        )
        WHERE end_date IS NULL
    ),
    -- This CTE uses standards list and creates flags to apply supervision level and time spent criteria, as well as bringing various supervision and system session dates
    standards AS (
        SELECT standards.* EXCEPT(Offender_ID),
                pei.person_id,
                latest_system_session_start_date,
                earliest_supervision_start_date_in_latest_system,
                latest_supervision_start_date,
                sup_levels.start_date AS current_supervision_level_start,
                sup_levels.previous_start_date AS previous_supervision_level_start,
                LPAD(CAST(Offender_ID AS string), 8, '0') AS Offender_ID,
                -- Criteria: Currently on Minimum/Medium
                # TODO(#12606): Switch to using only ingested supervision level data when Workflows operated from prod
                -- Due to data lags, our ingested supervision level is sometimes not accurate compared to standards sheet, so using the latter
                CASE WHEN ( standards.Supervision_Level LIKE '%MINIMUM%'
                            AND sup_levels.supervision_level = 'MINIMUM'
                          )
                            OR 
                          ( standards.Supervision_Level LIKE ('%MEDIUM%') 
                            AND sup_levels.supervision_level = 'MEDIUM'
                            AND supervision_level_raw_text != '6P3'
                            )  
                    THEN 1 
                    ELSE 0 END AS eligible_supervision_level,
                -- Criteria: Medium or Less for 18+ months
                CASE WHEN (
                            standards.Supervision_Level LIKE ('%MINIMUM%') 
                            AND sup_levels.supervision_level = 'MINIMUM'
                            AND DATE_DIFF(current_date('US/Eastern'),sup_levels.start_date,MONTH)>=12
                            )
                        OR (
                            standards.Supervision_Level LIKE ('%MEDIUM%') 
                            AND sup_levels.supervision_level = 'MEDIUM'
                            AND supervision_level_raw_text != '6P3' 
                            AND DATE_DIFF(current_date('US/Eastern'),sup_levels.start_date,MONTH)>=18
                             )
                        THEN 1
                    WHEN standards.Supervision_Level LIKE ('%MINIMUM%')
                        AND sup_levels.supervision_level = 'MINIMUM'
                        AND previous_supervision_level IN ('MEDIUM') 
                        AND previous_supervision_level_raw_text != '6P3' 
                        AND DATE_DIFF(current_date('US/Eastern'),sup_levels.previous_start_date,MONTH)>=18 
                        THEN 2
                     WHEN standards.Supervision_Level LIKE ('%MEDIUM%')
                        AND sup_levels.supervision_level = 'MEDIUM' 
                        AND supervision_level_raw_text != '6P3'
                        AND previous_supervision_level IN ('MINIMUM') 
                        AND DATE_DIFF(current_date('US/Eastern'),sup_levels.previous_start_date,MONTH)>=18 
                        THEN 2
                    ELSE 0 END AS time_on_level_flag,
                CASE WHEN standards.Supervision_Level LIKE "%MINIMUM%" THEN DATE_ADD(sup_levels.start_date, INTERVAL 12 MONTH)
                     WHEN standards.Supervision_Level LIKE "%MEDIUM%" THEN DATE_ADD(sup_levels.start_date, INTERVAL 18 MONTH)
                    ELSE '9999-01-01' END AS date_sup_level_eligible,
                -- Criteria: No Arrests in past year
                -- Implemented as "no ARRP in past year" which could be true even if Last_ARR_note is over 12 months old and the last check *was* positive. 
                -- However, VERY few people have arrest checks that are more than 12 months old, so effectively this means we're just checking that the last ARR check is negative
                -- If someone had a positive arrest check in the past year, but their last one is negative, we're still considering them eligible
                -- We don't have full arrest history so we can't check this another way
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
            FROM `{project_id}.{static_reference_dataset}.us_tn_standards_due` standards
            LEFT JOIN `{project_id}.{base_dataset}.state_person_external_id` pei
                ON pei.external_id = LPAD(CAST(Offender_ID AS string), 8, '0')
            LEFT JOIN calc_dates
                ON pei.person_id = calc_dates.person_id
            LEFT JOIN sup_levels
                ON pei.person_id = sup_levels.person_id
            WHERE date_of_standards = (
                SELECT MAX(date_of_standards)
                FROM `{project_id}.{static_reference_dataset}.us_tn_standards_due`
            )
    ), 
    -- Joins together sentences and standards sheet
    sentences_join AS (
        SELECT  standards.*, 
                sentence_put_together.* EXCEPT(Offender_ID),
                sl.previous_supervision_level,
                sl.supervision_level AS supervision_level_internal,
                latest_start_higher_sup_level,
                CASE WHEN sentence_put_together.Offender_ID IS NULL THEN 1 ELSE 0 END AS missing_sent_info,
                -- Criteria: For people on ISC (interstate compact) who are currently on minimum, were assigned to minimum right after supervision intake
                -- (unassigned supervision level) and haven't had a higher supervision level during this system session, we mark them as automatically
                -- eligible for compliant reporting
                # TODO(#12606): Switch to using only ingested supervision level data when Workflows operated from prod
                -- Due to data lags, our ingested supervision level is sometimes not accurate compared to standards sheet, so using the latter
                CASE WHEN sl.previous_supervision_level = 'UNASSIGNED'
                        AND standards.Supervision_Level LIKE ('%MINIMUM%')
                        AND sl.supervision_level = 'MINIMUM'
                        AND COALESCE(latest_start_higher_sup_level,'1900-01-01') < latest_system_session_start_date 
                        THEN 'Eligible'
                    -- For people where the previous supervision level is not Intake (Unassigned) but could be Limited (Compliant Reporting), Unknown, Null, etc
                    -- but who are on minimum without a higher level this system session, we flag them as eligible pending review
                     WHEN COALESCE(sl.previous_supervision_level,'MISSING') != 'UNASSIGNED'
                        AND standards.Supervision_Level LIKE ('%MINIMUM%') 
                        AND sl.supervision_level = 'MINIMUM'
                        AND COALESCE(latest_start_higher_sup_level,'1900-01-01') < latest_system_session_start_date 
                        THEN 'Needs Review'
                    END AS no_sup_level_higher_than_mininmum
        FROM standards
        LEFT JOIN `{project_id}.{analyst_dataset}.us_tn_sentence_logic_materialized` sentence_put_together
            USING(Offender_ID)
        LEFT JOIN sup_levels sl
            ON sl.person_id = standards.person_id
    ),
    -- This CTE pulls information on drug contacts to understand who satisfies the criteria for drug screens
    contacts_cte AS (
        SELECT OffenderID AS Offender_ID, 
                CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) AS contact_date, 
                ContactNoteType,
                CASE WHEN ContactNoteType IN ('DRUN','DRUX','DRUM') THEN 1 ELSE 0 END AS negative_drug_test,
                1 AS drug_screen,
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.ContactNoteType_latest`
        -- Limit to DRU% (DRUN,DRUM, etc) type contacts in the last 12 months 
        -- While the policy technically states the test must have been passed within the last 12 months, increasing the lookback
        -- window allows us to include people on the margin of eligibility who may have received a negative drug screen
        -- just before the 12 month cutoff
        WHERE DATE_DIFF(CURRENT_DATE,CAST(CAST(ContactNoteDateTime AS datetime) AS DATE),DAY) <= 365
        AND ContactNoteType LIKE '%DRU%'
    ),
    -- This CTE calculates total drug screens and total negative screens in the past year
    dru_contacts AS (
        SELECT Offender_ID, 
                sum(drug_screen) AS total_screens_in_past_year,
                sum(negative_drug_test) as sum_negative_tests_in_past_year, 
                max(is_most_recent_test_negative) as is_most_recent_test_negative,
                max(most_recent_test_date) as most_recent_test_date,
                max(most_recent_positive_test_date) as most_recent_positive_test_date,
                
        FROM (
            SELECT *,
                -- Since we've already limited to just drug screen contacts, this looks at whether the latest screen was a negative one 
                CASE WHEN ROW_NUMBER() OVER(PARTITION BY Offender_ID ORDER BY contact_date DESC) = 1
                        AND ContactNoteType IN ('DRUN','DRUX','DRUM') THEN 1 
                     WHEN ROW_NUMBER() OVER(PARTITION BY Offender_ID ORDER BY contact_date DESC) = 1
                        AND ContactNoteType NOT IN ('DRUN','DRUX','DRUM') THEN 0                     
                    END AS is_most_recent_test_negative,
                -- Most recent test date
                CASE WHEN ROW_NUMBER() OVER(PARTITION BY Offender_ID ORDER BY contact_date DESC) = 1
                     THEN contact_date 
                     END AS most_recent_test_date,
                -- Most recent positive test date
                CASE WHEN ContactNoteType NOT IN ('DRUN','DRUX','DRUM') 
                    AND ROW_NUMBER() OVER(PARTITION BY Offender_ID
                                            ORDER BY CASE WHEN ContactNoteType NOT IN ('DRUN','DRUX','DRUM') THEN 0 ELSE 1 END ASC, contact_date DESC) = 1
                    THEN contact_date
                    END AS most_recent_positive_test_date
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
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.ContactNoteType_latest`
        WHERE ContactNoteType IN ('DRUN','DRUX','DRUM')
        GROUP BY 1
    ),
    -- This CTE excludes people who have a flag indicating Lifetime Supervision or Life Sentence - both are ineligible for CR or overdue
    CSL AS (
        SELECT OffenderID as Offender_ID, LifetimeSupervision, LifeDeathHabitual 
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.JOSentence_latest`
        WHERE TRUE
        QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderID ORDER BY CASE WHEN LifetimeSupervision = 'Y' THEN 0 ELSE 1 END,
                                                                    CASE WHEN LifeDeathHabitual IS NOT NULL THEN 0 ELSE 1 END) = 1
    ),
    life_sentence_ISC AS (
        SELECT DISTINCT OffenderID AS Offender_ID,
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.ISCSentence_latest`
        WHERE sentence LIKE '%LIFE%'
    ),
    -- This CTE excludes people who have been rejected from CR because of criminal record or court order
    CR_rejection_codes AS (
        SELECT DISTINCT OffenderID as Offender_ID
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.ContactNoteType_latest`
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
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.ContactNoteType_latest`
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
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.ContactNoteType_latest`
        WHERE ContactNoteType IN ('DECF', 'DEDF','DEDU','DEIO','DEIR')
        GROUP BY 1
    ),
    -- serious sanctions criteria
    sanctions AS (
        SELECT DISTINCT OffenderID as Offender_ID,
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.Violations_latest`
        JOIN `{project_id}.{us_tn_raw_data_up_to_date_dataset}.Sanctions_latest` 
            USING(TriggerNumber)
        WHERE CAST(SanctionLevel AS INT) > 1
            AND CAST(CAST(ProposedDate AS datetime) AS DATE) >= DATE_SUB(CURRENT_DATE('US/Eastern'),INTERVAL 12 MONTH)
    ),
    sanctions_array AS (
        SELECT OffenderID as Offender_ID, ARRAY_AGG(ProposedSanction IGNORE NULLS) AS sanctions_in_last_year
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.Violations_latest`
        JOIN `{project_id}.{us_tn_raw_data_up_to_date_dataset}.Sanctions_latest` 
            USING(TriggerNumber)
        WHERE CAST(CAST(ProposedDate AS datetime) AS DATE) >= DATE_SUB(CURRENT_DATE('US/Eastern'),INTERVAL 12 MONTH)
        GROUP BY 1
    ),
    sanction_eligible_date AS (
        SELECT OffenderID as Offender_ID, 
              MAX(CASE WHEN CAST(SanctionLevel AS INT) > 1 THEN DATE_ADD(CAST(CAST(ProposedDate AS datetime) AS DATE), INTERVAL 12 MONTH)
                  ELSE '1900-01-01' END) AS date_sanction_eligible
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.Violations_latest`
        JOIN `{project_id}.{us_tn_raw_data_up_to_date_dataset}.Sanctions_latest` 
            USING(TriggerNumber)
        GROUP BY 1
    ),
    -- we conservatively use these codes 
    zero_tolerance_codes AS (
        SELECT OffenderID as Offender_ID, 
                ARRAY_AGG(STRUCT(ContactNoteType,CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) AS contact_date)) AS zt_codes,
                MAX(CAST(CAST(ContactNoteDateTime AS datetime) AS DATE)) AS latest_zero_tolerance_sanction_date
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.ContactNoteType_latest`
        WHERE ContactNoteType IN ('VWAR','PWAR','ZTVR','COHC')
        GROUP BY OffenderID
    ),
    person_status_cte AS (
            SELECT OffenderID as Offender_ID, OffenderStatus AS person_status
            FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.OffenderName_latest`
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
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.OffenderExemptions_latest`
    ),
    inv AS (
        SELECT * EXCEPT(InvoiceDate, UnPaidAmount, InvoiceAmount),
                        CAST(SPLIT(InvoiceDate,' ')[OFFSET(0)] AS DATE) as InvoiceDate,
                        ROUND(CAST(UnPaidAmount AS FLOAT64)) AS UnPaidAmount,
                        CAST(InvoiceAmount AS FLOAT64 ) AS InvoiceAmount,
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.OffenderInvoices_latest`
    ),
    -- Returns all accounts with permanent exemption types
    permanent_exemptions AS (
        SELECT DISTINCT exemptions.AccountSAK
              FROM exemptions 
              WHERE ReasonCode IN ('SSDB','JORD','CORD','SISS') 
              AND EndDate IS NULL              
        ),
    current_exemptions AS (
        SELECT AccountSAK, 
                ARRAY_AGG(ReasonCode IGNORE NULLS) AS current_exemption_type,
                MAX(CASE WHEN ReasonCode IN ('SSDB','JORD','CORD','SISS') THEN 1 ELSE 0 END) AS has_permanent_exemption
              FROM exemptions 
              WHERE EndDate IS NULL
        GROUP BY 1
        ),
    -- These CTEs calculate unpaid total.The first CTE calculates it for the most recent system session, the second calculates
    -- for all. First, we join on any exemption when invoice is greater than exemption amount
    -- (since sometimes a person can be billed $0 for an item that they have an exemption for), and join on the same FeeItemID and for
    -- any invoices during an exemption. Next, there are various edge cases where paid amount (invoice minus unpaid) is greater
    -- than actual amount owed (invoice minus exempt). When that happens, actual unpaid balance is set to the difference between unpaid and exempt
    -- Otherwise, we take the UnpaidAmount and sum it 
    arrearages_latest AS (
            SELECT standards.Offender_ID, 
                    accounts.AccountSAK, 
                    SUM(
                        CASE WHEN COALESCE(inv_1.InvoiceAmount,0) - COALESCE(inv_1.UnPaidAmount,0) >= COALESCE(inv_1.InvoiceAmount,0) - COALESCE(exemption_1.ExemptAmount,0) THEN COALESCE(inv_1.UnPaidAmount,0) - COALESCE(exemption_1.ExemptAmount,0)
                             ELSE COALESCE(inv_1.UnPaidAmount,0)
                             END
                    ) as unpaid_total_latest,
            FROM standards
            LEFT JOIN `{project_id}.{us_tn_raw_data_up_to_date_dataset}.OffenderAccounts_latest` accounts
                ON standards.Offender_ID = accounts.OffenderID
            LEFT JOIN inv inv_1 
                ON accounts.AccountSAK = inv_1.AccountSAK
                AND inv_1.InvoiceDate >= COALESCE(DATE_TRUNC(latest_supervision_start_date,MONTH),DATE_TRUNC(latest_system_session_start_date,MONTH),'1900-01-01')
            LEFT JOIN exemptions exemption_1
                ON inv_1.AccountSAK = exemption_1.AccountSAK
                AND inv_1.FeeItemID = exemption_1.FeeItemID
                AND inv_1.InvoiceDate BETWEEN exemption_1.StartDate AND COALESCE(exemption_1.EndDate,'9999-01-01')
                AND inv_1.InvoiceAmount >= COALESCE(exemption_1.ExemptAmount,0)
            GROUP BY 1,2
    ),
    arrearages_all AS (
            SELECT standards.Offender_ID, 
                    accounts.AccountSAK, 
                    SUM(
                        CASE WHEN COALESCE(inv_2.InvoiceAmount,0) - COALESCE(inv_2.UnPaidAmount,0) >= COALESCE(inv_2.InvoiceAmount,0) - COALESCE(exemption_2.ExemptAmount,0) THEN COALESCE(inv_2.UnPaidAmount,0) - COALESCE(exemption_2.ExemptAmount,0)
                             ELSE COALESCE(inv_2.UnPaidAmount,0)
                             END
                    ) as unpaid_total_all,
            FROM standards
            LEFT JOIN `{project_id}.{us_tn_raw_data_up_to_date_dataset}.OffenderAccounts_latest` accounts
                ON standards.Offender_ID = accounts.OffenderID
            LEFT JOIN inv inv_2 
                ON accounts.AccountSAK = inv_2.AccountSAK
            LEFT JOIN exemptions exemption_2
                ON inv_2.AccountSAK = exemption_2.AccountSAK
                AND inv_2.FeeItemID = exemption_2.FeeItemID
                AND inv_2.InvoiceDate BETWEEN exemption_2.StartDate AND COALESCE(exemption_2.EndDate,'9999-01-01')
                AND inv_2.InvoiceAmount >= COALESCE(exemption_2.ExemptAmount,0)                
            GROUP BY 1,2
    ),
    arrearages AS (
        SELECT arrearages_latest.*, unpaid_total_all
        FROM arrearages_latest
        JOIN arrearages_all
            USING(Offender_ID, AccountSAK)
    ),
    payments AS (
        SELECT AccountSAK, 
            CAST(SPLIT(PaymentDate,' ')[OFFSET(0)] AS DATE) as PaymentDate,
            CAST(PaidAmount AS FLOAT64) - CAST(UnAppliedAmount AS FLOAT64) AS PaidAmount,
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.OffenderPayments_latest`
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
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.ContactNoteType_latest`
        WHERE ContactNoteType = 'TEPE'
        QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderID ORDER BY ContactNoteDateTime DESC) = 1
    ),
    latest_zzz AS (
        SELECT OffenderID as Offender_ID, 
                CAST(SPLIT(ContactNoteDateTime,' ')[OFFSET(0)] AS DATE) as latest_zzz_date,
                ContactNoteType as latest_zzz_code,
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.ContactNoteType_latest`
        WHERE ContactNoteType IN ('ZZZZ','ZZZC')
        QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderID ORDER BY ContactNoteDateTime DESC) = 1
    ), 
    latest_off_mvmt AS (
        SELECT OffenderID as Offender_ID,
                CAST(SPLIT(LastUpdateDate,' ')[OFFSET(0)] AS DATE) as latest_mvmt_date,
                MovementType as latest_mvmt_code,
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.OffenderMovement_latest`
        WHERE MovementType LIKE '%DI%'
        QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderID ORDER BY MovementDateTime DESC) = 1
    ),
    -- Parole board action conditions live in a different data source than special conditions associated with sentences
    -- The BoardAction data is unique on person, hearing date, hearing type, and staff ID. For our purposes, we keep 
    -- conditions where "final decision" is yes, and keep all distinct parole conditions on a given person/day
    -- Then we keep all relevant hearings that happen in someone's latest system session, and keep all codes since then
    board_conditions AS (
        SELECT Offender_ID, hearing_date, condition, Decode AS condition_description
        FROM (
            SELECT DISTINCT OffenderID AS Offender_ID, CAST(HearingDate AS DATE) AS hearing_date, ParoleCondition1, ParoleCondition2, ParoleCondition3, ParoleCondition4, ParoleCondition5
            FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.BoardAction_latest`
            WHERE FinalDecision = 'Y'
        )
        UNPIVOT(condition for c in (ParoleCondition1, ParoleCondition2, ParoleCondition3, ParoleCondition4, ParoleCondition5))
        LEFT JOIN (
            SELECT *
            FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.CodesDescription_latest`
            WHERE CodesTableID = 'TDPD030'
        ) codes 
            ON condition = codes.Code
    ),
    relevant_conditions AS (
        SELECT standards.Offender_ID, ARRAY_AGG(STRUCT(condition, condition_description)) AS board_conditions
        FROM standards
        JOIN board_conditions
            ON standards.Offender_ID = board_conditions.Offender_ID
            AND hearing_date >= COALESCE(latest_system_session_start_date,'1900-01-01')
        GROUP BY 1
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
    -- TN uses assessment scores to determine who is a "drug offender" - anyone who received a Moderate or High assessment for the 
    -- AlcoholDrugNeedLevel. We currently assume this is based on someone's latest assessment 
    assessment AS (    
        SELECT *
        FROM (
            SELECT person_id,
                    assessment_date, 
                    MAX(CASE WHEN COALESCE(REPLACE(JSON_EXTRACT(assessment_metadata, "$.ALCOHOL_DRUG_NEED_LEVEL"), '"',''), 'MISSING') IN ('MOD','HIGH') THEN 1 ELSE 0 END) AS high_ad_client
            FROM `{project_id}.{base_dataset}.state_assessment`
            GROUP BY 1,2
        )
        WHERE TRUE
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY assessment_date DESC) = 1
    ),
    add_more_flags_1 AS (
        SELECT sentences_join.* ,
                dru_contacts.* EXCEPT(total_screens_in_past_year,Offender_ID),
                account_flags.*,
                sum_payments.last_paid_amount,
                arrearages.* EXCEPT(Offender_ID),
                zt_codes,
                high_ad_client,
                assessment_date AS latest_assessment_date,
                CASE WHEN time_on_level_flag = 2 THEN previous_supervision_level_start
                     WHEN time_on_level_flag = 1 THEN current_supervision_level_start
                    END AS eligible_level_start,
                CASE WHEN time_on_level_flag > 0 THEN 1 ELSE 0 END AS time_on_level_flag_adj,
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
            CASE WHEN COALESCE(dui_flag,0) = 0 THEN 'Eligible' ELSE 'Ineligible' END as dui_flag_eligibility,
            CASE WHEN COALESCE(dui_last_5_years_flag,0) = 0 THEN 'Eligible' ELSE 'Ineligible' END AS dui_last_5_years_flag_eligibility,
            CASE WHEN GREATEST(COALESCE(homicide_flag,0),COALESCE(homicide_flag_ever,0),COALESCE(homicide_flag_prior,0)) = 1 THEN 'Ineligible' ELSE 'Eligible' END AS homicide_eligibility,                  
            CASE WHEN total_screens_in_past_year IS NULL THEN 0 
                ELSE total_screens_in_past_year END AS total_screens_in_past_year,
        -- Logic here: for non-drug-offense:
            -- They must be 6 months or more past their latest positive test. This will be null for people with no positive test - for those, we assume they meet this condition
            -- They must have at least 1 negative test in the past year 
        CASE WHEN COALESCE(high_ad_client,0) = 0 
            AND DATE_DIFF(CURRENT_DATE('US/Eastern'), COALESCE(most_recent_positive_test_date, '1900-01-01'), DAY) >= 180 
            AND sum_negative_tests_in_past_year >= 1
            -- If someone has had a positive test, even if its more than 6 months old, we enforce that their most recent test is negative
            AND is_most_recent_test_negative = 1        
             THEN 1
            -- These two conditions ensure that selecting this flag doesnt automatically eliminate people missing sentencing info or where drug offense = 1 since there are separate flags for that
             WHEN high_ad_client IS NULL THEN NULL
             WHEN COALESCE(high_ad_client,0) = 1 THEN NULL
             ELSE 0 
             END AS drug_screen_pass_flag_non_drug_offense,
        -- Logic here: for drug offenses
            -- If there are 2 negative tests in the past year, the most recent test is negative, and the most recent positive test (if it exists) is over 12 months old, they're eligible
        CASE WHEN COALESCE(high_ad_client,0) = 1 
            AND sum_negative_tests_in_past_year >= 2 
            AND is_most_recent_test_negative = 1 
            AND DATE_DIFF(CURRENT_DATE('US/Eastern'), COALESCE(most_recent_positive_test_date, '1900-01-01'), DAY) >= 365
            THEN 1
             -- These two conditions ensure that selecting this flag doesnt automatically eliminate people missing sentencing info
             -- or where drug offense = 0 since there are separate flags for that
             WHEN high_ad_client IS NULL THEN NULL
             WHEN COALESCE(high_ad_client,0) = 0 THEN NULL
             ELSE 0 
             END AS drug_screen_pass_flag_drug_offense,
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
        CASE WHEN COALESCE(unpaid_total_latest,0) <= 500 THEN 'low_balance'
             WHEN permanent_exemptions.AccountSAK IS NOT NULL THEN 'exempt'
             WHEN unpaid_total_latest <= 2000 AND have_past_1_month_payment = 1 AND have_past_2_month_payment = 1 AND have_past_3_month_payment = 1 THEN 'regular_payments'
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
        LEFT JOIN arrearages 
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
        LEFT JOIN assessment
            USING(person_id)    
        LEFT JOIN zero_tolerance_codes 
            ON sentences_join.Offender_ID = zero_tolerance_codes.Offender_ID
            AND zero_tolerance_codes.latest_zero_tolerance_sanction_date  > COALESCE(sentence_start_date,'0001-01-01')
    ),
    add_more_flags_2 AS (
        SELECT
            COALESCE(full_name,TO_JSON_STRING(STRUCT(first_name as given_names,"" as middle_names,"" as name_suffix,last_name as surname)))  AS person_name,
            add_more_flags_1.person_id,
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
            latest_supervision_start_date,
            latest_system_session_start_date,
            earliest_supervision_start_date_in_latest_system,
            DATE_DIFF(COALESCE(sentence_expiration_date_internal,EXP_Date),sentence_start_date,DAY) AS sentence_length_days,
            DATE_DIFF(COALESCE(sentence_expiration_date_internal,EXP_Date),sentence_start_date,YEAR) AS sentence_length_years,
            add_more_flags_1.Offender_ID AS person_external_id,
            person_status,
            special_conditions_on_current_sentences,
            COALESCE(unpaid_total_latest,0) AS current_balance,
            COALESCE(unpaid_total_all,0) AS current_balance_all,
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
            supervision_level_internal,
            Supervision_Level AS supervision_level,
            eligible_level_start,
            Plan_Start_Date AS supervision_level_start,
            current_supervision_level_start AS supervision_level_start_internal,
            no_sup_level_higher_than_mininmum,
            previous_supervision_level,
            latest_start_higher_sup_level,
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
            most_recent_positive_test_date,
            sanctions_in_last_year,
            board_conditions,
            arrests_past_year AS last_arr_check_past_year,
            zt_codes,
            Last_Sanctions_Type AS last_sanction,
            Last_DRU_Note AS last_drug_screen_date,
            Last_DRU_Type AS last_drug_screen_result,
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
            homicide_eligibility,
            dui_flag_eligibility,
            dui_last_5_years_flag_eligibility,
            drug_screen_pass_flag_drug_offense,
            drug_screen_pass_flag_non_drug_offense,
            COALESCE(drug_offense,drug_offense_ever) AS drug_offense,
            high_ad_client,
            latest_assessment_date,
            sum_negative_tests_in_past_year,
            is_most_recent_test_negative,
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
                        AND dui_flag_eligibility = 'Eligible'
                        AND homicide_eligibility = 'Eligible'
                        AND dui_last_5_years_flag_eligibility = 'Eligible'
                        AND latest_expiration_date_for_excluded_offenses IS NOT NULL THEN DATE_ADD(latest_expiration_date_for_excluded_offenses, INTERVAL 10 YEAR)
                WHEN domestic_flag_eligibility in ('Eligible','Discretion')
                        AND sex_offense_flag_eligibility in ('Eligible','Discretion')
                        AND assaultive_offense_flag_eligibility in ('Eligible','Discretion')
                        AND maybe_assaultive_flag_eligibility in ('Eligible','Discretion')
                        AND unknown_offense_flag_eligibility in ('Eligible','Discretion')
                        AND missing_offense_flag_eligibility in ('Eligible','Discretion')
                        AND young_victim_flag_eligibility in ('Eligible','Discretion')
                        AND homicide_eligibility = 'Eligible'
                        AND dui_flag_eligibility = 'Eligible'
                        AND dui_last_5_years_flag_eligibility = 'Eligible' THEN '1900-01-01'
                END AS date_offenses_eligible,
            date_sup_level_eligible,
            date_arrest_check_eligible,
            COALESCE(date_sanction_eligible,'1900-01-01') AS date_sanction_eligible,
            COALESCE(Last_SPE_Note,'1900-01-01') AS date_spe_eligible,
            CASE WHEN COALESCE(drug_offense,drug_offense_ever) = 0 AND earliest_DRUN_date >= latest_supervision_start_date THEN earliest_DRUN_date
                 WHEN COALESCE(drug_offense,drug_offense_ever) = 0 AND earliest_DRUN_date < COALESCE(latest_supervision_start_date, Plan_Start_Date) THEN COALESCE(Last_DRU_Note,'1900-01-01')
                  WHEN COALESCE(drug_offense,drug_offense_ever) = 0 AND earliest_DRUN_date IS NULL THEN '1900-01-01'
                  WHEN COALESCE(drug_offense,drug_offense_ever) = 1 and sum_negative_tests_in_past_year >= 2 and is_most_recent_test_negative = 1 THEN Last_DRU_Note
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
                AND dui_flag_eligibility in ('Eligible')
                AND dui_last_5_years_flag_eligibility in ('Eligible','Eligible - Expired','Discretion')
                AND homicide_eligibility = 'Eligible'
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
                AND dui_flag_eligibility = 'Eligible'
                AND dui_last_5_years_flag_eligibility = 'Eligible'
                AND homicide_eligibility = 'Eligible'
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
                AND fines_fees_eligible not in ('Ineligible')
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
                AND dui_flag_eligibility = 'Eligible'
                AND dui_last_5_years_flag_eligibility = 'Eligible'
                AND homicide_eligibility = 'Eligible'
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
                AND fines_fees_eligible not in ('Ineligible')
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
                AND dui_flag_eligibility = 'Eligible'
                AND dui_last_5_years_flag_eligibility = 'Eligible'
                AND homicide_eligibility = 'Eligible'
                -- These flags can be null if sentencing info is missing or if drug_offense = 1 or drug_offense = 0, respectively
                AND COALESCE(drug_screen_pass_flag_non_drug_offense,1) = 1
                AND COALESCE(drug_screen_pass_flag_drug_offense,1) = 1
                -- Since this flag is for offense type discretion anyway, ok to surface people missing sentence info
                # AND missing_sent_info = 0
                AND cr_not_previous_rejected_flag = 1
                AND cr_not_rejected_x_months_flag = 1
                AND no_lifetime_flag = 1
                AND eligible_counties = 1
                AND fines_fees_eligible not in ('Ineligible') 
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
                AND dui_flag_eligibility = 'Eligible'
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
                AND homicide_eligibility = 'Eligible'
            THEN 1 ELSE 0 END AS all_eligible_and_offense_and_zt_and_fines_discretion,
        FROM add_more_flags_1    
        LEFT JOIN contacts_cte_arrays 
            USING(Offender_ID)
        LEFT JOIN relevant_conditions
            USING(Offender_ID)
        LEFT JOIN sanctions_array 
            USING(Offender_ID)
        LEFT JOIN sanction_eligible_date
            USING(Offender_ID)
        LEFT JOIN drun_contacts
            USING(Offender_ID)
        LEFT JOIN rejection_codes_max_date
            USING(Offender_ID)
        LEFT JOIN special_conditions
            ON add_more_flags_1.person_id = special_conditions.person_id
        LEFT JOIN `{project_id}.{base_dataset}.state_person` sp
            ON sp.person_id = add_more_flags_1.person_id
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
        -- These fields are not currently being used in analysis or determining who is eligible - they were created to
        -- determine when someone did or will be eligible, but the plan is to answer those questions going forward using
         -- a sessionized view like compliant_reporting_sessions
        GREATEST(date_offenses_eligible,
                date_sup_level_eligible,
                date_arrest_check_eligible,
                date_sanction_eligible,
                date_spe_eligible,
                date_drug_screen_eligible,
                date_cr_rejection_eligible) AS greatest_date_eligible,
        CASE 
             -- People on ICOTS and minimum with no active TN sentences should automatically be on compliant reporting
             WHEN supervision_type LIKE '%ISC%' 
                AND supervision_level LIKE '%MINIMUM%' 
                AND has_TN_sentence = 0 
                AND no_lifetime_flag = 1 
                AND no_sup_level_higher_than_mininmum in ('Eligible')
                THEN 'c4'
             WHEN all_eligible = 1 
                AND overdue_for_discharge_no_case_closure = 0 
                AND overdue_for_discharge_within_90 = 0 
                THEN 'c1'
             WHEN all_eligible_and_offense_discretion = 1 
                AND overdue_for_discharge_no_case_closure = 0 
                AND overdue_for_discharge_within_90 = 0 
                THEN 'c2'
             WHEN all_eligible_and_offense_and_zt_discretion = 1 
                AND overdue_for_discharge_no_case_closure = 0 
                AND overdue_for_discharge_within_90 = 0 
                THEN 'c3' 
             END AS compliant_reporting_eligible,
        
        CASE WHEN supervision_type LIKE '%ISC%' 
            AND supervision_level LIKE '%MINIMUM%' 
            AND has_TN_sentence = 0 
            AND no_lifetime_flag = 1
            AND no_sup_level_higher_than_mininmum in ('Eligible') 
            THEN 1 
        ELSE 0 END AS eligible_c4,
        
        CASE WHEN supervision_type LIKE '%ISC%' 
            AND supervision_level LIKE '%MINIMUM%' 
            AND has_TN_sentence = 0 
            AND no_lifetime_flag = 1
            AND no_sup_level_higher_than_mininmum in ('Needs Review') 
            THEN 1 
        ELSE 0 END AS eligible_c4_review,
        
        CASE WHEN all_eligible = 1 
            AND overdue_for_discharge_no_case_closure = 0 
            AND overdue_for_discharge_within_90 = 0 
            THEN 1 
        ELSE 0 END AS eligible_c1,
        CASE WHEN all_eligible_and_offense_discretion = 1 
            AND overdue_for_discharge_no_case_closure = 0 
            AND overdue_for_discharge_within_90 = 0 
            THEN 1 
        ELSE 0 END AS eligible_c2,
        CASE WHEN all_eligible_and_offense_and_zt_discretion = 1 
            AND overdue_for_discharge_no_case_closure = 0 
            AND overdue_for_discharge_within_90 = 0 
            THEN 1 
        ELSE 0 END AS eligible_c3
    FROM add_more_flags_2
"""

US_TN_COMPLIANT_REPORTING_LOGIC_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    base_dataset=STATE_BASE_DATASET,
    view_id=US_TN_COMPLIANT_REPORTING_LOGIC_VIEW_NAME,
    description=US_TN_COMPLIANT_REPORTING_LOGIC_VIEW_DESCRIPTION,
    view_query_template=US_TN_COMPLIANT_REPORTING_LOGIC_QUERY_TEMPLATE,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        StateCode.US_TN.value
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_COMPLIANT_REPORTING_LOGIC_VIEW_BUILDER.build_and_print()
