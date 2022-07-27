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
            WHERE state_code = 'US_TN' 
            QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY session_id_end DESC) = 1 
        ) ss
        -- Here, we're not restricting supervision super sessions to be within a system session, but since we're
        -- restricting to keeping someone's latest system session, that will happen by definition
        LEFT JOIN `{project_id}.{sessions_dataset}.compartment_level_0_super_sessions_materialized` cs
            ON ss.person_id = cs.person_id
            AND cs.start_date >= ss.start_date
            AND cs.compartment_level_0 = 'SUPERVISION'
            AND cs.state_code = 'US_TN'
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
        SELECT  DISTINCT
                standards.first_name,
                standards.last_name,
                standards.Phone_Number AS phone_number,
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
                standards.Last_ARR_Note,
                standards.Last_FEE_Note,
                standards.FEE_Note_Due,
                CASE WHEN standards.SPE_Note_Due IS NULL AND standards.Last_SPE_Note IS NULL AND standards.Last_SPET_Date IS NULL THEN 'none'
                     WHEN standards.SPE_Note_Due IS NULL AND standards.Last_SPE_Note IS NOT NULL AND standards.Last_SPET_Date IS NOT NULL THEN 'terminated'
                     WHEN standards.SPE_Note_Due > CURRENT_DATE('US/Eastern') THEN 'current'
                     END AS spe_flag,
                standards.SPE_Note_Due AS  SPE_note_due,
                standards.Last_SPE_Note AS last_SPE_note, 
                standards.Last_SPET_Date AS last_SPET_date,
                standards.EXP_Date AS sentence_expiration_date,
                standards.Staff_ID AS officer_id,
                standards.Staff_First_Name AS staff_first_name,
                standards.Staff_Last_Name AS staff_last_name,
                standards.Case_Type AS supervision_type,
                standards.Supervision_Level AS supervision_level,
                standards.Plan_Start_Date AS supervision_level_start,
                standards.Last_Sanctions_Type AS last_sanction,
                standards.Last_DRU_Note AS last_drug_screen_date,
                standards.Last_DRU_Type AS last_drug_screen_result,
                standards.Region as district,
                -- Criteria: Special Conditions are up to date
                CASE WHEN SPE_Note_Due <= current_date('US/Eastern') THEN 0 ELSE 1 END AS spe_conditions_not_overdue_flag,
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
                /* 
                    This flag determines "how" someone becomes eligible for the time on supervision level criteria. The options are
                    1. 'minimum' if being on minimum for 12 months is the earliest someone becomes eligible
                    2. 'medium' if being on medium for 18 months is the earliest someone becomes eligible
                    3. 'medium or less' if moving from medium -> min or min -> medium and collectively spending 18 months is how someone becomes eligible
                    This field is then used to calculate eligible_level_start (the time from which we should count how long someone has been on the 
                    qualifying level) and date_supervision_level_eligible which determines the date when someone becomes eligible for this criteria
                    
                    Logic:
                    if currently on minimum:
                        - if previously medium:
                            - if previous + 18 < current + 12 then 'medium or less'
                    else: 'minimum'
                    
                    if currently on medium:
                        - if previously minimum: "medium or less"
                    else: "medium"
                */ 
                # TODO(#12606): Switch to using only ingested supervision level data when Workflows operated from prod
                -- Due to data lags, our ingested supervision level is sometimes not accurate compared to standards sheet, so using the latter
                CASE WHEN ( standards.Supervision_Level LIKE ('%MINIMUM%') 
                            AND sup_levels.supervision_level = 'MINIMUM'
                            ) 
                    THEN (
                            CASE 
                                WHEN DATE_DIFF(current_date('US/Eastern'),sup_levels.start_date,MONTH)>=12 THEN 'minimum'
                                WHEN ( previous_supervision_level IN ('MEDIUM') 
                                        AND previous_supervision_level_raw_text != '6P3'
                                    )
                                AND DATE_ADD(sup_levels.previous_start_date, INTERVAL 18 MONTH) < DATE_ADD(sup_levels.start_date, INTERVAL 12 MONTH) THEN 'medium_or_less'
                            ELSE 'minimum'
                            END
                            )
                    WHEN ( standards.Supervision_Level LIKE ('%MEDIUM%') 
                            AND sup_levels.supervision_level = 'MEDIUM'
                            AND supervision_level_raw_text != '6P3' 
                            ) 
                    THEN (
                            CASE 
                                WHEN DATE_DIFF(current_date('US/Eastern'), sup_levels.start_date,MONTH)>=18 THEN 'medium'
                                WHEN previous_supervision_level IN ('MINIMUM') THEN 'medium_or_less'
                            ELSE 'medium'
                            END
                    )
                    END AS sup_level_eligibility_type,
                /*
                 Criteria: No Arrests in past year
                     Implemented as "no ARRP in past year" which could be true even if Last_ARR_note is over 12 months old and the last check *was* positive. 
                     However, VERY few people have arrest checks that are more than 12 months old, so effectively this means we're just checking that the last ARR check is negative
                     If someone had a positive arrest check in the past year, but their last one is negative, we're still considering them eligible
                     We don't have full arrest history so we can't check this another way
                */
                CASE WHEN Last_ARR_Type = 'ARRP' AND DATE_DIFF(current_date('US/Eastern'),Last_ARR_Note,MONTH)<12 THEN 0
                     ELSE 1 END AS eligible_arrests_flag,
                CASE WHEN DATE_DIFF(current_date('US/Eastern'),Last_ARR_Note,MONTH)<12 THEN Last_ARR_Type
                    ELSE NULL END AS arrests_past_year,
            FROM `{project_id}.{static_reference_dataset}.us_tn_standards_due` standards
            LEFT JOIN `{project_id}.{base_dataset}.state_person_external_id` pei
                ON pei.external_id = LPAD(CAST(Offender_ID AS string), 8, '0')
                AND pei.state_code = 'US_TN'
            LEFT JOIN calc_dates
                ON pei.person_id = calc_dates.person_id
            LEFT JOIN sup_levels
                ON pei.person_id = sup_levels.person_id
            WHERE date_of_standards = (
                SELECT MAX(date_of_standards)
                FROM `{project_id}.{static_reference_dataset}.us_tn_standards_due`
            )
    ), 
    -- This CTE uses flags from the previous CTE to determine when people become eligible for being on the right supervision level, and also determine C4 eligibility (ISC + minimum people)
    determine_sup_level_eligibility AS (
         SELECT standards.*,
            CASE 
                WHEN sup_level_eligibility_type = 'medium_or_less' THEN previous_supervision_level_start
                ELSE current_supervision_level_start
                END AS eligible_level_start,
            CASE 
                WHEN sup_level_eligibility_type = 'minimum' THEN DATE_ADD(current_supervision_level_start, INTERVAL 12 MONTH)
                WHEN sup_level_eligibility_type = 'medium' THEN DATE_ADD(current_supervision_level_start, INTERVAL 18 MONTH)
                WHEN sup_level_eligibility_type = 'medium_or_less' THEN DATE_ADD(previous_supervision_level_start, INTERVAL 18 MONTH)
            END as date_supervision_level_eligible,
            /*
             Criteria: For people on ISC (interstate compact) who are currently on minimum, were assigned to minimum right after supervision intake
             (unassigned supervision level) and haven't had a higher supervision level during this system session, we mark them as automatically
             eligible for compliant reporting
            */
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
                END AS no_sup_level_higher_than_mininmum,
                sl.previous_supervision_level,
                sl.supervision_level AS supervision_level_internal,
                latest_start_higher_sup_level
        FROM standards
        LEFT JOIN sup_levels sl
            ON sl.person_id = standards.person_id
    ),
    -- Joins together sentences and standards sheet
    sentences_join AS (
        SELECT  determine_sup_level_eligibility.*, 
                sentence_put_together.* EXCEPT(Offender_ID),
                CASE WHEN sentence_put_together.Offender_ID IS NULL THEN 1 ELSE 0 END AS missing_sent_info,
        FROM determine_sup_level_eligibility
        LEFT JOIN `{project_id}.{analyst_dataset}.us_tn_sentence_logic_materialized` sentence_put_together
            USING(Offender_ID)
    ),
    -- This CTE looks at anyone who has ever received a ZTPD contact (zero tolerance contact for failing a meth test) so they can be excluded from almost-eligible
    -- TODO(#13587) - reference state-agnostic view when #13587 is closed
    meth_contacts AS (
        SELECT DISTINCT OffenderID AS Offender_ID
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.ContactNoteType_latest`
        WHERE ContactNoteType LIKE '%ZTPD%'
    ),
    -- This CTE pulls information on drug contacts to understand who satisfies the criteria for drug screens
    contacts_cte AS (
        SELECT OffenderID AS Offender_ID, 
                CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) AS contact_date, 
                ContactNoteType,
                CASE WHEN ContactNoteType IN ('DRUN','DRUX','DRUM') THEN 1 ELSE 0 END AS negative_drug_test,
                1 AS drug_screen,
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.ContactNoteType_latest`
        /*
         Limit to DRU% (DRUN,DRUM, etc) type contacts in the last 12 months 
        */
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
    /* 
        This CTE excludes people who have been rejected from CR for other reasons, that are not static
        For example, people can be denied for failure to pay fines/fees, for having compliance problems, etc
        But those things could change over time, so perhaps we will exclude people if they have any of these rejection
        Codes in the last X months, but not older than that
        Rejection codes not included:
            - DECT: DENIED COMPLIANT REPORTING, INSUFFICENT TIME IN SUPERVISON LEVEL - since we're checking for that
    */
    CR_rejection_codes_dynamic AS (
        SELECT OffenderID as Offender_ID,
                ARRAY_AGG(CASE WHEN contact_date >= DATE_SUB(current_date('US/Eastern'), INTERVAL 3 MONTH)
                      AND ContactNoteType IN 
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
                      THEN ContactNoteType
                      END
                      IGNORE NULLS
                  ) AS cr_rejections_past_3_months
        FROM (
          SELECT *, 
            CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) as contact_date
          FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.ContactNoteType_latest`
        )
        GROUP BY 1
    ),
    -- serious sanctions criteria
    sanctions AS (
        SELECT *
        FROM (
          SELECT OffenderID as Offender_ID,
                        MAX(CASE WHEN sanction_level > 1 THEN proposed_date END) AS latest_serious_sanction_date,
                        ARRAY_AGG(CASE WHEN proposed_date >= DATE_SUB(CURRENT_DATE('US/Eastern'),INTERVAL 12 MONTH) THEN 
                                  STRUCT(proposed_date, ProposedSanction) END IGNORE NULLS) AS sanctions_in_last_year,
                FROM 
                (
                  SELECT *,
                      CAST(CAST(ProposedDate AS datetime) AS DATE) AS proposed_date,
                      CAST(SanctionLevel AS INT) AS sanction_level
                    FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.Violations_latest`
                    JOIN `{project_id}.{us_tn_raw_data_up_to_date_dataset}.Sanctions_latest` 
                    USING(TriggerNumber)
                )
                GROUP BY 1
        )
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
    /*
     These CTEs calculate unpaid total.The first CTE calculates it for the most recent system session, the second calculates
     for all. First, we join on any exemption when invoice is greater than exemption amount
     (since sometimes a person can be billed $0 for an item that they have an exemption for), and join on the same FeeItemID and for
     any invoices during an exemption. Next, there are various edge cases where paid amount (invoice minus unpaid) is greater
     than actual amount owed (invoice minus exempt). When that happens, actual unpaid balance is set to the difference between unpaid and exempt
     Otherwise, we take the UnpaidAmount and sum it
    */ 
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
    /*
     Parole board action conditions live in a different data source than special conditions associated with sentences
     The BoardAction data is unique on person, hearing date, hearing type, and staff ID. For our purposes, we keep 
     conditions where "final decision" is yes, and keep all distinct parole conditions on a given person/day
     Then we keep all relevant hearings that happen in someone's latest system session, and keep all codes since then
    */
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
            WHERE state_code = 'US_TN'
            GROUP BY 1,2
        )
        WHERE TRUE
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY assessment_date DESC) = 1
    ),
    add_more_flags_1 AS (
        SELECT 
                -- First, create all the flags needed to group people into eligibility (eligible, almost, not) and discretion (c2 - offense discretion, c3 - zero tolerance discretion)
                -- If offense is eligible then look at whether or not discretion is required
                CASE 
                    -- If sentence info is missing, that is captured elsewhere
                    WHEN COALESCE(eligible_offense, 1) = 1 
                    THEN (
                        CASE WHEN COALESCE(eligible_offense_discretion, 1) = 0 THEN 1
                        ELSE 2
                        END
                    ) 
                    ELSE 0 END offense_type_eligibility,
                -- time on supervision level
                CASE
                    WHEN DATE_DIFF(CURRENT_DATE, date_supervision_level_eligible, DAY) > 0 THEN 'eligible'
                    ELSE (
                        CASE
                            WHEN DATE_DIFF(date_supervision_level_eligible, CURRENT_DATE, DAY) <= 90 THEN 'almost_eligible'
                            ELSE 'ineligible'
                            END
                    )
                    END as eligible_time_on_supervision_level,
                -- no sanctions higher than level 1 in the past year
                CASE
                    WHEN COALESCE(latest_serious_sanction_date, '1900-01-01') <= DATE_SUB(CURRENT_DATE('US/Eastern'),INTERVAL 12 MONTH) THEN 'eligible'
                    WHEN COALESCE(latest_serious_sanction_date, '1900-01-01') <= DATE_SUB(CURRENT_DATE('US/Eastern'),INTERVAL 9 MONTH) THEN 'almost_eligible'
                    ELSE 'ineligible' END as eligible_serious_sanctions,
                /* Drug screening logic:
                - If someone is not categorized as a drug offender:
                    1. They must be 6 months or more past their latest positive test. This will be null for people with no positive test - for those, we assume they meet this condition
                    2. They must have at least 1 negative test in the past year
                    If they meet the first condition but not the second, and have never tested positive for meth, we surface them as almost eligible
                - If someone is categorized as a drug offender:
                    - If there are 2 negative tests in the past year, the most recent test is negative, and the most recent positive test (if it exists) is over 12 months old, they're eligible
                */
                CASE 
                    WHEN COALESCE(high_ad_client,0) = 0  AND DATE_DIFF(CURRENT_DATE('US/Eastern'), COALESCE(most_recent_positive_test_date, '1900-01-01'), DAY) >= 180
                    THEN ( 
                            CASE 
                                WHEN sum_negative_tests_in_past_year >= 1
                                -- If someone has had a positive test, even if its more than 6 months old, we enforce that their most recent test is negative
                                AND is_most_recent_test_negative = 1
                            THEN 'eligible'
                            WHEN COALESCE(total_screens_in_past_year,0) = 0 AND meth_contacts.Offender_ID IS NULL THEN 'almost_eligible'
                            END
                        ) 
                    WHEN COALESCE(high_ad_client,0) = 1 
                        AND sum_negative_tests_in_past_year >= 2 
                        AND is_most_recent_test_negative = 1 
                        AND DATE_DIFF(CURRENT_DATE('US/Eastern'), COALESCE(most_recent_positive_test_date, '1900-01-01'), DAY) >= 365
                    THEN 'eligible'
                    ELSE 'ineligible'
                    END AS drug_screen_eligibility,   
                -- Flag for whether discretion is required because of zero tolerance codes or missing sentencing info
                CASE 
                    /*
                    The logic here is: violations on diversion/probation can change someone expiration date, and we have reason to 
                    think that there are often a lot of judgement orders/revocation orders that affect these supervision types that don't 
                    appear in the sentencing data (or appear with a major lag). Zero tolerance codes are used as a proxy to conservatively
                    exclude people from compliant reporting and overdue for discharge, with the assumption that there might be more to their 
                    sentence that we don't know about. Since parole information is generally more reliable and updated, we don't apply that same
                    filter to people on parole
                */
                    WHEN zero_tolerance_codes.Offender_ID IS NOT NULL AND supervision_type NOT LIKE '%PAROLE%' THEN 3
                    WHEN missing_sent_info = 1 THEN 3
                    ELSE 1 END AS zt_discretion_eligibility,
                -- Counties in JD 17 don't allow CR for probation cases
                CASE WHEN judicial_district = '17' AND supervision_type LIKE '%PROBATION%' THEN 0 ELSE 1 END AS eligible_counties,
                -- Exclude anyone on lifetime supervision or with a life sentence
                CASE WHEN COALESCE(LifetimeSupervision,'N') != 'Y' AND COALESCE(LifeDeathHabitual,'N') = 'N' AND life_sentence_ISC.Offender_ID IS NULL THEN 1 
                    ELSE 0 END AS eligible_lifetime_flag,
                -- fines and fees
                CASE WHEN COALESCE(unpaid_total_latest,0) <= 500 THEN 'low_balance'
                     WHEN permanent_exemptions.AccountSAK IS NOT NULL THEN 'exempt'
                     WHEN COALESCE(unpaid_total_latest,0) <= 2000 AND have_past_1_month_payment = 1 AND have_past_2_month_payment = 1 AND have_past_3_month_payment = 1 THEN 'regular_payments'
                    ELSE 'ineligible' END AS fines_fees_eligible,
                -- Exclude anyone from CR who was previous rejected
                CASE WHEN 
                    CR_rejection_codes.Offender_ID IS NULL THEN 1 ELSE 0 END AS eligible_cr_previous_rejection,
                -- consider those rejected for non-final reasons as almost eligible. Otherwise, they're eligible 
                CASE WHEN cr_rejections_past_3_months IS NOT NULL THEN 1 
                    ELSE 0
                    END AS cr_recent_rejection_eligible_bool,
                
                -- Other fields, used for analysis or surfacing on the front end
                cr_rejections_past_3_months,                
                sentences_join.* EXCEPT(Offender_ID,has_TN_sentence),
                sentences_join.Offender_ID AS person_external_id,
                sum_payments.last_paid_amount,
                zt_codes,
                high_ad_client,
                assessment_date AS latest_assessment_date,                  
                COALESCE(total_screens_in_past_year,0) AS total_screens_in_past_year,
                CASE WHEN permanent_exemptions.AccountSAK IS NOT NULL THEN 'Fees Waived' 
                     END AS exemption_notes,
                current_exemption_type,
                COALESCE(full_name,TO_JSON_STRING(STRUCT(first_name as given_names,"" as middle_names,"" as name_suffix,last_name as surname)))  AS person_name,
                COALESCE(has_TN_sentence,0) AS has_TN_sentence,
                COALESCE(sentence_expiration_date_internal,sentence_expiration_date) AS expiration_date,
                DATE_DIFF(COALESCE(sentence_expiration_date_internal,sentence_expiration_date), current_date('US/Eastern'), DAY) AS time_to_expiration_days,
                CASE WHEN COALESCE(sentence_expiration_date_internal,sentence_expiration_date) < current_date('US/Eastern') 
                    AND COALESCE(LifetimeSupervision,'N') != 'Y' AND COALESCE(LifeDeathHabitual,'N') = 'N' AND life_sentence_ISC.Offender_ID IS NULL
                    AND (zero_tolerance_codes.Offender_ID IS NULL OR supervision_type LIKE '%PAROLE%')
                    AND person_status in ("ACTV","PEND")
                    AND missing_at_least_1_exp_date = 0
                    AND latest_tepe_date IS NOT NULL
                    THEN 1 ELSE 0 END AS overdue_for_discharge_tepe,
                CASE WHEN COALESCE(sentence_expiration_date_internal,sentence_expiration_date) < current_date('US/Eastern') 
                    AND COALESCE(LifetimeSupervision,'N') != 'Y' AND COALESCE(LifeDeathHabitual,'N') = 'N' AND life_sentence_ISC.Offender_ID IS NULL
                    AND (zero_tolerance_codes.Offender_ID IS NULL OR supervision_type LIKE '%PAROLE%')
                    AND person_status in ("ACTV","PEND")
                    AND missing_at_least_1_exp_date = 0
                    AND latest_tepe_date IS NULL
                    THEN 1 ELSE 0 END AS overdue_for_discharge_no_case_closure,
                CASE WHEN COALESCE(sentence_expiration_date_internal,sentence_expiration_date) <= DATE_ADD(current_date('US/Eastern'), INTERVAL 90 DAY) 
                    AND COALESCE(LifetimeSupervision,'N') != 'Y' AND COALESCE(LifeDeathHabitual,'N') = 'N' AND life_sentence_ISC.Offender_ID IS NULL
                    AND (zero_tolerance_codes.Offender_ID IS NULL OR supervision_type LIKE '%PAROLE%')
                    AND missing_at_least_1_exp_date = 0
                    AND person_status in ("ACTV","PEND")
                    THEN 1 ELSE 0 END AS overdue_for_discharge_within_90,
                CASE WHEN COALESCE(sentence_expiration_date_internal,sentence_expiration_date) <= DATE_ADD(current_date('US/Eastern'), INTERVAL 180 DAY) 
                    AND COALESCE(LifetimeSupervision,'N') != 'Y' AND COALESCE(LifeDeathHabitual,'N') = 'N' AND life_sentence_ISC.Offender_ID IS NULL
                    AND (zero_tolerance_codes.Offender_ID IS NULL OR supervision_type LIKE '%PAROLE%')
                    AND missing_at_least_1_exp_date = 0
                    AND person_status in ("ACTV","PEND")
                    THEN 1 ELSE 0 END AS overdue_for_discharge_within_180,
                DATE_DIFF(COALESCE(sentence_expiration_date,sentence_expiration_date_internal),sentence_start_date,DAY) AS sentence_length_days,
                DATE_DIFF(COALESCE(sentence_expiration_date,sentence_expiration_date_internal),sentence_start_date,YEAR) AS sentence_length_years,
                person_status,
                special_conditions_on_current_sentences,
                COALESCE(unpaid_total_latest,0) AS current_balance,
                COALESCE(unpaid_total_all,0) AS current_balance_all,
                most_recent_payment AS last_payment_date,
                last_paid_amount AS last_payment_amount,
                latest_tepe_date,
                latest_zzz_date,
                latest_zzz_code,
                latest_mvmt_code,
                latest_mvmt_date,
                current_supervision_level_start AS supervision_level_start_internal,
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
                lifetime_offenses AS lifetime_offenses_all,
                DRUN_array AS last_DRUN,
                most_recent_positive_test_date,
                sanctions_in_last_year,
                latest_serious_sanction_date,
                DATE_ADD(COALESCE(latest_serious_sanction_date,'1900-01-01'), INTERVAL 12 MONTH) AS date_serious_sanction_eligible,
                board_conditions,
                arrests_past_year AS last_arr_check_past_year,
                COALESCE(drug_offense,drug_offense_ever) AS drug_offense_overall,
                sum_negative_tests_in_past_year,
                is_most_recent_test_negative,
                earliest_DRUN_date,  
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
        LEFT JOIN contacts_cte_arrays 
            USING(Offender_ID)
        LEFT JOIN relevant_conditions
            USING(Offender_ID)
        LEFT JOIN drun_contacts
            USING(Offender_ID)
        LEFT JOIN meth_contacts
            USING(Offender_ID)
        LEFT JOIN special_conditions
            USING(person_id)
        LEFT JOIN `{project_id}.{base_dataset}.state_person` sp
            ON sp.person_id = sentences_join.person_id
            AND sp.state_code = 'US_TN'
        LEFT JOIN person_status_cte
            USING(Offender_ID)
        LEFT JOIN latest_tepe
            ON sentences_join.Offender_ID = latest_tepe.Offender_ID
            AND latest_tepe.latest_tepe_date >= DATE_SUB(COALESCE(sentence_expiration_date_internal,sentence_expiration_date), INTERVAL 30 DAY)
        LEFT JOIN latest_zzz
            on sentences_join.Offender_ID = latest_zzz.Offender_ID
            AND latest_zzz.latest_zzz_date >= latest_tepe.latest_tepe_date
        LEFT JOIN latest_off_mvmt 
            on sentences_join.Offender_ID = latest_off_mvmt.Offender_ID
            AND latest_off_mvmt.latest_mvmt_date >= latest_tepe.latest_tepe_date    
        LEFT JOIN zero_tolerance_codes 
            ON sentences_join.Offender_ID = zero_tolerance_codes.Offender_ID
            AND zero_tolerance_codes.latest_zero_tolerance_sanction_date  > COALESCE(sentence_start_date,'0001-01-01')
    ),
    determine_criteria AS (
        SELECT *, 
            CASE
                -- People on ICOTS and minimum with no active TN sentences should automatically be on compliant reporting 
                WHEN supervision_type LIKE '%ISC%' 
                    AND supervision_level LIKE '%MINIMUM%' 
                    AND has_TN_sentence = 0 
                    AND eligible_lifetime_flag = 1 
                    AND no_sup_level_higher_than_mininmum in ('Eligible')
                THEN 'c4'
                WHEN LEAST(
                        eligible_supervision_level,
                        eligible_arrests_flag,
                        spe_conditions_not_overdue_flag,
                        offense_type_eligibility,
                        zt_discretion_eligibility,
                        eligible_counties,
                        eligible_lifetime_flag,
                        eligible_cr_previous_rejection,
                        1 - overdue_for_discharge_no_case_closure, 
                        1 - overdue_for_discharge_within_90
                    ) > 0
                    THEN CONCAT(
                        'c',
                        CAST(
                            GREATEST(
                                eligible_supervision_level,
                                eligible_arrests_flag,
                                spe_conditions_not_overdue_flag,
                                offense_type_eligibility,
                                zt_discretion_eligibility,
                                eligible_counties,
                                eligible_lifetime_flag,
                                eligible_cr_previous_rejection,
                                1 - overdue_for_discharge_no_case_closure, 
                                1 - overdue_for_discharge_within_90
                            )
                        AS STRING)
                    ) END AS cr_eligible_discretion_level,
            eligible_time_on_supervision_level_bool + eligible_serious_sanctions_bool + drug_screen_eligibility_bool + fines_fees_eligible_bool + cr_recent_rejection_eligible_bool AS remaining_addl_criteria_needed,
            CASE WHEN eligible_time_on_supervision_level_bool + eligible_serious_sanctions_bool + drug_screen_eligibility_bool + fines_fees_eligible_bool + cr_recent_rejection_eligible_bool = 0 THEN 'eligible'
                 WHEN eligible_time_on_supervision_level_bool + eligible_serious_sanctions_bool + drug_screen_eligibility_bool + fines_fees_eligible_bool + cr_recent_rejection_eligible_bool > 0 THEN 'almost_eligible'
                 END AS cr_eligible_complete
        FROM 
            (
            SELECT *,
                CASE WHEN eligible_time_on_supervision_level = 'almost_eligible' THEN 1
                     WHEN eligible_time_on_supervision_level = 'eligible' THEN 0
                     END as eligible_time_on_supervision_level_bool,
                CASE WHEN eligible_serious_sanctions = 'almost_eligible' THEN 1
                     WHEN eligible_serious_sanctions = 'eligible' THEN 0
                     END as eligible_serious_sanctions_bool,
                CASE WHEN drug_screen_eligibility = 'almost_eligible' THEN 1
                     WHEN drug_screen_eligibility = 'eligible' THEN 0
                     END as drug_screen_eligibility_bool,
                CASE WHEN fines_fees_eligible = 'ineligible' AND current_balance BETWEEN 500 AND 2000 THEN 1
                     WHEN fines_fees_eligible != 'ineligible' THEN 0
                     END as fines_fees_eligible_bool,
            FROM add_more_flags_1            
            )
    )
    SELECT *,
        CASE WHEN cr_eligible_discretion_level IN ('c1','c2','c3') THEN remaining_addl_criteria_needed 
            WHEN cr_eligible_discretion_level = 'c4' THEN 0
            END AS remaining_criteria_needed,
        CASE 
            WHEN cr_eligible_discretion_level = 'c4' THEN cr_eligible_discretion_level
            WHEN cr_eligible_complete IN ('eligible','almost_eligible') THEN cr_eligible_discretion_level 
            END AS compliant_reporting_eligible
    FROM determine_criteria
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
