#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""TEMPORARY BQ view contraining the logic for calculating if a person is eligible to be
transferred to compliant reporting."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_COMPLIANT_REPORTING_LOGIC_VIEW_NAME = "us_tn_compliant_reporting_logic"

US_TN_COMPLIANT_REPORTING_LOGIC_DESCRIPTION = (
    """Calculates if people on supervision are eligible for compliant reporting."""
)

US_TN_COMPLIANT_REPORTING_LOGIC_QUERY_TEMPLATE = """
    -- First CTE uses standards list and creates flags to apply supervision level and time spent criteria
    WITH standards AS (
        SELECT * EXCEPT(Offender_ID),
                LPAD(CAST(Offender_ID AS string), 8, '0') AS Offender_ID,
                -- Criteria: 1 year on Minimum, 18 months on Medium
                CASE WHEN Supervision_Level LIKE '%MINIMUM%' AND DATE_DIFF(CURRENT_DATE('US/Eastern'),Plan_Start_Date,MONTH)>=12 THEN 1
                     WHEN Supervision_Level LIKE '%MEDIUM%' AND DATE_DIFF(CURRENT_DATE('US/Eastern'),Plan_Start_Date,MONTH)>=18 THEN 1
                     ELSE 0 END AS time_on_level_flag,
                -- Criteria: Currently on Minimum or Medium
                CASE WHEN (Supervision_Level LIKE '%MEDIUM%' OR Supervision_Level LIKE '%MINIMUM%') THEN 1 ELSE 0 END AS eligible_supervision_level,
            FROM `{project_id}.static_reference_tables.us_tn_standards_due`
            WHERE date_of_standards = (
                SELECT MAX(date_of_standards)
                FROM `{project_id}.static_reference_tables.us_tn_standards_due`
            )
    ), 
    -- This CTE pulls past supervision plan information to catch edge cases where if someone moved from minimum -> medium less than 18 months ago,
    -- but collectively has been on medium or less for 18 months, that still counts
    supervision_plan AS (
        SELECT *
        FROM (
            SELECT * , 
                LAG(SupervisionLevelClean) OVER(partition by Offender_ID ORDER BY PlanStartDate ASC) AS prev_level,
                LAG(PlanStartDate) OVER(partition by Offender_ID ORDER BY PlanStartDate ASC) AS prev_start,
                DATE_DIFF(CURRENT_DATE('US/Eastern'),PlanStartDate, MONTH) AS time_since_current_start,
                DATE_DIFF(CURRENT_DATE('US/Eastern'),LAG(PlanStartDate) OVER(partition by Offender_ID ORDER BY PlanStartDate ASC), MONTH) AS time_since_prev_start
            FROM (
                SELECT OffenderID as Offender_ID, 
                CAST(CAST(PlanStartDate AS datetime) AS DATE) AS PlanStartDate,
                CAST(CAST(PlanEndDate AS datetime) AS DATE) AS PlanEndDate,
                SupervisionLevel,
                PlanType,
                -- Recategorize raw text into MINIMUM and MEDIUM
                CASE WHEN SupervisionLevel IN ('4MI', 'MIN', 'Z1A', 'Z1M','ZMI' ) THEN 'MINIMUM' 
                    WHEN SupervisionLevel IN ('4ME','KG2','KN2','MED','QG2','QN2','VG2','VN2','XG2','XMD','XN2','Z2A','Z2M','ZME') THEN 'MEDIUM'
                    END AS SupervisionLevelClean
                FROM `{project_id}.us_tn_raw_data_up_to_date_views.SupervisionPlan_latest`
                )
        )
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
    -- This CTE combines the people with edge case situations in the 2nd CTE to the main list in the first CTE
    -- It also creates a flag for Sanctions Greater Than Level 1 In Past Year
    -- Note: Once we have sanctions data, we need to pull from there and look at full universe of sanctions in the last year, and
    -- whether any were above level 1
    sup_plan_standards AS (
        SELECT standards.*,
            CASE WHEN standards.supervision_level LIKE '%MEDIUM%' AND supervision_plan.Offender_ID IS NOT NULL THEN 1
                ELSE time_on_level_flag
                END AS time_on_level_flag_adj,
            -- The logic here is: if you don't have a last sanction date, or its older than 12 months, you're eligible
            -- if your last type is IPDS or OPRD (both level 1 sanctions), then regardless of date you're eligible
            -- Note, I pulled these two codes based on the the current values taken by this variable. 
            -- This is not a comprehensive list and needs to be updated
            -- when we have sanctions data
            CASE WHEN Last_Sanctions_Date IS NULL THEN 1
                 WHEN DATE_DIFF(CURRENT_DATE('US/Eastern'),Last_Sanctions_Date,MONTH)>=12 THEN 1
                 WHEN Last_Sanctions_Type IN ('IPDS','OPRD') THEN 1
                 ELSE 0 END AS no_serious_sanctions_flag
        FROM standards
        LEFT JOIN supervision_plan 
            USING(Offender_ID)
    ),
    -- Sentencing data lives in 3 places: Sentence, Diversion, and ISCSentence (out of state sentence)
    -- The first two have similar structures, so I union them and then flag ineligible offenses
    -- ISC Sentence has a different structure and very different offense type strings since these sentences are from all over the country
    -- The general wrangling here is to :
    -- 1) Flag ineligible offenses across all 3 datasets
    -- 2) Flag active and inactive sentences
    -- 3) Use 1) and 2) to compute current offenses, lifetime offenses, max expiration date, and most recent start date
    -- The offense type criteria is:
        -- Offense type not domestic abuse or sexual assault
        -- Offense type not DUI in past 5 years
        -- Offense type not crime against person that resulted in physical bodily harm
        -- Offense type not crime where victim was under 18. Note, I did not see any OffenseDescriptions referencing a victim aged 
        -- less than 18, but did see some for people under 13, so that's what I've included 
    diversion AS (
        SELECT OffenderID AS offender_id,
              OffenseDescription AS offense_description,
              CAST(CAST(ExpirationDate AS DATETIME) AS DATE) AS expiration_date,
              CAST(CAST(ExpirationDate AS DATETIME) AS DATE) AS full_expiration_date,
              CAST(CAST(DiversionGrantedDate AS DATETIME) AS DATE) AS sentence_effective_date,
              CASE WHEN DiversionStatus = 'C' THEN 'IN' ELSE 'AC' END AS sentence_status,
              CAST(CAST(DiversionGrantedDate AS DATETIME) AS DATE) AS offense_date,
              CAST(ConvictionCounty AS STRING) AS conviction_county,
              'DIVERSION' AS sentence_source,
        FROM `{project_id}.us_tn_raw_data_up_to_date_views.Diversion_latest` d
        LEFT JOIN `{project_id}.us_tn_raw_data_up_to_date_views.OffenderStatute_latest`
            USING(Offense)
    ),
    sentences AS (
        SELECT offender_id,
              offense_description,
              expiration_date,
              full_expiration_date,
              sentence_effective_date,
              sentence_status,
              offense_date,
              CAST(conviction_county AS STRING) as conviction_county,
              'SENTENCE' AS sentence_source,
        FROM `{project_id}.sessions.us_tn_sentences_preprocessed_materialized`
    ),
    prior_record AS (
        SELECT OffenderID as offender_id,
                OffenseDescription AS offense_description,
                CAST(CAST(DispositionDate AS DATETIME) AS DATE) AS expiration_date,
                CAST(CAST(DispositionDate AS DATETIME) AS DATE) AS full_expiration_date,
                CAST(CAST(ArrestDate AS DATETIME) AS DATE) AS sentence_effective_date,
                'Prior' AS sentence_status,
                CAST(CAST(OffenseDate AS DATETIME) AS DATE) AS offense_date,
                CourtName AS conviction_county,
                'PriorRecord' AS sentence_source,
        FROM `{hardcoded_prod_project_id}.us_tn_raw_data.PriorRecord` pr
        LEFT JOIN `{project_id}.us_tn_raw_data_up_to_date_views.OffenderStatute_latest` ofs
            ON COALESCE(pr.ConvictionOffense, pr.ChargeOffense) = ofs.Offense

    ),
    isc_sent AS (
        SELECT OffenderID AS Offender_ID,
                ConvictedOffense AS offense_description,
                CAST(CAST(ExpirationDate AS DATETIME) AS DATE) AS expiration_date,
                CAST(CAST(ExpirationDate AS DATETIME) AS DATE) AS full_expiration_date,
                CAST(CAST(SentenceImposedDate AS DATETIME) AS DATE) AS sentence_effective_date,
                CASE WHEN CAST(CAST(ExpirationDate AS DATETIME) AS DATE) < CURRENT_DATE('US/Eastern') THEN 'IN' ELSE 'AC' END AS sentence_status,
                CAST(CAST(OffenseDate AS DATETIME) AS DATE) AS offense_date,
                Jurisdiction AS conviction_county,
                'ISC' AS sentence_source,
                CASE WHEN SAFE_CAST(ConvictedOffense AS INT) IS NULL THEN 0 ELSE 1 END AS missing_offense,
                CASE WHEN (ConvictedOffense LIKE '%DRUG%' AND ConvictedOffense NOT LIKE '%ADULTERATION%')
                    OR ConvictedOffense LIKE '%METH%'
                    OR ConvictedOffense LIKE '%POSSESSION OF%CONT%'
                    OR ConvictedOffense LIKE '%POSS%OF%CONT%'
                    OR ConvictedOffense LIKE '%POSS%OF%AMPH%'
                    OR ConvictedOffense LIKE '%POSS%OF%HEROIN%'
                    OR ConvictedOffense LIKE '%VGCSA%'
                    OR ConvictedOffense LIKE '%SIMPLE POSS%'
                    OR ConvictedOffense LIKE '%COCAINE%'
                    OR ConvictedOffense LIKE '%CONT. SUBSTANCE%'
                    OR ConvictedOffense LIKE '%MARIJUANA%'
                        THEN 1 ELSE 0 END AS drug_offense,
                    CASE WHEN ConvictedOffense LIKE '%DOMESTIC%' THEN 1 ELSE 0 END AS domestic_flag,
                    CASE WHEN ConvictedOffense LIKE '%SEX%' THEN 1 ELSE 0 END AS sex_offense_flag,
                    CASE WHEN DATE_DIFF(CURRENT_DATE,CAST(CAST(OffenseDate AS DATETIME) AS DATE),YEAR) <5 AND (ConvictedOffense LIKE '%DUI%' OR ConvictedOffense LIKE '%DWI%' OR ConvictedOffense LIKE '%INFLUENCE%') THEN 1 ELSE 0 END AS dui_last_5_years,
                    CASE WHEN ConvictedOffense LIKE '%BURGLARY%' 
                        OR ConvictedOffense LIKE '%ROBBERY%'
                        OR ConvictedOffense LIKE '%ARSON%'
                        THEN 1 ELSE 0 END AS maybe_assaultive_flag,
                    CASE WHEN ConvictedOffense LIKE '%MURDER%' 
                        OR ConvictedOffense LIKE '%HOMICIDE%'
                        OR ConvictedOffense LIKE '%MANSLAUGHTER%'
                        OR ConvictedOffense LIKE '%RAPE%'
                        OR ConvictedOffense LIKE '%MOLEST%'
                        OR ConvictedOffense LIKE '%BATTERY%'
                        OR ConvictedOffense LIKE '%ASSAULT%'
                        OR ConvictedOffense LIKE '%STALKING%'
                        OR ConvictedOffense LIKE '%CRIMES AGAINST PERSON%'
                        THEN 1 ELSE 0 END AS assaultive_offense_flag,
                    CASE WHEN ConvictedOffense LIKE '%THEFT%' 
                        OR ConvictedOffense LIKE '%LARCENY%'
                        OR ConvictedOffense LIKE '%FORGERY%'
                        OR ConvictedOffense LIKE '%FRAUD%'
                        OR ConvictedOffense LIKE '%STOLE%'
                        OR ConvictedOffense LIKE '%SHOPLIFTING%'
                        OR ConvictedOffense LIKE '%EMBEZZLEMENT%'
                        OR ConvictedOffense LIKE '%FLAGRANT NON-SUPPORT%'
                        OR ConvictedOffense LIKE '%ESCAPE%'
                        OR ConvictedOffense LIKE '%BREAKING AND ENTERING%'
                        OR ConvictedOffense LIKE '%PROPERTY%'
                        OR ConvictedOffense LIKE '%STEAL%'
                        THEN 0 ELSE 1 END AS unknown_offense_flag,
                        CASE WHEN (ConvictedOffense LIKE '%13%' AND ConvictedOffense LIKE '%VICT%' ) THEN 1 ELSE 0 END AS young_victim_flag,
                
        FROM `{project_id}.us_tn_raw_data_up_to_date_views.ISCSentence_latest`
        WHERE sentence != 'LIFE'
    ),
    sent_union_diversion AS (
        SELECT *
        FROM sentences
        
        UNION ALL

        SELECT *
        FROM diversion

        UNION ALL

        SELECT *
        FROM prior_record 
    ),
    sentence_flags AS (
    SELECT sent_union_diversion.*,
       CASE WHEN COALESCE(offense_description,'NOT DEFINED') = 'NOT DEFINED' THEN 1 ELSE 0 END missing_offense,
       CASE WHEN (offense_description LIKE '%DRUG%' AND offense_description NOT LIKE '%ADULTERATION%')
                    OR offense_description LIKE '%METH%'
                    OR offense_description LIKE '%SIMPLE POSS%'
                    OR offense_description LIKE '%COCAINE%'
                    OR offense_description LIKE '%CONT. SUBSTANCE%'
                    OR offense_description LIKE '%MARIJUANA%'
                THEN 1 ELSE 0 END AS drug_offense,
        CASE WHEN offense_description LIKE '%DOMESTIC%' THEN 1 ELSE 0 END AS domestic_flag,
        CASE WHEN (offense_description LIKE '%SEX%' AND offense_description NOT LIKE '%REGIS%') THEN 1 ELSE 0 END AS sex_offense_flag,
        CASE WHEN (COALESCE(AssaultiveOffenseFlag,'N') = 'Y') THEN 1 ELSE 0 END AS assaultive_offense_flag,
        CASE WHEN (offense_description LIKE '%13%' AND offense_description LIKE '%VICT%' ) THEN 1 ELSE 0 END AS young_victim_flag,
        0 AS maybe_assaultive_flag,
        0 AS unknown_offense_flag,
        CASE WHEN DATE_DIFF(CURRENT_DATE,offense_date,YEAR) <5 AND (offense_description LIKE '%DUI%' OR offense_description LIKE '%INFLUENCE%') THEN 1 ELSE 0 END AS dui_last_5_years,
    FROM sent_union_diversion
    LEFT JOIN 
        (
        SELECT OffenseDescription, AssaultiveOffenseFlag
        FROM  `{project_id}.us_tn_raw_data_up_to_date_views.OffenderStatute_latest` 
        WHERE TRUE
        QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenseDescription 
                                    ORDER BY CASE WHEN AssaultiveOffenseFlag = 'Y' THEN 0 ELSE 1 END) = 1
        ) statute
            ON offense_description = statute.OffenseDescription 
    ),
    sent_union_isc AS (
        SELECT *,
            MAX(CASE WHEN sentence_status NOT IN ('IN','Prior') THEN 1 ELSE 0 END) OVER(PARTITION BY offender_id) AS has_active_sentence,
        FROM (
            SELECT * 
            FROM sentence_flags

            UNION ALL 

            SELECT *
            FROM isc_sent
        )
    ) ,
    -- The next two CTEs take the maximum of the offense type flag for a given person, first for all sentences and
    -- then for all sentences not marked inactive
    -- According to the policy, people are ineligible if they are currently serving a sentence for certain offenses
    -- If they served a sentence for those offenses historically, the District Director can still approve them for CR on a case by case basis
    -- If they served a sentence for those offenses historically and those sentences expired 10 years or more ago, the DD process is waived
    -- Here we create flags for whether a person ever had certain offenses, and also flags if all those offenses expired more than 10 years ago
    all_sentences AS (
        SELECT 
            Offender_ID,
            COALESCE(MAX(expiration_date),MAX(full_expiration_date)) AS sentence_expiration_date_internal, 
            MAX(CASE WHEN expiration_date IS NULL AND full_expiration_date IS NULL THEN 1 ELSE 0 END) AS missing_at_least_1_exp_date,
            max(missing_offense) AS missing_offense_ever,
            max(case when missing_offense = 1 and expiration_date <= DATE_SUB(CURRENT_DATE('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) AS all_missing_offenses_expired,
            max(drug_offense) AS drug_offense_ever,
            max(domestic_flag) AS domestic_flag_ever,
            max(case when domestic_flag = 1 and expiration_date <= DATE_SUB(CURRENT_DATE('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) AS all_domestic_offenses_expired,
            max(sex_offense_flag) AS sex_offense_flag_ever,
            max(case when sex_offense_flag = 1 and expiration_date <= DATE_SUB(CURRENT_DATE('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) AS all_sex_offenses_expired,
            max(assaultive_offense_flag) AS assaultive_offense_flag_ever,
            max(case when assaultive_offense_flag = 1 and expiration_date <= DATE_SUB(CURRENT_DATE('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) AS all_assaultive_offenses_expired,
            max(young_victim_flag) AS young_victim_flag_ever,
            max(case when young_victim_flag = 1 and expiration_date <= DATE_SUB(CURRENT_DATE('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) AS all_young_victim_offenses_expired,
            max(dui_last_5_years) AS dui_last_5_years_flag,
            max(maybe_assaultive_flag) AS maybe_assaultive_flag_ever,
            max(case when maybe_assaultive_flag = 1 and expiration_date <= DATE_SUB(CURRENT_DATE('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) AS all_maybe_assaultive_offenses_expired,
            max(unknown_offense_flag) AS unknown_offense_flag_ever,
            max(case when unknown_offense_flag = 1 and expiration_date <= DATE_SUB(CURRENT_DATE('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) AS all_unknown_offenses_expired,
            ARRAY_AGG(STRUCT(offense_description,expiration_date)) AS lifetime_offenses,
        FROM sent_union_isc
        WHERE sentence_source != 'PriorRecord'
        -- WHERE sentence_status != 'Prior'
        GROUP BY 1
    ),
    prior_sentences AS (
        SELECT 
            Offender_ID,
            max(missing_offense) AS missing_offense_prior,
            max(drug_offense) AS drug_offense_prior,
            max(domestic_flag) AS domestic_flag_prior,
            max(sex_offense_flag) AS sex_offense_flag_prior,
            max(assaultive_offense_flag) AS assaultive_offense_flag_prior,
            max(young_victim_flag) AS young_victim_flag_prior,
            max(dui_last_5_years) AS dui_last_5_years_prior,
            max(maybe_assaultive_flag) AS maybe_assaultive_flag_prior,
            max(unknown_offense_flag) AS unknown_offense_flag_prior,
            ARRAY_AGG(offense_description IGNORE NULLS) AS prior_offenses,
        FROM sent_union_isc
        WHERE sentence_status = 'Prior'
        GROUP BY 1
    ),
    active_sentences AS (
        SELECT 
            Offender_ID,
            max(missing_offense) AS missing_offense,
            max(drug_offense) AS drug_offense,
            max(domestic_flag) AS domestic_flag,
            max(sex_offense_flag) AS sex_offense_flag,
            max(assaultive_offense_flag) AS assaultive_offense_flag,
            max(young_victim_flag) AS young_victim_flag,
            max(maybe_assaultive_flag) AS maybe_assaultive_flag,
            max(unknown_offense_flag) AS unknown_offense_flag,
            ARRAY_AGG(offense_description IGNORE NULLS) AS active_offenses,
        FROM sent_union_isc
        WHERE sentence_status not in ('IN','Prior')
        GROUP BY 1
    ),
    -- if you have no active sentences or diversions, take the latest start date as your most recent start date
    sentence_start_for_inactive AS (
        SELECT Offender_ID,
                has_active_sentence,
                MAX(sentence_effective_date) AS sentence_start_date,
        FROM sent_union_isc
        WHERE has_active_sentence = 0
        GROUP BY 1,2
    ),
    -- if you have an active sentences or diversion, take the earliest start date of your active sentences as your most recent start date
    sentence_start_for_active AS (
        SELECT Offender_ID,
                has_active_sentence,
                MIN(sentence_effective_date) AS sentence_start_date,
        FROM sent_union_isc
        WHERE has_active_sentence = 1
        AND sentence_status not in ('IN','Prior')
        GROUP BY 1,2
    ),
    union_start_dates AS (
        SELECT *
        FROM sentence_start_for_active
        
        UNION ALL
        
        SELECT * 
        FROM sentence_start_for_inactive
    ),
    -- This CTE puts it all together to get offense type flags for each person, pulls the last known judicial district,
    -- and the last available conviction county for all sentences not marked inactive
    sentence_put_together AS (
        SELECT all_sentences.*,
                active_sentences.* EXCEPT(Offender_ID),
                prior_sentences.* EXCEPT(Offender_ID),
                union_start_dates.sentence_start_date,
                union_start_dates.has_active_sentence,
                judicial_district_code AS judicial_district,
                conviction_county,
        FROM all_sentences 
        LEFT JOIN active_sentences 
            USING(Offender_ID)
        LEFT JOIN prior_sentences 
            USING(Offender_ID)
        LEFT JOIN union_start_dates
            USING(Offender_ID)
        LEFT JOIN `{project_id}.state.state_person_external_id` pei
            ON all_sentences.Offender_ID = pei.external_id
        LEFT JOIN (
            SELECT person_id, judicial_district_code
            FROM `{project_id}.sessions.us_tn_judicial_district_sessions_materialized`
            WHERE judicial_district_end_date IS NULL
            -- the where clause should remove duplicates on person id but for 0.5% of cases this doesnt happen so the qualify statement picks the JD
            -- associated with the latest SED
            QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY judicial_district_start_date DESC) = 1
        ) jd
            USING(person_id)
        LEFT JOIN (
            SELECT 
                Offender_ID,
                conviction_county,
            FROM sentence_flags
            WHERE sentence_status != 'IN'
            QUALIFY ROW_NUMBER() OVER(partition by Offender_ID ORDER BY sentence_effective_date DESC) = 1 
        )
            USING(Offender_ID)
    ),
    -- Joins together sentences and standards sheet
    sentences_join AS (
        SELECT *, 
            CASE WHEN sentence_put_together.Offender_ID IS NULL THEN 1 ELSE 0 END AS missing_sent_info
        FROM sup_plan_standards
        LEFT JOIN sentence_put_together
            USING(Offender_ID)
    ),
    -- This CTE pulls information on drug contacts to understand who satisfies the criteria for drug screens
    contacts_cte AS (
        SELECT OffenderID AS Offender_ID, 
                CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) AS contact_date, 
                ContactNoteType,
                CASE WHEN ContactNoteType = 'DRUN' THEN 1 ELSE 0 END AS negative_drug_test,
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
                     AND ContactNoteType = 'DRUN' THEN 1 ELSE 0 END AS most_recent_test_negative
            FROM contacts_cte 
        )
        GROUP BY 1    
        ORDER BY 2 desc
    ), 
    -- This CTE keeps all negative screens in the last year as an array to be displayed
    contacts_cte_arrays AS (
        SELECT Offender_ID, ARRAY_AGG(contact_date) as DRUN_array
        FROM contacts_cte 
        WHERE ContactNoteType = 'DRUN'
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
        AND CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 3 MONTH)
    ),
    -- we conservatively use these codes 
    zero_tolerance_codes AS (
    SELECT OffenderID as Offender_ID, 
            MAX(CAST(CAST(ContactNoteDateTime AS datetime) AS DATE)) AS latest_zero_tolerance_sanction_date
    FROM `{project_id}.us_tn_raw_data_up_to_date_views.ContactNoteType_latest`
    WHERE ContactNoteType IN ('VWAR','PWAR','ZTVR','COHC')
    GROUP BY OffenderID
    ),
    person_status_cte AS (
            SELECT OffenderID as Offender_ID, OffenderStatus AS person_status
            FROM   `{project_id}.us_tn_raw_data_up_to_date_views.OffenderName_latest`
            WHERE TRUE
            QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderID 
                                    ORDER BY SequenceNumber DESC) = 1
    ),
    add_more_flags AS (
        SELECT sentences_join.* ,
                dru_contacts.* EXCEPT(total_screens_in_past_year,Offender_ID),
            -- General pattern:
            -- a) if dont have the flag now, didnt have it in past sentences or prior record, eligible
            -- b) if don't have the flag now, didnt have it in prior record, DID have it in past sentences by those sentences expired over 10 years ago, eligible
            -- c) if dont have flag now, had it for inactive sentences, and all past sentences not expired over 10 years ago, discretion
            -- d) if dont have flag now, had it for prior record, discretion
            -- CASE WHEN COALESCE(domestic_flag,0) = 0 AND COALESCE(domestic_flag_ever,0) = 0 THEN 'Eligible'
            --      WHEN COALESCE(domestic_flag,0) = 0 AND COALESCE(domestic_flag_ever,0) = 1 AND COALESCE(all_domestic_offenses_expired,0) = 1 THEN 'Eligible'
            --      WHEN COALESCE(domestic_flag,0) = 0 AND COALESCE(domestic_flag_ever,0) = 1 AND COALESCE(all_domestic_offenses_expired,0) = 0 THEN 'Discretion'
            --      ELSE 'Ineligible' END AS domestic_flag_eligibility,
            -- CASE WHEN COALESCE(sex_offense_flag,0) = 0 AND COALESCE(sex_offense_flag_ever,0) = 0  THEN 'Eligible'
            --      WHEN COALESCE(sex_offense_flag,0) = 0 AND COALESCE(sex_offense_flag_ever,0) = 1 AND COALESCE(all_sex_offenses_expired,0) = 1 THEN 'Eligible'
            --      WHEN COALESCE(sex_offense_flag,0) = 0 AND COALESCE(sex_offense_flag_ever,0) = 1 AND COALESCE(all_sex_offenses_expired,0) = 0 THEN 'Discretion'
            --      ELSE 'Ineligible' END AS sex_offense_flag_eligibility,
            -- CASE WHEN COALESCE(assaultive_offense_flag,0) = 0 AND COALESCE(assaultive_offense_flag_ever,0) = 0 THEN 'Eligible'
            --      WHEN COALESCE(assaultive_offense_flag,0) = 0 AND COALESCE(assaultive_offense_flag_ever,0) = 1 AND COALESCE(all_assaultive_offenses_expired,0) = 1 THEN 'Eligible'
            --      WHEN COALESCE(assaultive_offense_flag,0) = 0 AND COALESCE(assaultive_offense_flag_ever,0) = 1 AND COALESCE(all_assaultive_offenses_expired,0) = 0 THEN 'Discretion'
            --      ELSE 'Ineligible' END AS assaultive_offense_flag_eligibility,
            -- CASE WHEN COALESCE(maybe_assaultive_flag,0) = 0 AND COALESCE(maybe_assaultive_flag_ever,0) = 0 THEN 'Eligible'
            --      WHEN COALESCE(maybe_assaultive_flag,0) = 0 AND COALESCE(maybe_assaultive_flag_ever,0) = 1 AND COALESCE(all_maybe_assaultive_offenses_expired,0) = 1 THEN 'Eligible'
            --      WHEN COALESCE(maybe_assaultive_flag,0) = 0 AND COALESCE(maybe_assaultive_flag_ever,0) = 1 AND COALESCE(all_maybe_assaultive_offenses_expired,0) = 0 THEN 'Discretion'
            --      WHEN COALESCE(maybe_assaultive_flag,0) = 1 THEN 'Discretion'
            --      ELSE 'Ineligible' END AS maybe_assaultive_flag_eligibility,
            -- CASE WHEN COALESCE(unknown_offense_flag,0) = 0 AND COALESCE(unknown_offense_flag_ever,0) = 0 THEN 'Eligible'
            --      WHEN COALESCE(unknown_offense_flag,0) = 0 AND COALESCE(unknown_offense_flag_ever,0) = 1 AND COALESCE(all_unknown_offenses_expired,0) = 1 THEN 'Eligible'
            --      WHEN COALESCE(unknown_offense_flag,0) = 0 AND COALESCE(unknown_offense_flag_ever,0) = 1 AND COALESCE(all_unknown_offenses_expired,0) = 0 THEN 'Discretion'
            --      WHEN COALESCE(unknown_offense_flag,0) = 1 THEN 'Discretion'
            --      ELSE 'Ineligible' END AS unknown_offense_flag_eligibility,
            -- CASE WHEN COALESCE(missing_offense,0) = 0 AND COALESCE(missing_offense_ever,0) = 0 THEN 'Eligible'
            --      WHEN COALESCE(missing_offense,0) = 0 AND COALESCE(missing_offense_ever,0) = 1 AND COALESCE(all_missing_offenses_expired,0) = 1 THEN 'Eligible'
            --      WHEN COALESCE(missing_offense,0) = 0 AND COALESCE(missing_offense_ever,0) = 1 AND COALESCE(all_missing_offenses_expired,0) = 0 THEN 'Discretion'
            --      WHEN COALESCE(missing_offense,0) = 0 AND COALESCE(missing_offense_ever,0) = 1 AND COALESCE(missing_offense_ever,0) = 1 AND COALESCE(all_missing_offenses_expired,0) = 0 THEN 'Discretion'
            --      ELSE 'Discretion' END AS missing_offense_flag_eligibility,
            -- CASE WHEN COALESCE(young_victim_flag,0) = 0 AND COALESCE(young_victim_flag_ever,0) = 0 THEN 'Eligible'
            --      WHEN COALESCE(young_victim_flag,0) = 0 AND COALESCE(young_victim_flag_ever,0) = 1 AND COALESCE(all_young_victim_offenses_expired,0) = 1 THEN 'Eligible'
            --      WHEN COALESCE(young_victim_flag,0) = 0 AND COALESCE(young_victim_flag_ever,0) = 1 AND COALESCE(all_young_victim_offenses_expired,0) = 0 THEN 'Discretion'
            --      ELSE 'Ineligible' END AS young_victim_flag_eligibility,
            -- CASE WHEN COALESCE(dui_last_5_years_flag,0) = 0 THEN 'Eligible' ELSE 'Ineligible' END AS dui_last_5_years_flag_eligibility,
            CASE WHEN COALESCE(domestic_flag,0) = 0 AND COALESCE(domestic_flag_ever,0) = 0 AND COALESCE(domestic_flag_prior,0) = 0 THEN 'Eligible'
                 WHEN COALESCE(domestic_flag,0) = 0 AND COALESCE(domestic_flag_prior,0) = 0 AND COALESCE(domestic_flag_ever,0) = 1 AND COALESCE(all_domestic_offenses_expired,0) = 1 THEN 'Eligible'
                 WHEN COALESCE(domestic_flag,0) = 0 AND COALESCE(domestic_flag_ever,0) = 1 AND COALESCE(all_domestic_offenses_expired,0) = 0 THEN 'Discretion'
                 WHEN COALESCE(domestic_flag,0) = 0 AND COALESCE(domestic_flag_prior,0) = 1 THEN 'Discretion'
                 ELSE 'Ineligible' END AS domestic_flag_eligibility,
            CASE WHEN COALESCE(sex_offense_flag,0) = 0 AND COALESCE(sex_offense_flag_ever,0) = 0 AND COALESCE(sex_offense_flag_prior,0) = 0  THEN 'Eligible'
                 WHEN COALESCE(sex_offense_flag,0) = 0 AND COALESCE(sex_offense_flag_prior,0) = 0 AND COALESCE(sex_offense_flag_ever,0) = 1 AND COALESCE(all_sex_offenses_expired,0) = 1 THEN 'Eligible'
                 WHEN COALESCE(sex_offense_flag,0) = 0 AND COALESCE(sex_offense_flag_ever,0) = 1 AND COALESCE(all_sex_offenses_expired,0) = 0 THEN 'Discretion'
                 WHEN COALESCE(sex_offense_flag,0) = 0 AND COALESCE(sex_offense_flag_prior,0) = 1 THEN 'Discretion'
                 ELSE 'Ineligible' END AS sex_offense_flag_eligibility,
            CASE WHEN COALESCE(assaultive_offense_flag,0) = 0 AND COALESCE(assaultive_offense_flag_ever,0) = 0 AND COALESCE(assaultive_offense_flag_prior,0) = 0 THEN 'Eligible'
                 WHEN COALESCE(assaultive_offense_flag,0) = 0 AND COALESCE(assaultive_offense_flag_prior,0) = 0 AND COALESCE(assaultive_offense_flag_ever,0) = 1 AND COALESCE(all_assaultive_offenses_expired,0) = 1 THEN 'Eligible'
                 WHEN COALESCE(assaultive_offense_flag,0) = 0 AND COALESCE(assaultive_offense_flag_ever,0) = 1 AND COALESCE(all_assaultive_offenses_expired,0) = 0 THEN 'Discretion'
                 WHEN COALESCE(assaultive_offense_flag,0) = 0 AND COALESCE(assaultive_offense_flag_prior,0) = 1 THEN 'Discretion'
                 ELSE 'Ineligible' END AS assaultive_offense_flag_eligibility,
            CASE WHEN COALESCE(maybe_assaultive_flag,0) = 0 AND COALESCE(maybe_assaultive_flag_ever,0) = 0 AND COALESCE(maybe_assaultive_flag_prior,0) = 0 THEN 'Eligible'
                 WHEN COALESCE(maybe_assaultive_flag,0) = 0 AND COALESCE(maybe_assaultive_flag_prior,0) = 0 AND COALESCE(maybe_assaultive_flag_ever,0) = 1 AND COALESCE(all_maybe_assaultive_offenses_expired,0) = 1 THEN 'Eligible'
                 WHEN COALESCE(maybe_assaultive_flag,0) = 0 AND COALESCE(maybe_assaultive_flag_ever,0) = 1 AND COALESCE(all_maybe_assaultive_offenses_expired,0) = 0 THEN 'Discretion'
                 WHEN COALESCE(maybe_assaultive_flag,0) = 0 AND COALESCE(maybe_assaultive_flag_prior,0) = 1 THEN 'Discretion'
                 WHEN COALESCE(maybe_assaultive_flag,0) = 1 THEN 'Discretion'
                 ELSE 'Ineligible' END AS maybe_assaultive_flag_eligibility,
            CASE WHEN COALESCE(unknown_offense_flag,0) = 0 AND COALESCE(unknown_offense_flag_ever,0) = 0 AND COALESCE(unknown_offense_flag_prior,0) = 0 THEN 'Eligible'
                 WHEN COALESCE(unknown_offense_flag,0) = 0 AND COALESCE(unknown_offense_flag_prior,0) = 0 AND COALESCE(unknown_offense_flag_ever,0) = 1 AND COALESCE(all_unknown_offenses_expired,0) = 1 THEN 'Eligible'
                 WHEN COALESCE(unknown_offense_flag,0) = 0 AND COALESCE(unknown_offense_flag_ever,0) = 1 AND COALESCE(all_unknown_offenses_expired,0) = 0 THEN 'Discretion'
                 WHEN COALESCE(unknown_offense_flag,0) = 0 AND COALESCE(unknown_offense_flag_prior,0) = 1 THEN 'Discretion'
                 WHEN COALESCE(unknown_offense_flag,0) = 1 THEN 'Discretion'
                 ELSE 'Ineligible' END AS unknown_offense_flag_eligibility,
            CASE WHEN COALESCE(missing_offense,0) = 0 AND COALESCE(missing_offense_ever,0) = 0 AND COALESCE(missing_offense_prior,0) = 0 THEN 'Eligible'
                 -- If only missing offense is in prior record, I don't think that will / should exclude
                 WHEN COALESCE(missing_offense,0) = 0 AND COALESCE(missing_offense_ever,0) = 0 AND COALESCE(missing_offense_prior,0) = 1 THEN 'Eligible'
                 WHEN COALESCE(missing_offense,0) = 0 AND COALESCE(missing_offense_prior,0) = 0 AND COALESCE(missing_offense_ever,0) = 1 AND COALESCE(all_missing_offenses_expired,0) = 1 THEN 'Eligible'
                 WHEN COALESCE(missing_offense,0) = 0 AND COALESCE(missing_offense_ever,0) = 1 AND COALESCE(all_missing_offenses_expired,0) = 0 THEN 'Discretion'
                 ELSE 'Discretion' END AS missing_offense_flag_eligibility,
            CASE WHEN COALESCE(young_victim_flag,0) = 0 AND COALESCE(young_victim_flag_ever,0) = 0 AND COALESCE(young_victim_flag_prior,0) = 0 THEN 'Eligible'
                 WHEN COALESCE(young_victim_flag,0) = 0 AND COALESCE(young_victim_flag_prior,0) = 0 AND COALESCE(young_victim_flag_ever,0) = 1 AND COALESCE(all_young_victim_offenses_expired,0) = 1 THEN 'Eligible'
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
            WHEN drug_offense = 0 AND Last_DRU_Note IS NULL THEN 1
             -- Potentially comment out
             WHEN drug_offense = 0 AND DATE_DIFF(CURRENT_DATE,Last_DRU_Note,MONTH)>= 12 THEN 1
             WHEN drug_offense = 0 and Last_DRU_Type = 'DRUN' THEN 1
             -- Potentially comment out
             WHEN drug_offense = 0 and sum_negative_tests_in_past_year >= 1 THEN 1
             -- These two conditions ensure that selecting this flag doesnt automatically eliminate people missing sentencing info
             -- or where drug offense = 1 since there are separate flags for that
             WHEN drug_offense IS NULL THEN NULL
             WHEN drug_offense = 1 THEN NULL
             ELSE 0 END AS drug_screen_pass_flag_non_drug_offense,
        -- Logic here: for drug offenses
        -- If there are 2 negative tests in the past year and the most recent test is negative, eligible
        CASE WHEN drug_offense = 1 and sum_negative_tests_in_past_year >= 2 and most_recent_test_negative = 1 THEN 1
             -- These two conditions ensure that selecting this flag doesnt automatically eliminate people missing sentencing info
             -- or where drug offense = 0 since there are separate flags for that
             WHEN drug_offense = 0 THEN NULL
             WHEN drug_offense IS NULL THEN NULL
             ELSE 0 END AS drug_screen_pass_flag_drug_offense,
        -- Criteria: Special Conditions are up to date
        CASE WHEN SAFE_CAST(SPE_Note_Due AS DATE) <= CURRENT_DATE('US/Eastern') THEN 0 ELSE 1 END AS spe_conditions_not_overdue_flag,
        -- Counties in JD 17 don't allow CR for probation cases
        CASE WHEN judicial_district = '17' AND Case_Type LIKE '%PROBATION%' THEN 0 ELSE 1 END AS eligible_counties,
        -- Exclude anyone on lifetime supervision or with a life sentence
        CASE WHEN 
            COALESCE(LifetimeSupervision,'N') != 'Y' AND COALESCE(LifeDeathHabitual,'N') = 'N' THEN 1 ELSE 0 END AS no_lifetime_flag,
        -- Exclude anyone from CR who was previous rejected
        CASE WHEN 
            CR_rejection_codes.Offender_ID IS NULL THEN 1 ELSE 0 END AS cr_not_previous_rejected_flag,
        CASE WHEN 
            CR_rejection_codes_dynamic.Offender_ID IS NULL THEN 1 ELSE 0 END AS cr_not_rejected_x_months_flag,
        CASE WHEN zero_tolerance_codes.Offender_ID IS NULL THEN 1 ELSE 0 END AS no_zero_tolerance_codes,
        FROM sentences_join
        LEFT JOIN dru_contacts 
            USING(Offender_ID)
        LEFT JOIN CSL
            USING(Offender_ID)
        LEFT JOIN CR_rejection_codes 
            USING(Offender_ID)
        LEFT JOIN CR_rejection_codes_dynamic 
            USING(Offender_ID)
        LEFT JOIN zero_tolerance_codes 
            ON sentences_join.Offender_ID = zero_tolerance_codes.Offender_ID
            AND zero_tolerance_codes.latest_zero_tolerance_sanction_date  > COALESCE(sentence_start_date,'0001-01-01')

    )
    SELECT
        COALESCE(full_name,TO_JSON_STRING(STRUCT(first_name as given_names,"" as middle_names,"" as name_suffix,last_name as surname)))  AS person_name,
        pei.person_id,
        first_name,
        last_name,
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
        EXP_Date AS sentence_expiration_date,
        missing_at_least_1_exp_date,
        sentence_expiration_date_internal,
        COALESCE(sentence_expiration_date_internal,EXP_Date) AS expiration_date,
        DATE_DIFF(COALESCE(sentence_expiration_date_internal,EXP_Date), CURRENT_DATE('US/Eastern'), DAY) AS time_to_expiration_days,
        CASE WHEN COALESCE(sentence_expiration_date_internal,EXP_Date) < CURRENT_DATE('US/Eastern') 
            AND no_lifetime_flag = 1
            AND (no_zero_tolerance_codes = 1 OR Case_Type LIKE '%PAROLE%')
            AND person_status in ("ACTV","PEND")
            AND missing_at_least_1_exp_date = 0
            THEN 1 ELSE 0 END AS overdue_for_discharge,
        CASE WHEN COALESCE(sentence_expiration_date_internal,EXP_Date) <= DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 90 DAY) 
            AND no_lifetime_flag = 1
            AND (no_zero_tolerance_codes = 1 OR Case_Type LIKE '%PAROLE%')
            AND missing_at_least_1_exp_date = 0
            AND person_status in ("ACTV","PEND")
            THEN 1 ELSE 0 END AS overdue_for_discharge_within_90,
        CASE WHEN COALESCE(sentence_expiration_date_internal,EXP_Date) <= DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 180 DAY) 
            AND no_lifetime_flag = 1
            AND (no_zero_tolerance_codes = 1 OR Case_Type LIKE '%PAROLE%')
            AND missing_at_least_1_exp_date = 0
            AND person_status in ("ACTV","PEND")
            THEN 1 ELSE 0 END AS overdue_for_discharge_within_180,
        sentence_start_date,
        DATE_DIFF(COALESCE(EXP_Date,sentence_expiration_date_internal),sentence_start_date,DAY) AS sentence_length_days,
        DATE_DIFF(COALESCE(EXP_Date,sentence_expiration_date_internal),sentence_start_date,YEAR) AS sentence_length_years,
        Offender_ID AS person_external_id,
        person_status,
        Staff_ID AS officer_id,
        Staff_First_Name AS staff_first_name,
        Staff_Last_Name AS staff_last_name,
        Case_Type AS supervision_type,
        judicial_district,
        Supervision_Level AS supervision_level,
        Plan_Start_Date AS supervision_level_start,
        active_offenses AS offense_type,
        lifetime_offenses,
        has_active_sentence,
        DRUN_array AS last_DRUN,
        Last_Sanctions_Type AS last_sanction,
        Last_DRU_Note AS last_drug_screen_date,
        Region as district,
        time_on_level_flag_adj,
        no_serious_sanctions_flag,
        spe_conditions_not_overdue_flag,
        domestic_flag,
        sex_offense_flag,
        assaultive_offense_flag,
        young_victim_flag,
        dui_last_5_years_flag,
        domestic_flag_ever,
        all_domestic_offenses_expired,
        sex_offense_flag_ever,
        all_sex_offenses_expired,
        assaultive_offense_flag_ever,
        all_assaultive_offenses_expired,
        young_victim_flag_ever,
        drug_screen_pass_flag_drug_offense,
        drug_screen_pass_flag_non_drug_offense,
        drug_offense,
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
        CASE WHEN domestic_flag_eligibility = 'Eligible'
            AND sex_offense_flag_eligibility = 'Eligible'
            AND assaultive_offense_flag_eligibility = 'Eligible'
            AND maybe_assaultive_flag_eligibility = 'Eligible'
            AND unknown_offense_flag_eligibility = 'Eligible'
            AND missing_offense_flag_eligibility = 'Eligible'
            AND young_victim_flag_eligibility = 'Eligible'
            AND dui_last_5_years_flag_eligibility = 'Eligible'
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
            AND spe_conditions_not_overdue_flag = 1
            AND domestic_flag_eligibility = 'Eligible'
            AND sex_offense_flag_eligibility = 'Eligible'
            AND assaultive_offense_flag_eligibility in ('Eligible')
            AND maybe_assaultive_flag_eligibility in ('Eligible')
            AND unknown_offense_flag_eligibility in ('Eligible')
            AND missing_offense_flag_eligibility in ('Eligible')
            AND young_victim_flag_eligibility = 'Eligible'
            AND dui_last_5_years_flag_eligibility = 'Eligible'
            -- These flags can be null if sentencing info is missing or if drug_offense = 1 or drug_offense = 0, respectively
            AND COALESCE(drug_screen_pass_flag_non_drug_offense,1) = 1
            AND COALESCE(drug_screen_pass_flag_drug_offense,1) = 1
            AND missing_sent_info = 0
            AND cr_not_previous_rejected_flag = 1
            -- AND cr_not_rejected_x_months_flag = 1
            AND no_lifetime_flag = 1
            -- The logic here is the following: violations on diversion/probation can change someone expiration date, and we have reason to 
            -- think that there are often a lot of judgement orders/revocation orders that affect these supervision types that don't 
            -- appear in the sentencing data (or appear with a major lag). Zero tolerance codes are used as a proxy to conservatively
            -- exclude people from compliant reporting and overdue for discharge, with the assumption that there might be more to their 
            -- sentence that we don't know about. Since parole information is generally more reliable and updated, we don't apply that same
            -- filter to people on parole
            AND (no_zero_tolerance_codes = 1 OR Case_Type LIKE '%PAROLE%')
            AND eligible_counties = 1
        THEN 1 ELSE 0 END AS all_eligible,
        CASE WHEN eligible_supervision_level = 1
            AND time_on_level_flag_adj = 1 
            AND no_serious_sanctions_flag = 1 
            AND spe_conditions_not_overdue_flag = 1
            AND domestic_flag_eligibility in ('Eligible','Discretion')
            AND sex_offense_flag_eligibility in ('Eligible','Discretion')
            AND assaultive_offense_flag_eligibility in ('Eligible','Discretion')
            AND maybe_assaultive_flag_eligibility in ('Eligible','Discretion')
            AND unknown_offense_flag_eligibility in ('Eligible','Discretion')
            AND missing_offense_flag_eligibility in ('Eligible','Discretion')
            AND young_victim_flag_eligibility in ('Eligible','Discretion')
            AND dui_last_5_years_flag_eligibility = 'Eligible'
            -- These flags can be null if sentencing info is missing or if drug_offense = 1 or drug_offense = 0, respectively
            AND COALESCE(drug_screen_pass_flag_non_drug_offense,1) = 1
            AND COALESCE(drug_screen_pass_flag_drug_offense,1) = 1
            AND missing_sent_info = 0
            AND cr_not_previous_rejected_flag = 1
            -- AND cr_not_rejected_x_months_flag = 1
            AND no_lifetime_flag = 1
            -- The logic here is the following: violations on diversion/probation can change someone expiration date, and we have reason to 
            -- think that there are often a lot of judgement orders/revocation orders that affect these supervision types that don't 
            -- appear in the sentencing data (or appear with a major lag). Zero tolerance codes are used as a proxy to conservatively
            -- exclude people from compliant reporting and overdue for discharge, with the assumption that there might be more to their 
            -- sentence that we don't know about. Since parole information is generally more reliable and updated, we don't apply that same
            -- filter to people on parole
            AND (no_zero_tolerance_codes = 1 OR Case_Type LIKE '%PAROLE%')
            AND eligible_counties = 1
        THEN 1 ELSE 0 END AS all_eligible_and_discretion
    FROM add_more_flags    
    LEFT JOIN contacts_cte_arrays 
        USING(Offender_ID)
    LEFT JOIN `{project_id}.state.state_person_external_id` pei
        ON pei.external_id = Offender_ID
    LEFT JOIN `{project_id}.state.state_person` sp
        ON sp.person_id = pei.person_id
    LEFT JOIN person_status_cte
        USING(Offender_ID)
"""

US_TN_COMPLIANT_REPORTING_LOGIC_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_TN_COMPLIANT_REPORTING_LOGIC_VIEW_NAME,
    view_query_template=US_TN_COMPLIANT_REPORTING_LOGIC_QUERY_TEMPLATE,
    description=US_TN_COMPLIANT_REPORTING_LOGIC_DESCRIPTION,
    hardcoded_prod_project_id="recidiviz-123",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_COMPLIANT_REPORTING_LOGIC_VIEW_BUILDER.build_and_print()
