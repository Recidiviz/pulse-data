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
"""Creates a view to wrangle sentencing data for Compliant Reporting eligibility"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_SENTENCE_LOGIC_VIEW_NAME = "us_tn_sentence_logic"

US_TN_SENTENCE_LOGIC_VIEW_DESCRIPTION = (
    """Creates a view to wrangle sentencing data for Compliant Reporting eligibility"""
)

US_TN_SENTENCE_LOGIC_QUERY_TEMPLATE = """
    -- Sentencing data lives in 4 places: Sentence, Diversion, PriorRecord, and ISCSentence (out of state sentence) 
    -- The first three have similar structures, so I union them and then flag ineligible offenses
    -- ISC Sentence has a different structure and very different offense type strings since these sentences are from all over the country
    -- The general wrangling here is to :
    -- 1) Flag ineligible offenses across all 3 datasets
    -- 2) Flag active and inactive sentences (not applicable for PriorRecord)
    -- 3) Use 1) and 2) to compute current offenses, lifetime offenses, max expiration date, and most recent start date
    -- The offense type criteria is:
        -- Offense type not domestic abuse or sexual assault
        -- Offense type not DUI in past 5 years
        -- Offense type not crime against person that resulted in physical bodily harm
        -- Offense type not crime where victim was under 18. Note, I did not see any OffenseDescriptions referencing a victim aged 
        -- less than 18, but did see some for people under 13, so that's what I've included 
    WITH diversion AS (
        SELECT OffenderID AS offender_id,
              OffenseDescription AS offense_description,
              CAST(CAST(ExpirationDate AS DATETIME) AS DATE) AS expiration_date,
              CAST(CAST(ExpirationDate AS DATETIME) AS DATE) AS full_expiration_date,
              CAST(CAST(DiversionGrantedDate AS DATETIME) AS DATE) AS sentence_effective_date,
              CASE WHEN DiversionStatus = 'C' THEN 'IN' ELSE 'AC' END AS sentence_status,
              CAST(CAST(DiversionGrantedDate AS DATETIME) AS DATE) AS offense_date,
              CAST(ConvictionCounty AS STRING) AS conviction_county,
              CaseNumber AS docket_number,
              'DIVERSION' AS sentence_source,
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.Diversion_latest` d
        LEFT JOIN `{project_id}.{us_tn_raw_data_up_to_date_dataset}.OffenderStatute_latest`
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
              case_number AS docket_number,
              'SENTENCE' AS sentence_source,
        FROM `{project_id}.{sessions_dataset}.us_tn_sentences_preprocessed_materialized`
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
                DocketNumber AS docket_number,
                'PriorRecord' AS sentence_source,
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.PriorRecord_latest` pr
        LEFT JOIN `{project_id}.{us_tn_raw_data_up_to_date_dataset}.OffenderStatute_latest` ofs
            ON COALESCE(pr.ConvictionOffense, pr.ChargeOffense) = ofs.Offense
        -- This avoids double counting offenses when surfaced on front end since prior record has ~10% entries autogenerated from TN sentences data
        -- and we're capturing that with Sentence and Diversion tables
        WHERE DispositionText NOT LIKE 'PRIOR RECORD AUTO CREATED%'
    ),
    isc_sent AS (
        SELECT OffenderID AS Offender_ID,
                ConvictedOffense AS offense_description,
                CAST(CAST(ExpirationDate AS DATETIME) AS DATE) AS expiration_date,
                CAST(CAST(ExpirationDate AS DATETIME) AS DATE) AS full_expiration_date,
                CAST(CAST(SentenceImposedDate AS DATETIME) AS DATE) AS sentence_effective_date,
                CASE WHEN CAST(CAST(ExpirationDate AS DATETIME) AS DATE) < current_date('US/Eastern') THEN 'IN' ELSE 'AC' END AS sentence_status,
                CAST(CAST(OffenseDate AS DATETIME) AS DATE) AS offense_date,
                Jurisdiction AS conviction_county,
                CaseNumber AS docket_number,
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
                    OR ConvictedOffense LIKE '%HEROIN%'
                    OR ConvictedOffense LIKE '%CONT. SUBSTANCE%'
                    OR ConvictedOffense LIKE '%MARIJUANA%'
                        THEN 1 ELSE 0 END AS drug_offense,
                    CASE WHEN ConvictedOffense LIKE '%DOMESTIC%' THEN 1 ELSE 0 END AS domestic_flag,
                    CASE WHEN ConvictedOffense LIKE '%SEX%' OR ConvictedOffense LIKE '%RAPE%' THEN 1 ELSE 0 END AS sex_offense_flag,
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
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.ISCSentence_latest`
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
                        OR offense_description LIKE '%HEROIN%'
                    THEN 1 ELSE 0 END AS drug_offense,
            CASE WHEN offense_description LIKE '%DOMESTIC%' THEN 1 ELSE 0 END AS domestic_flag,
            CASE WHEN ((offense_description LIKE '%SEX%' OR offense_description LIKE '%RAPE%') AND offense_description NOT LIKE '%REGIS%') THEN 1 ELSE 0 END AS sex_offense_flag,
            CASE WHEN DATE_DIFF(CURRENT_DATE,offense_date,YEAR) <5 AND (offense_description LIKE '%DUI%' OR offense_description LIKE '%INFLUENCE%') THEN 1 ELSE 0 END AS dui_last_5_years,
            0 AS maybe_assaultive_flag,
            CASE WHEN (COALESCE(AssaultiveOffenseFlag,'N') = 'Y') THEN 1 ELSE 0 END AS assaultive_offense_flag,
            0 AS unknown_offense_flag,
            CASE WHEN (offense_description LIKE '%13%' AND offense_description LIKE '%VICT%' ) THEN 1 ELSE 0 END AS young_victim_flag,
        FROM sent_union_diversion
        LEFT JOIN 
            (
            SELECT OffenseDescription, AssaultiveOffenseFlag
            FROM  `{project_id}.{us_tn_raw_data_up_to_date_dataset}.OffenderStatute_latest` 
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
    -- The next two CTEs take the maximum of the offense type flag for a given person, first for all sentences, then for all prior sentences, and
    -- then for all sentences not marked inactive or prior
    -- According to the policy, people are ineligible if they are currently serving a sentence for certain offenses
    -- If they served a sentence for those offenses historically, the District Director can still approve them for CR on a case by case basis
    -- If they served a sentence for those offenses historically and those sentences expired 10 years or more ago, the DD process is waived
    -- Here we create flags for whether a person ever had certain offenses, and also flags if all those offenses expired more than 10 years ago
    all_sentences AS (
        SELECT 
            Offender_ID,
            COALESCE(MAX(expiration_date),MAX(full_expiration_date)) AS sentence_expiration_date_internal, 
            MAX(CASE WHEN sentence_source IN ('SENTENCE','DIVERSION') AND sentence_status != 'IN' THEN 1 ELSE 0 END) AS has_TN_sentence,
            MAX(CASE WHEN expiration_date IS NULL AND full_expiration_date IS NULL THEN 1 ELSE 0 END) AS missing_at_least_1_exp_date,
            max(missing_offense) AS missing_offense_ever,
            max(case when missing_offense = 1 and expiration_date <= DATE_SUB(current_date('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) AS all_missing_offenses_expired,
            max(drug_offense) AS drug_offense_ever,
            max(domestic_flag) AS domestic_flag_ever,
            max(case when domestic_flag = 1 and expiration_date <= DATE_SUB(current_date('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) AS all_domestic_offenses_expired,
            max(sex_offense_flag) AS sex_offense_flag_ever,
            max(case when sex_offense_flag = 1 and expiration_date <= DATE_SUB(current_date('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) AS all_sex_offenses_expired,
            max(assaultive_offense_flag) AS assaultive_offense_flag_ever,
            max(case when assaultive_offense_flag = 1 and expiration_date <= DATE_SUB(current_date('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) AS all_assaultive_offenses_expired,
            max(young_victim_flag) AS young_victim_flag_ever,
            max(case when young_victim_flag = 1 and expiration_date <= DATE_SUB(current_date('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) AS all_young_victim_offenses_expired,
            max(dui_last_5_years) AS dui_last_5_years_flag,
            max(maybe_assaultive_flag) AS maybe_assaultive_flag_ever,
            max(case when maybe_assaultive_flag = 1 and expiration_date <= DATE_SUB(current_date('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) AS all_maybe_assaultive_offenses_expired,
            max(unknown_offense_flag) AS unknown_offense_flag_ever,
            max(case when unknown_offense_flag = 1 and expiration_date <= DATE_SUB(current_date('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) AS all_unknown_offenses_expired,
            max(case when domestic_flag = 1 OR sex_offense_flag = 1 OR assaultive_offense_flag = 1 OR young_victim_flag = 1 OR dui_last_5_years = 1 OR maybe_assaultive_flag = 1 then expiration_date END) AS latest_expiration_date_for_excluded_offenses,
            ARRAY_AGG(offense_description IGNORE NULLS) AS lifetime_offenses,
            ARRAY_AGG(docket_number IGNORE NULLS) AS docket_numbers,
        FROM sent_union_isc
        WHERE sentence_source != 'PriorRecord'
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
    )
    -- Put it all together to get offense type flags for each person, pulls the last known judicial district,
    -- and the last available conviction county for all sentences not marked inactive
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
    LEFT JOIN `{project_id}.{state_base_dataset}.state_person_external_id` pei
        ON all_sentences.Offender_ID = pei.external_id
    LEFT JOIN (
        SELECT person_id, judicial_district_code
        FROM `{project_id}.{sessions_dataset}.us_tn_judicial_district_sessions_materialized`
        WHERE judicial_district_end_date IS NULL
        -- the where clause should remove duplicates on person id but for 0.5% of cases this doesnt happen so the qualify statement picks the JD
        -- associated with the latest SED
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY judicial_district_start_date DESC) = 1
    ) jd
        USING(person_id)
    LEFT JOIN (
        SELECT 
            Offender_ID,
            CASE WHEN Decode is not null then CONCAT(conviction_county, ' - ', Decode) 
                 ELSE conviction_county END AS conviction_county,
        FROM sent_union_isc
        LEFT JOIN (
            SELECT *
            FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.CodesDescription_latest`
            WHERE CodesTableID = 'TDPD130'
        ) codes 
            ON conviction_county = codes.Code
        WHERE TRUE
        QUALIFY ROW_NUMBER() OVER(partition by Offender_ID ORDER BY sentence_effective_date DESC) = 1 
    )
        USING(Offender_ID)
"""

US_TN_SENTENCE_LOGIC_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_SENTENCE_LOGIC_VIEW_NAME,
    description=US_TN_SENTENCE_LOGIC_VIEW_DESCRIPTION,
    view_query_template=US_TN_SENTENCE_LOGIC_QUERY_TEMPLATE,
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region("us_tn"),
    sessions_dataset=SESSIONS_DATASET,
    state_base_dataset=STATE_BASE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_SENTENCE_LOGIC_VIEW_BUILDER.build_and_print()
