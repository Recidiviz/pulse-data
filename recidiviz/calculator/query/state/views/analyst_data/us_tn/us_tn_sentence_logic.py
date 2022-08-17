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
    -- The next two CTEs take the maximum of the offense type flag for a given person, first for all sentences, then for all prior sentences, and
    -- then for all sentences not marked inactive or prior
    -- According to the policy, people are ineligible if they are currently serving a sentence for certain offenses
    -- If they served a sentence for those offenses historically, the District Director can still approve them for CR on a case by case basis
    -- If they served a sentence for those offenses historically and those sentences expired 10 years or more ago, the DD process is waived
    -- Here we create flags for whether a person ever had certain offenses, and also flags if all those offenses expired more than 10 years ago
    WITH all_sentences AS (
        SELECT 
            person_id,
            COALESCE(MAX(expiration_date),MAX(full_expiration_date)) AS sentence_expiration_date_internal, 
            MAX(CASE WHEN sentence_source IN ('SENTENCE','DIVERSION') AND sentence_status != 'IN' THEN 1 ELSE 0 END) AS has_TN_sentence,
            MAX(CASE WHEN expiration_date IS NULL AND full_expiration_date IS NULL THEN 1 ELSE 0 END) AS missing_at_least_1_exp_date,
            max(missing_offense) AS missing_offense_ever,
            max(case when missing_offense = 1 then (case when expiration_date <= DATE_SUB(current_date('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) end) AS all_missing_offenses_expired,
            max(drug_offense) AS drug_offense_ever,
            max(domestic_flag) AS domestic_flag_ever,
            max(case when domestic_flag = 1 then (case when expiration_date <= DATE_SUB(current_date('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) end) AS all_domestic_offenses_expired,
            max(sex_offense_flag) AS sex_offense_flag_ever,
            max(case when sex_offense_flag = 1 then (case when expiration_date <= DATE_SUB(current_date('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) end) AS all_sex_offenses_expired,
            max(assaultive_offense_flag) AS assaultive_offense_flag_ever,
            max(case when assaultive_offense_flag = 1 then (case when expiration_date <= DATE_SUB(current_date('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) end) AS all_assaultive_offenses_expired,
            max(young_victim_flag) AS young_victim_flag_ever,
            max(case when young_victim_flag = 1 then (case when expiration_date <= DATE_SUB(current_date('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) end) AS all_young_victim_offenses_expired,
            #max(dui_last_5_years) AS dui_last_5_years_flag,
            max(case when DATE_DIFF(CURRENT_DATE,offense_date,YEAR) <5 AND dui_flag = 1 THEN 1 ELSE 0 END) as dui_last_5_years_flag,
            max(maybe_assaultive_flag) AS maybe_assaultive_flag_ever,
            max(case when maybe_assaultive_flag = 1 then (case when expiration_date <= DATE_SUB(current_date('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) end) AS all_maybe_assaultive_offenses_expired,
            max(unknown_offense_flag) AS unknown_offense_flag_ever,
            max(homicide_flag) AS homicide_flag_ever,
            max(case when unknown_offense_flag = 1 then (case when expiration_date <= DATE_SUB(current_date('US/Eastern'),INTERVAL 10 YEAR) then 1 else 0 end) end) AS all_unknown_offenses_expired,
            max(case when domestic_flag = 1 OR sex_offense_flag = 1 OR assaultive_offense_flag = 1 OR dui_flag = 1 OR young_victim_flag = 1 OR maybe_assaultive_flag = 1 then expiration_date END) AS latest_expiration_date_for_excluded_offenses,
            /* If there are any lifetime supervision or sentence flags, across all sentences, then no_lifetime_flag = 0 */
            min(COALESCE(no_lifetime_flag,1)) AS no_lifetime_flag,
            ARRAY_AGG(offense_description IGNORE NULLS) AS lifetime_offenses,
        FROM `{project_id}.{analyst_dataset}.us_tn_cr_raw_sentence_preprocessing_materialized`
        WHERE sentence_source != 'PriorRecord'
        GROUP BY 1
    ),
    prior_sentences AS (
        SELECT 
            person_id,
            max(missing_offense) AS missing_offense_prior,
            max(drug_offense) AS drug_offense_prior,
            max(domestic_flag) AS domestic_flag_prior,
            max(sex_offense_flag) AS sex_offense_flag_prior,
            max(assaultive_offense_flag) AS assaultive_offense_flag_prior,
            max(young_victim_flag) AS young_victim_flag_prior,
            max(homicide_flag) AS homicide_flag_prior,
            max(case when DATE_DIFF(CURRENT_DATE,offense_date,YEAR) <5 AND dui_flag = 1 THEN 1 ELSE 0 END) AS dui_last_5_years_prior,
            max(maybe_assaultive_flag) AS maybe_assaultive_flag_prior,
            max(unknown_offense_flag) AS unknown_offense_flag_prior,
            ARRAY_AGG(offense_description IGNORE NULLS) AS prior_offenses,
        FROM `{project_id}.{analyst_dataset}.us_tn_cr_raw_sentence_preprocessing_materialized`
        WHERE sentence_status = 'Prior'
        GROUP BY 1
    ),
    active_sentences AS (
        SELECT 
            person_id,
            max(missing_offense) AS missing_offense,
            max(drug_offense) AS drug_offense,
            max(domestic_flag) AS domestic_flag,
            max(sex_offense_flag) AS sex_offense_flag,
            max(dui_flag) as dui_flag,
            max(assaultive_offense_flag) AS assaultive_offense_flag,
            max(young_victim_flag) AS young_victim_flag,
            max(homicide_flag) AS homicide_flag,
            max(maybe_assaultive_flag) AS maybe_assaultive_flag,
            max(unknown_offense_flag) AS unknown_offense_flag,
            ARRAY_AGG(offense_description IGNORE NULLS) AS active_offenses,
            -- This ensures only docket numbers for active sentences are shown. If there are no active sentences, this is left as null
            ARRAY_AGG(docket_number IGNORE NULLS) AS docket_numbers,
        FROM `{project_id}.{analyst_dataset}.us_tn_cr_raw_sentence_preprocessing_materialized`
        WHERE sentence_status not in ('IN','Prior')
        GROUP BY 1
    ),
    -- if you have no active sentences or diversions, take the latest start date as your most recent start date
    sentence_start_for_inactive AS (
        SELECT person_id,
                has_active_sentence,
                MAX(sentence_effective_date) AS sentence_start_date,
        FROM `{project_id}.{analyst_dataset}.us_tn_cr_raw_sentence_preprocessing_materialized`
        WHERE has_active_sentence = 0
        GROUP BY 1,2
    ),
    -- if you have an active sentences or diversion, take the earliest start date of your active sentences as your most recent start date
    sentence_start_for_active AS (
        SELECT person_id,
                has_active_sentence,
                MIN(sentence_effective_date) AS sentence_start_date,
        FROM `{project_id}.{analyst_dataset}.us_tn_cr_raw_sentence_preprocessing_materialized`
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
            active_sentences.* EXCEPT(person_id),
            prior_sentences.* EXCEPT(person_id),
            union_start_dates.sentence_start_date,
            union_start_dates.has_active_sentence,
            judicial_district_code AS judicial_district,
            conviction_county,
            /* If any of these flags are 1, a person isn't eligible at all */ 
            CASE 
                WHEN GREATEST(domestic_flag, sex_offense_flag, assaultive_offense_flag, young_victim_flag, dui_flag, dui_last_5_years_flag, homicide_flag_ever, COALESCE(homicide_flag_prior,0)) = 1 THEN 0
                ELSE 1 END AS eligible_offense,
            
            /* These flags determine if there is discretion involved.
                - If any of the prior (except missing) is 1, then discretion is needed
                - If any of the other flags is 1 AND they're not all expired, then discretion is needed
                - Else, no discretion needed
            */
            CASE 
                WHEN GREATEST(COALESCE(domestic_flag_prior,0), 
                             COALESCE(sex_offense_flag_prior,0), 
                             COALESCE(assaultive_offense_flag_prior,0), 
                             COALESCE(maybe_assaultive_flag_prior,0), 
                             COALESCE(unknown_offense_flag_prior,0), 
                             COALESCE(young_victim_flag_prior,0)) = 1 THEN 1
                WHEN GREATEST(domestic_flag_ever, sex_offense_flag_ever, assaultive_offense_flag_ever, maybe_assaultive_flag_ever, unknown_offense_flag_ever, missing_offense_ever, young_victim_flag_ever) = 1
                THEN (
                    CASE WHEN LEAST(COALESCE(all_domestic_offenses_expired,1), 
                                    COALESCE(all_sex_offenses_expired,1),
                                    COALESCE(all_assaultive_offenses_expired,1),
                                    COALESCE(all_maybe_assaultive_offenses_expired,1),
                                    COALESCE(all_unknown_offenses_expired,1),
                                    COALESCE(all_missing_offenses_expired,1),
                                    COALESCE(all_young_victim_offenses_expired,1)
                                    ) = 0 
                        THEN 1
                        ELSE 0
                        END
                )
                ELSE 0 END as eligible_offense_discretion, 
             -- Want 1 Offense Type Eligibility Flag that takes on values 0, 1, or 2. 0 is ineligible, 1 is C1, and 2 is C2
             
             /* These flags are retained to later be able to pull an array of lifetime offenses that are expired to display */   
            CASE WHEN GREATEST(domestic_flag,domestic_flag_prior) = 0 AND domestic_flag_ever = 1 AND all_domestic_offenses_expired = 1 THEN 'Eligible - Expired'
                 END AS domestic_flag_eligibility,
            CASE WHEN GREATEST(sex_offense_flag,sex_offense_flag_prior) = 0 AND sex_offense_flag_ever = 1 AND all_sex_offenses_expired = 1 THEN 'Eligible - Expired'
                 END AS sex_offense_flag_eligibility,
            CASE WHEN GREATEST(assaultive_offense_flag,assaultive_offense_flag_prior) = 0 AND assaultive_offense_flag_ever = 1 AND all_assaultive_offenses_expired = 1 THEN 'Eligible - Expired'
                 END AS assaultive_offense_flag_eligibility,
            CASE WHEN GREATEST(maybe_assaultive_flag,maybe_assaultive_flag_prior) = 0 AND maybe_assaultive_flag_ever = 1 AND all_maybe_assaultive_offenses_expired = 1 THEN 'Eligible - Expired'
                 END AS maybe_assaultive_flag_eligibility,
            CASE WHEN GREATEST(unknown_offense_flag,unknown_offense_flag_prior) = 0 AND unknown_offense_flag_ever = 1 AND all_unknown_offenses_expired = 1 THEN 'Eligible - Expired'
                 END AS unknown_offense_flag_eligibility,
            CASE WHEN GREATEST(missing_offense,missing_offense_prior) = 0 AND missing_offense_ever = 1 AND all_missing_offenses_expired = 1 THEN 'Eligible - Expired'
                 END AS missing_offense_flag_eligibility,
            CASE WHEN GREATEST(young_victim_flag,young_victim_flag_prior) = 0 AND young_victim_flag_ever = 1 AND all_young_victim_offenses_expired = 1 THEN 'Eligible - Expired'
                 END AS young_victim_flag_eligibility,
    FROM all_sentences 
    LEFT JOIN active_sentences 
        USING(person_id)
    LEFT JOIN prior_sentences 
        USING(person_id)
    LEFT JOIN union_start_dates
        USING(person_id)
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
            person_id,
            STRING_AGG(
                CASE WHEN Decode is not null then CONCAT(conviction_county, ' - ', Decode) 
                 ELSE conviction_county END, ', '
                  ) AS conviction_county,
        FROM `{project_id}.{analyst_dataset}.us_tn_cr_raw_sentence_preprocessing_materialized`
        LEFT JOIN (
            SELECT *
            FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.CodesDescription_latest`
            WHERE CodesTableID = 'TDPD130'
        ) codes 
            ON conviction_county = codes.Code
        -- This limits to only pulling conviction county for active sentences
        WHERE sentence_status not in ('IN','Prior')
        GROUP BY 1
    )
    USING(person_id)
"""

US_TN_SENTENCE_LOGIC_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_SENTENCE_LOGIC_VIEW_NAME,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    description=US_TN_SENTENCE_LOGIC_VIEW_DESCRIPTION,
    view_query_template=US_TN_SENTENCE_LOGIC_QUERY_TEMPLATE,
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region("us_tn"),
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_SENTENCE_LOGIC_VIEW_BUILDER.build_and_print()
