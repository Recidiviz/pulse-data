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
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_CR_RAW_SENTENCE_PREPROCESSING_VIEW_NAME = "us_tn_cr_raw_sentence_preprocessing"

US_TN_CR_RAW_SENTENCE_PREPROCESSING_VIEW_DESCRIPTION = "Creates a view that surfaces sessions during which clients are eligible for Compliant Reporting due to offense types, drug screening results, and other sentence-based exclusions."

US_TN_CR_RAW_SENTENCE_PREPROCESSING_QUERY_TEMPLATE = """
    /*
    Sentencing data lives in 4 places: Sentence, Diversion, PriorRecord, and ISCSentence (out of state sentence) 
    The first three have similar structures, so I union them and then flag ineligible offenses
    ISC Sentence has a different structure and very different offense type strings since these sentences are from all over the country
    The general wrangling here is to :
        1) Flag ineligible offenses across all 3 datasets
        2) Flag active and inactive sentences (not applicable for PriorRecord)
        3) Use 1) and 2) to compute current offenses, lifetime offenses, max expiration date, and most recent start date
        The offense type criteria is:
            Offense type not domestic abuse or sexual assault
            Offense type not DUI in past 5 years
            Offense type not crime against person that resulted in physical bodily harm
            Offense type not crime where victim was under 18. Note, I did not see any OffenseDescriptions referencing a victim aged 
            less than 18, but did see some for people under 13, so that's what I've included 
    */
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
        --TODO(#11448): Exclude ISCSentence, Diversion, and PriorRecord sentences when those are ingested
        SELECT
            SPLIT(external_id, '-')[OFFSET(0)] AS offender_id,
            description AS offense_description,
            completion_date AS expiration_date,
            projected_completion_date_max AS full_expiration_date,
            effective_date AS sentence_effective_date,
            status_raw_text AS sentence_status,
            offense_date,
            SPLIT(external_id, '-')[OFFSET(1)] AS conviction_county,
            SPLIT(external_id, '-')[OFFSET(3)] AS docket_number,
            'SENTENCE' AS sentence_source
        FROM `{project_id}.{sessions_dataset}.us_tn_sentences_preprocessed_materialized`
    ),
    CSL AS (
       SELECT 
            OffenderID as Offender_ID, 
            CAST(CAST(SentenceEffectiveDate AS DATETIME) AS DATE) AS sentence_effective_date,
            LifetimeSupervision, 
            LifeDeathHabitual 
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.JOSentence_latest`
        JOIN `{project_id}.{us_tn_raw_data_up_to_date_dataset}.Sentence_latest` s
        USING(OffenderID, ConvictionCounty,CaseYear,CaseNumber, CountNumber)
        WHERE TRUE
        QUALIFY ROW_NUMBER() OVER(
            PARTITION BY OffenderID 
            ORDER BY CASE 
                WHEN LifetimeSupervision = 'Y' THEN 0 ELSE 1 END,
                CASE WHEN LifeDeathHabitual IS NOT NULL THEN 0 ELSE 1 END
        ) = 1
    )
    ,
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
        /* This avoids double counting offenses when surfaced on front end since prior record has ~10% entries autogenerated from TN sentences data
        and we're capturing that with Sentence and Diversion tables */
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
                    OR ConvictedOffense LIKE '%POSS%CONT%'
                    OR ConvictedOffense LIKE '%POSS%AMPH%'
                    OR ConvictedOffense LIKE '%POSS%HEROIN%'
                    OR ConvictedOffense LIKE '%VGCSA%'
                    OR ConvictedOffense LIKE '%SIMPLE POSS%'
                    OR ConvictedOffense LIKE '%COCAINE%'
                    OR ConvictedOffense LIKE '%HEROIN%'
                    OR ConvictedOffense LIKE '%CONT. SUBSTANCE%'
                    OR ConvictedOffense LIKE '%MARIJUANA%'
                    OR ConvictedOffense LIKE '%NARCOTIC%'
                    OR ConvictedOffense LIKE '%OPIUM%'
                        THEN 1 ELSE 0 END AS drug_offense,
                
                CASE WHEN ConvictedOffense LIKE '%DOMESTIC%' THEN 1 ELSE 0 END AS domestic_flag,
                
                CASE WHEN ConvictedOffense LIKE '%SEX%' OR ConvictedOffense LIKE '%RAPE%' THEN 1 ELSE 0 END AS sex_offense_flag,
                
                CASE WHEN ConvictedOffense LIKE '%DUI%' OR ConvictedOffense LIKE '%DWI%' OR ConvictedOffense LIKE '%INFLUENCE%' THEN 1 ELSE 0 END AS dui_flag,
                
                #CASE WHEN DATE_DIFF(CURRENT_DATE,CAST(CAST(OffenseDate AS DATETIME) AS DATE),YEAR) <5 AND (ConvictedOffense LIKE '%DUI%' OR ConvictedOffense LIKE '%DWI%' OR ConvictedOffense LIKE '%INFLUENCE%') THEN 1 ELSE 0 END AS dui_last_5_years,
                
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
                
                CASE WHEN (ConvictedOffense LIKE '%HOMICIDE%' AND ConvictedOffense LIKE '%MURDER%' ) THEN 1 ELSE 0 END AS homicide_flag,
                
                CASE WHEN sentence LIKE '%LIFE%' THEN 0 ELSE 1 END AS no_lifetime_flag,
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.ISCSentence_latest`
    )
    ,
    sent_union_diversion AS (
        SELECT *
        FROM sentences
    
        UNION ALL
        
        SELECT *
        FROM diversion
        
        UNION ALL
        
        SELECT *
        FROM prior_record
    )
    ,
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
            
            CASE WHEN offense_description LIKE '%DUI%' OR offense_description LIKE '%INFLUENCE%' THEN 1 ELSE 0 END AS dui_flag,
            
            #CASE WHEN DATE_DIFF(CURRENT_DATE,offense_date,YEAR) <5 AND (offense_description LIKE '%DUI%' OR offense_description LIKE '%INFLUENCE%') THEN 1 ELSE 0 END AS dui_last_5_years,
            
            0 AS maybe_assaultive_flag,
            
            CASE WHEN (COALESCE(AssaultiveOffenseFlag,'N') = 'Y') THEN 1 ELSE 0 END AS assaultive_offense_flag,
            
            0 AS unknown_offense_flag,
            
            CASE WHEN (offense_description LIKE '%13%' AND offense_description LIKE '%VICT%' ) THEN 1 ELSE 0 END AS young_victim_flag,
            
            CASE WHEN (offense_description LIKE '%HOMICIDE%' AND offense_description LIKE '%MURDER%' ) THEN 1 ELSE 0 END AS homicide_flag,
            
            CASE WHEN COALESCE(LifetimeSupervision,'N') != 'Y' AND COALESCE(LifeDeathHabitual,'N') = 'N' THEN 1 ELSE 0 END AS no_lifetime_flag,
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
        LEFT JOIN CSL
            USING(offender_id, sentence_effective_date)
    )
    SELECT a.* EXCEPT (sentence_effective_date),
            COALESCE(sentence_effective_date, '1900-01-01') AS sentence_effective_date,
        MAX(CASE WHEN sentence_status NOT IN ('IN','Prior') THEN 1 ELSE 0 END) OVER(PARTITION BY offender_id) AS has_active_sentence,
        pei.person_id
    FROM (
        SELECT * 
        FROM sentence_flags
        UNION ALL 
        SELECT *
        FROM isc_sent
    ) a
    INNER JOIN `{project_id}.{base_dataset}.state_person_external_id` pei
        ON a.Offender_ID = pei.external_id
        AND pei.state_code = 'US_TN' 
     
"""

US_TN_CR_RAW_SENTENCE_PREPROCESSING_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    base_dataset=STATE_BASE_DATASET,
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        StateCode.US_TN.value
    ),
    view_id=US_TN_CR_RAW_SENTENCE_PREPROCESSING_VIEW_NAME,
    description=US_TN_CR_RAW_SENTENCE_PREPROCESSING_VIEW_DESCRIPTION,
    view_query_template=US_TN_CR_RAW_SENTENCE_PREPROCESSING_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_CR_RAW_SENTENCE_PREPROCESSING_VIEW_BUILDER.build_and_print()
