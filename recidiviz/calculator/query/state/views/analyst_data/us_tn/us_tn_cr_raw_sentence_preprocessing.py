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
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_CR_RAW_SENTENCE_PREPROCESSING_VIEW_NAME = "us_tn_cr_raw_sentence_preprocessing"

US_TN_CR_RAW_SENTENCE_PREPROCESSING_VIEW_DESCRIPTION = "Creates a view that surfaces sessions during which clients are eligible for Compliant Reporting due to offense types, drug screening results, and other sentence-based exclusions."

US_TN_CR_RAW_SENTENCE_PREPROCESSING_QUERY_TEMPLATE = """
    /*
    Sentencing data lives in 4 places: Sentence, Diversion, ISCSentence (out of state sentence), and PriorRecord.
    The first three are ingested into our state schema, PriorRecord is not. ISCSentences have different Offense Type strings
    so when flagging ineligible offenses, we do it differently based on sentence source. 
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
    -- TODO(#18364): Remove sentence metadata hacky fixes once #18148 merged in
    WITH sentences AS (
        SELECT
            person_id,
            description AS offense_description,
            completion_date AS expiration_date,
            projected_completion_date_max AS full_expiration_date,
            effective_date AS sentence_effective_date,
            status_raw_text AS sentence_status,
            offense_date,
            life_sentence,
            is_violent,
            county_code AS conviction_county,
            JSON_EXTRACT_SCALAR(sentence_metadata, '$.CASE_NUMBER') AS docket_number,
            JSON_EXTRACT_SCALAR(sentence_metadata, '$.SENTENCE_SOURCE') AS sentence_source,
        FROM `{project_id}.{sessions_dataset}.us_tn_sentences_preprocessed_materialized`
    ),
    prior_record AS (
        SELECT pei.person_id,
                OffenseDescription AS offense_description,
                CAST(CAST(DispositionDate AS DATETIME) AS DATE) AS expiration_date,
                CAST(CAST(DispositionDate AS DATETIME) AS DATE) AS full_expiration_date,
                CAST(CAST(ArrestDate AS DATETIME) AS DATE) AS sentence_effective_date,
                'Prior' AS sentence_status,
                CAST(CAST(OffenseDate AS DATETIME) AS DATE) AS offense_date,
                FALSE AS life_sentence,
                -- placeholder for union to work, we bring in this info from raw data later
                FALSE AS is_violent,
                CourtName AS conviction_county,
                DocketNumber AS docket_number,
                'PriorRecord' AS sentence_source,
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.PriorRecord_latest` pr
        LEFT JOIN `{project_id}.{us_tn_raw_data_up_to_date_dataset}.OffenderStatute_latest` ofs
            ON COALESCE(pr.ConvictionOffense, pr.ChargeOffense) = ofs.Offense
        LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
            ON pr.OffenderID = pei.external_id
            AND pei.state_code = 'US_TN' 
        /* This avoids double counting offenses when surfaced on front end since prior record has ~10% entries autogenerated from TN sentences data
        and we're capturing that with Sentence and Diversion tables */
        WHERE DispositionText NOT LIKE 'PRIOR RECORD AUTO CREATED%'
    ),
    sent_union_diversion AS (
        SELECT *
        FROM sentences
    
        UNION ALL
        
        SELECT *
        FROM prior_record
    )
    ,
    sentence_flags AS (
        SELECT sent_union_diversion.*,
           
           CASE WHEN COALESCE(offense_description,'NOT DEFINED') = 'NOT DEFINED' THEN 1 ELSE 0 END missing_offense,
           
           CASE WHEN sentence_source != "ISC" THEN (
                CASE WHEN (offense_description LIKE '%DRUG%' AND offense_description NOT LIKE '%ADULTERATION%')
                        OR offense_description LIKE '%METH%'
                        OR offense_description LIKE '%POSS%CONT%'
                        OR offense_description LIKE '%POSS%AMPH%'
                        OR offense_description LIKE '%POSS%HEROIN%'
                        OR offense_description LIKE '%VGCSA%'
                        OR offense_description LIKE '%SIMPLE POSS%'
                        OR offense_description LIKE '%COCAINE%'
                        OR offense_description LIKE '%HEROIN%'
                        OR offense_description LIKE '%CONT. SUBSTANCE%'
                        OR offense_description LIKE '%MARIJUANA%'
                        OR offense_description LIKE '%NARCOTIC%'
                        OR offense_description LIKE '%OPIUM%'
                            THEN 1 
                        ELSE 0 
                        END
           ) WHEN COALESCE(DrugOffensesFlag,'N') = 'Y' THEN 1 
           ELSE 0 END AS drug_offense,
                       
            CASE WHEN offense_description LIKE '%DOMESTIC%' THEN 1 ELSE 0 END AS domestic_flag,
            
            -- TODO(#18509): Replace with ingested `is_sex_offense` flag for all but PriorRecord when ingested for ISC/Diversion
            CASE WHEN sentence_source != 'ISC' AND (COALESCE(SexOffenderFlag,'N') = 'Y') THEN 1 
                 WHEN sentence_source = 'ISC' AND (offense_description LIKE '%SEX%' OR offense_description LIKE '%RAPE%') THEN 1
                 ELSE 0 END AS sex_offense_flag,
            
            CASE WHEN offense_description LIKE '%DUI%' OR 
                      offense_description LIKE '%INFLUENCE%' OR
                      offense_description LIKE '%DWI%' 
                      THEN 1 
                      ELSE 0 END AS dui_flag,
            
             CASE WHEN sentence_source = 'ISC' AND ( 
                        offense_description LIKE '%BURGLARY%' 
                        OR offense_description LIKE '%ROBBERY%'
                        OR offense_description LIKE '%ARSON%')
                 THEN 1 ELSE 0 END AS maybe_assaultive_flag,
            
            CASE WHEN sentence_source != 'PriorRecord' AND is_violent THEN 1
                 WHEN sentence_source = 'PriorRecord' AND COALESCE(AssaultiveOffenseFlag,'N') = 'Y' THEN 1
                 ELSE 0 END AS assaultive_offense_flag,
            
            CASE WHEN sentence_source = 'ISC' THEN (
                CASE WHEN offense_description LIKE '%THEFT%' 
                        OR offense_description LIKE '%LARCENY%'
                        OR offense_description LIKE '%FORGERY%'
                        OR offense_description LIKE '%FRAUD%'
                        OR offense_description LIKE '%STOLE%'
                        OR offense_description LIKE '%SHOPLIFTING%'
                        OR offense_description LIKE '%EMBEZZLEMENT%'
                        OR offense_description LIKE '%FLAGRANT NON-SUPPORT%'
                        OR offense_description LIKE '%ESCAPE%'
                        OR offense_description LIKE '%BREAKING AND ENTERING%'
                        OR offense_description LIKE '%PROPERTY%'
                        OR offense_description LIKE '%STEAL%'
                    THEN 0 
                    ELSE 1 
                    END
                )
                ELSE 0 END AS unknown_offense_flag,
            
            CASE WHEN (offense_description LIKE '%13%' AND offense_description LIKE '%VICT%' ) THEN 1 ELSE 0 END AS young_victim_flag,
            
            CASE WHEN (offense_description LIKE '%HOMICIDE%' OR offense_description LIKE '%MURDER%' ) THEN 1 ELSE 0 END AS homicide_flag,
            
            CASE WHEN life_sentence THEN 0 ELSE 1 END AS no_lifetime_flag,
            
        FROM sent_union_diversion
        LEFT JOIN 
            (
            SELECT DISTINCT
                   OffenseDescription, 
                   FIRST_VALUE(AssaultiveOffenseFlag) OVER(PARTITION BY OffenseDescription ORDER BY CASE WHEN AssaultiveOffenseFlag = 'Y' THEN 0 ELSE 1 END) AS AssaultiveOffenseFlag,
                   FIRST_VALUE(SexOffenderFlag) OVER(PARTITION BY OffenseDescription ORDER BY CASE WHEN SexOffenderFlag = 'Y' THEN 0 ELSE 1 END) AS SexOffenderFlag,
                   FIRST_VALUE(DrugOffensesFlag) OVER(PARTITION BY OffenseDescription ORDER BY CASE WHEN DrugOffensesFlag = 'Y' THEN 0 ELSE 1 END) AS DrugOffensesFlag 
            FROM  `{project_id}.{us_tn_raw_data_up_to_date_dataset}.OffenderStatute_latest` 
            ) statute
                ON offense_description = statute.OffenseDescription 
    )
    SELECT a.* EXCEPT (sentence_effective_date),
            COALESCE(sentence_effective_date, '1900-01-01') AS sentence_effective_date,
        MAX(CASE WHEN sentence_status NOT IN ('IN','Prior') THEN 1 ELSE 0 END) OVER(PARTITION BY person_id) AS has_active_sentence,
    FROM sentence_flags a     
"""

US_TN_CR_RAW_SENTENCE_PREPROCESSING_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
    view_id=US_TN_CR_RAW_SENTENCE_PREPROCESSING_VIEW_NAME,
    description=US_TN_CR_RAW_SENTENCE_PREPROCESSING_VIEW_DESCRIPTION,
    view_query_template=US_TN_CR_RAW_SENTENCE_PREPROCESSING_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_CR_RAW_SENTENCE_PREPROCESSING_VIEW_BUILDER.build_and_print()
