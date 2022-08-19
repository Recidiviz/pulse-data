# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""State-specific preprocessing for TN raw sentence data"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
    US_TN_RAW_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_SENTENCES_PREPROCESSED_VIEW_NAME = "us_tn_sentences_preprocessed"

US_TN_SENTENCES_PREPROCESSED_VIEW_DESCRIPTION = (
    """State-specific preprocessing for TN raw sentence data"""
)

US_TN_SENTENCES_PREPROCESSED_QUERY_TEMPLATE = """
    /*{description}*/
    WITH raw_data_cte AS
    (
    SELECT 
        CONCAT(js.OffenderID, '-', js.ConvictionCounty,'-', js.CaseYear, '-', js.CaseNumber, '-', js.CountNumber) AS external_id,
        jo_id.JudicialDistrict AS judicial_district,
        s.SentenceStatus AS status_raw_text,
        NULLIF(CAST(js.MinimumSentenceYears AS INT64)*365 + CAST(js.MinimumSentenceMonths AS INT64)*30 + CAST(js.MinimumSentenceDays AS INT64),0) AS min_sentence_length_days_calculated,
        NULLIF(CAST(js.MaximumSentenceYears AS INT64)*365 + CAST(js.MaximumSentenceMonths AS INT64)*30 + CAST(js.MaximumSentenceDays AS INT64),0) AS max_sentence_length_days_calculated,
        CAST(s.TotalProgramCredits AS INT64) AS total_program_credits,
        CAST(s.TotalBehaviorCredits AS INT64) AS total_behavior_credits,
        CAST(s.TotalPPSCCredits AS INT64) AS total_ppsc_credits,        
        CAST(s.TotalGEDCredits AS INT64) total_ged_credits,
        CAST(s.TotalLiteraryCredits AS INT64) total_literary_credits,
        CAST(s.TotalDrugAlcoholCredits AS INT64) total_drug_alcohol_credits,
        CAST(s.TotalEducationAttendanceCredits AS INT64) total_education_attendance_credits,
        CAST(s.TotalTreatmentCredits AS INT64) total_treatment_credits,
    FROM `{project_id}.{raw_dataset}.JOSentence_latest` js
    JOIN `{project_id}.{raw_dataset}.Sentence_latest` s
        USING(OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
    JOIN `{project_id}.{raw_dataset}.JOIdentification_latest` jo_id
        USING(OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
    )
    ,
    sentences_cte AS 
    (
    /*
    Unions together incarceration and supervision sentences from the ingested data, joins to raw data to pull in specific fields that we 
    are not currently ingesting, renames some fields to fit the state-agostic schema, and joins to the state charge data to pull
    in offense type information.
    */
    SELECT 
        sis.person_id,
        sis.state_code,
        sis.incarceration_sentence_id AS sentence_id,
        sis.external_id AS external_id,
        'INCARCERATION' AS sentence_type,
        JSON_EXTRACT_SCALAR(sis.sentence_metadata, '$.CONSECUTIVE_SENTENCE_ID') AS consecutive_sentence_external_id,
        sis.start_date AS effective_date,
        sis.date_imposed,
        sis.completion_date,
        sis.status,
        sis.parole_eligibility_date,
        --TODO(#13749): Update TN projected_max_release_date logic to actually reflect the max sentence length instead
        sis.projected_min_release_date AS projected_completion_date_min,
        sis.projected_max_release_date AS projected_completion_date_max,
        sis.initial_time_served_days,
        COALESCE(sis.is_life, FALSE) AS life_sentence,
        charge.* EXCEPT(person_id, state_code, external_id, status, status_raw_text, description),
        COALESCE(charge.description, statute.OffenseDescription) AS description,
    FROM `{project_id}.{state_base_dataset}.state_incarceration_sentence` AS sis
    LEFT JOIN `{project_id}.{state_base_dataset}.state_charge_incarceration_sentence_association` assoc
        ON assoc.state_code = sis.state_code
        AND assoc.incarceration_sentence_id = sis.incarceration_sentence_id
    LEFT JOIN `{project_id}.{analyst_dataset}.state_charge_with_labels_materialized` charge
        ON charge.state_code = assoc.state_code
        AND charge.charge_id = assoc.charge_id
    LEFT JOIN `{project_id}.{raw_dataset}.OffenderStatute_latest` statute
        ON charge.statute = statute.Offense
    WHERE sis.external_id IS NOT NULL
        AND sis.state_code = 'US_TN'
           
    UNION ALL
     
    SELECT 
        sss.person_id,
        sss.state_code,
        sss.supervision_sentence_id AS sentence_id,
        sss.external_id AS external_id,
        'SUPERVISION' AS sentence_type,
        JSON_EXTRACT_SCALAR(sss.sentence_metadata, '$.CONSECUTIVE_SENTENCE_ID') AS consecutive_sentence_external_id,
        sss.start_date AS effective_date,
        sss.date_imposed,
        sss.completion_date,
        sss.status,
        NULL AS parole_eligibility_date,
        --TODO(#13749): Update TN projected_max_release_date logic to actually reflect the max sentence length instead
        sss.projected_completion_date AS projected_completion_date_min,
        sss.projected_completion_date AS projected_completion_date_max,
        CAST(NULL AS INT64) AS initial_time_served_days,
        FALSE AS life_sentence,
        charge.* EXCEPT(person_id, state_code, external_id, status, status_raw_text, description),
        COALESCE(charge.description, statute.OffenseDescription) AS description,
    FROM `{project_id}.{state_base_dataset}.state_supervision_sentence` AS sss
    LEFT JOIN `{project_id}.{state_base_dataset}.state_charge_supervision_sentence_association` assoc
        ON assoc.state_code = sss.state_code
        AND assoc.supervision_sentence_id = sss.supervision_sentence_id
    LEFT JOIN `{project_id}.{analyst_dataset}.state_charge_with_labels_materialized` charge
        ON charge.state_code = assoc.state_code
        AND charge.charge_id = assoc.charge_id
    LEFT JOIN `{project_id}.{raw_dataset}.OffenderStatute_latest` statute
        ON charge.statute = statute.Offense
    WHERE sss.external_id IS NOT NULL
        AND sss.state_code = 'US_TN'
    )
    ,
    dedup_external_id_fields AS
    --TODO(#13745): Further investigate de-duplication of sentence external id in TN sentences
    (
    /*
    In prep for deduping based on external id, this view aggregates to the external id level taking the relevant date
    fields (min for start states and max for end dates). This gets joined in to the final sentence table so that we use 
    these values instead.
    */
    SELECT
        external_id,
        MIN(effective_date) AS effective_date,
        MIN(date_imposed) AS date_imposed,
        MAX(completion_date) AS completion_date,
        MAX(parole_eligibility_date) AS parole_eligibility_date,
        MAX(projected_completion_date_min) AS projected_completion_date_min,
        MAX(projected_completion_date_max) AS projected_completion_date_max,
    FROM sentences_cte
    GROUP BY 1
    )
    /*
    Joins back to sessions to create a "session_id_imposed" field as well as back to itself to pull in the consecutive
    sentence internal id.
    */
    SELECT 
        sen.person_id,
        sen.state_code,
        sen.sentence_id,
        sen.external_id AS external_id,
        sen.sentence_type,
        COALESCE(raw.judicial_district, 'EXTERNAL_UNKNOWN') AS judicial_district,
        sen.consecutive_sentence_external_id,
        dedup.effective_date,
        dedup.date_imposed,
        dedup.completion_date,
        sen.status,
        raw.status_raw_text,
        dedup.parole_eligibility_date,
        dedup.projected_completion_date_min,
        dedup.projected_completion_date_max,
        sen.initial_time_served_days,
        sen.life_sentence,
        raw.min_sentence_length_days_calculated,
        raw.max_sentence_length_days_calculated,
        sen.charge_id,
        sen.offense_date,
        sen.is_violent,
        sen.is_sex_offense,
        COALESCE(sen.classification_type, 'EXTERNAL_UNKNOWN') AS classification_type,
        COALESCE(sen.classification_subtype, 'EXTERNAL_UNKNOWN') AS classification_subtype,
        sen.description,
        sen.offense_type,
        sen.ncic_code,
        sen.statute,
        COALESCE(offense_type_ref.offense_type_short,'UNCATEGORIZED') AS offense_type_short,
        sen.uccs_code_uniform,
        sen.uccs_description_uniform,
        sen.uccs_category_uniform,
        sen.ncic_code_uniform,
        sen.ncic_description_uniform,
        sen.ncic_category_uniform,
        sen.nbirs_code_uniform,
        sen.nbirs_description_uniform,
        sen.nbirs_category_uniform,
        sen.crime_against_uniform,
        sen.is_drug_uniform,
        sen.is_violent_uniform,
        sen.offense_completed_uniform,
        sen.offense_attempted_uniform,
        sen.offense_conspired_uniform,        
        
        --these are TN specific fields which are not included in the state-agnostic schema at this point
        raw.total_program_credits,
        raw.total_behavior_credits,
        raw.total_ppsc_credits,        
        raw.total_ged_credits,
        raw.total_literary_credits,
        raw.total_drug_alcohol_credits,
        raw.total_education_attendance_credits,
        raw.total_treatment_credits,
        
        consecutive_sentence.sentence_id AS consecutive_sentence_id,
        -- Set the session_id_imposed if the sentence date imposed matches the session start date
        IF(ses.start_date = sen.date_imposed, ses.session_id, NULL) AS session_id_imposed,
        ses.session_id AS session_id_closest,
        DATE_DIFF(ses.start_date, sen.date_imposed, DAY) AS sentence_to_session_offset_days,
    FROM sentences_cte sen
    JOIN dedup_external_id_fields dedup
        USING(external_id)
    LEFT JOIN raw_data_cte AS raw
        USING(external_id)
    --TODO(#13012): Revisit join logic condition to see if we can improve hydration of imposed session id
    LEFT JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized` ses
        ON ses.person_id = sen.person_id
        AND ses.state_code = sen.state_code
        -- Join to all incarceration/supervision sessions and then pick the closest one to the date imposed
        AND (ses.compartment_level_1 LIKE 'INCARCERATION%' OR ses.compartment_level_1 LIKE 'SUPERVISION%')
        AND sen.date_imposed < COALESCE(ses.end_date, CURRENT_DATE('US/Eastern'))
    LEFT JOIN sentences_cte consecutive_sentence
        ON sen.consecutive_sentence_external_id = consecutive_sentence.external_id
    ---TODO(#13829): Investigate options for consecutive sentence relationship where supervision sentences are consecutive to incarceration sentences
        AND sen.sentence_type = consecutive_sentence.sentence_type
    LEFT JOIN `{project_id}.{analyst_dataset}.offense_type_mapping_materialized` offense_type_ref
        ON sen.state_code = offense_type_ref.state_code
        AND sen.description = offense_type_ref.offense_type
    --dedup to a single external id value,
    --prioritize the incarceration sentence over the supervision version when 1 sentence is in both tables
    QUALIFY ROW_NUMBER() OVER(PARTITION BY external_id ORDER BY effective_date,
        sentence_type, ABS(sentence_to_session_offset_days) ASC) = 1
"""

US_TN_SENTENCES_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_TN_SENTENCES_PREPROCESSED_VIEW_NAME,
    view_query_template=US_TN_SENTENCES_PREPROCESSED_QUERY_TEMPLATE,
    description=US_TN_SENTENCES_PREPROCESSED_VIEW_DESCRIPTION,
    raw_dataset=US_TN_RAW_DATASET,
    state_base_dataset=STATE_BASE_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_SENTENCES_PREPROCESSED_VIEW_BUILDER.build_and_print()
