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
"""Processed Sentencing Data"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_start_date_clause,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.calculator.query.state.views.sessions.state_sentence_configurations import (
    STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
    STATES_WITH_SEPARATE_INCARCERATION_SENTENCES_PREPROCESSED,
    STATES_WITH_SEPARATE_SENTENCES_PREPROCESSED,
    STATES_WITHOUT_INFERRED_SENTENCE_COMPLETION_DATE,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCES_PREPROCESSED_VIEW_NAME = "sentences_preprocessed"

SENTENCES_PREPROCESSED_VIEW_DESCRIPTION = """Processed Sentencing Data"""

# TODO(#13746): Investigate whether completion_date in state agnostic sentences preprocessed should allow for a date in the future
# TODO(#33402): deprecate `sentences_preprocessed` once all states are migrated to v2 infra
SENTENCES_PREPROCESSED_QUERY_TEMPLATE = f"""
    WITH
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
        CASE 
          WHEN sis.county_code = 'OUT_OF_STATE' 
            THEN 'OUT_OF_STATE'
          -- TODO(#16113): Add in TREATMENT subtype when methodology for this is resolved
          ELSE 'INCARCERATION' END AS sentence_sub_type,
        sis.effective_date,
        sis.date_imposed,
        sis.completion_date,
        sis.status,
        sis.status_raw_text,
        sis.parole_eligibility_date,
        sis.projected_min_release_date AS projected_completion_date_min,
        sis.projected_max_release_date AS projected_completion_date_max,
        sis.initial_time_served_days,
        COALESCE(sis.is_life, FALSE) AS life_sentence,
        sis.min_length_days,
        sis.max_length_days,
        sis.county_code,
        sis.sentence_metadata,
        charge.* EXCEPT(person_id, state_code, external_id, status, status_raw_text, county_code)
    FROM `{{project_id}}.normalized_state.state_incarceration_sentence` AS sis
    LEFT JOIN `{{project_id}}.normalized_state.state_charge_incarceration_sentence_association` assoc
        ON assoc.state_code = sis.state_code
        AND assoc.incarceration_sentence_id = sis.incarceration_sentence_id
    LEFT JOIN `{{project_id}}.sessions.charges_preprocessed` charge
        ON charge.state_code = assoc.state_code
        AND charge.charge_id = assoc.charge_id
    WHERE sis.external_id IS NOT NULL
        AND sis.state_code IN ({{v2_non_migrated_states}})
        AND sis.state_code NOT IN ({{special_states}}, {{incarceration_special_states}})

    UNION ALL

    SELECT * FROM `{{project_id}}.sessions.us_nd_incarceration_sentences_preprocessed`

    UNION ALL

    SELECT * FROM `{{project_id}}.sessions.us_co_incarceration_sentences_preprocessed`

    UNION ALL

    SELECT
        sss.person_id,
        sss.state_code,
        sss.supervision_sentence_id AS sentence_id,
        sss.external_id AS external_id,
        'SUPERVISION' AS sentence_type,
        CASE 
            WHEN sss.county_code = 'OUT_OF_STATE' 
                THEN 'OUT_OF_STATE'
            -- TODO(#16113): Add in TREATMENT subtype when methodology for this is resolved
            ELSE sss.supervision_type END AS sentence_sub_type,
        sss.effective_date,
        -- TODO(#14091): hydrate `date_imposed` for US_MO supervision sentences
        IF(sss.state_code = 'US_MO', COALESCE(sss.date_imposed, sss.effective_date), sss.date_imposed) AS date_imposed,
        sss.completion_date,
        sss.status,
        sss.status_raw_text,
        NULL AS parole_eligibility_date,
        sss.projected_completion_date AS projected_completion_date_min,
        sss.projected_completion_date AS projected_completion_date_max,
        CAST(NULL AS INT64) AS initial_time_served_days,
        FALSE AS life_sentence,
        sss.min_length_days,
        sss.max_length_days,
        sss.county_code,
        sss.sentence_metadata,
        charge.* EXCEPT(person_id, state_code, external_id, status, status_raw_text, county_code)
    FROM `{{project_id}}.normalized_state.state_supervision_sentence` AS sss
    LEFT JOIN `{{project_id}}.normalized_state.state_charge_supervision_sentence_association` assoc
        ON assoc.state_code = sss.state_code
        AND assoc.supervision_sentence_id = sss.supervision_sentence_id
    LEFT JOIN `{{project_id}}.sessions.charges_preprocessed` charge
        ON charge.state_code = assoc.state_code
        AND charge.charge_id = assoc.charge_id
    WHERE sss.external_id IS NOT NULL
        AND sss.state_code IN ({{v2_non_migrated_states}})
        AND sss.state_code NOT IN ({{special_states}})

    UNION ALL

    SELECT
        sent.person_id,
        sent.state_code,
        sent.sentence_id,
        sent.external_id,
        sentence_type,
        CASE
          WHEN sent.county_code = 'OUT_OF_STATE'
            THEN 'OUT_OF_STATE'
          -- TODO(#16113): Add in TREATMENT subtype when methodology for this is resolved
          ELSE sentence_type END AS sentence_sub_type,
        -- Use sentence status tables for completion_date and effective_date
        CAST(NULL AS DATE) AS effective_date,
        sent.imposed_date AS date_imposed,
        CAST(NULL AS DATE) AS completion_date,
        -- Use sentence status tables for the status data
        CAST(NULL AS STRING) AS status,
        CAST(NULL AS STRING) AS status_raw_text,
        -- Use the sentence length or sentence group length tables for sentence dates
        CAST(NULL AS DATE) AS parole_eligibility_date,
        CAST(NULL AS DATE) AS projected_completion_date_min,
        CAST(NULL AS DATE) AS projected_completion_date_max,
        sent.initial_time_served_days,
        COALESCE(sent.is_life, FALSE) AS life_sentence,
        -- Use the sentence length or sentence group length tables for sentence dates
        CAST(NULL AS INT64) AS min_length_days,
        CAST(NULL AS INT64) AS max_length_days,
        sent.county_code,
        sent.sentence_metadata,
        charge.* EXCEPT(person_id, state_code, external_id, status, status_raw_text, county_code)
    FROM `{{project_id}}.normalized_state.state_sentence` AS sent
    LEFT JOIN `{{project_id}}.normalized_state.state_charge_v2_state_sentence_association` assoc
        ON assoc.state_code = sent.state_code
        AND assoc.sentence_id = sent.sentence_id
    LEFT JOIN `{{project_id}}.sessions.charges_preprocessed` charge
        ON charge.state_code = assoc.state_code
        AND charge.charge_v2_id = assoc.charge_v2_id
    WHERE sent.external_id IS NOT NULL
        AND sent.state_code NOT IN ({{v2_non_migrated_states}})
    ),
    /*
    Joins back to sessions to create a "session_id_imposed" field as well as to the consecutive id preprocessed file
    sentence internal id.
    */
    sentences_with_session_id_imposed AS (
        SELECT
            sen.person_id,
            sen.state_code,
            sen.sentence_id,
            sen.external_id AS external_id,
            sen.sentence_type,
            sen.sentence_sub_type,
            sen.judicial_district,
            sen.effective_date,
            sen.date_imposed,
            sen.completion_date,
            sen.status,
            sen.status_raw_text,
            sen.parole_eligibility_date,
            sen.projected_completion_date_min,
            sen.projected_completion_date_max,
            -- Compute the release eligibility range percent from the min & max sentence length
            -- limited to values between 0-100%, NULL otherwise
            IF(
                min_length_days <= max_length_days AND max_length_days > 0,
                100 * min_length_days / max_length_days,
                CAST(NULL AS FLOAT64)
            ) AS release_eligibility_range_percent,
            sen.initial_time_served_days,
            sen.life_sentence,
            sen.min_length_days AS min_sentence_length_days_calculated,
            sen.max_length_days AS max_sentence_length_days_calculated,
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
            sen.uccs_code_uniform,
            sen.uccs_description_uniform,
            sen.uccs_category_uniform,
            sen.ncic_code_uniform,
            sen.ncic_description_uniform,
            sen.ncic_category_uniform,
            sen.nibrs_code_uniform,
            sen.nibrs_description_uniform,
            sen.nibrs_category_uniform,
            sen.crime_against_uniform,
            sen.is_drug_uniform,
            sen.is_violent_uniform,
            sen.offense_completed_uniform,
            sen.offense_attempted_uniform,
            sen.offense_conspired_uniform,
            CAST(NULL AS BOOL) AS is_violent_domestic,
            CAST(NULL AS BOOL) AS is_dui,
            CAST(NULL AS BOOL) AS is_victim_under_18,
            CAST(NULL AS BOOL) AS is_homicide,
            sen.county_code,
            sen.sentence_metadata,
            cs.consecutive_sentence_id,
            -- Set the session_id_imposed if the sentence date imposed matches the session start date
            IF(ses.start_date = sen.date_imposed, ses.session_id, NULL) AS session_id_imposed,
            ses.session_id AS session_id_closest,
            DATE_DIFF(ses.start_date, sen.date_imposed, DAY) AS sentence_to_session_offset_days
        FROM sentences_cte sen
        LEFT JOIN `{{project_id}}.sessions.consecutive_sentences_preprocessed_materialized` cs
            USING (person_id, state_code, sentence_id, sentence_type)
        -- TODO(#13012): Revisit join logic condition to see if we can improve hydration of imposed session id
        LEFT JOIN `{{project_id}}.sessions.compartment_sessions_materialized` ses
            ON ses.person_id = sen.person_id
            AND ses.state_code = sen.state_code
            -- Join to all incarceration/supervision sessions and then pick the closest one to the date imposed
            AND (ses.compartment_level_1 LIKE 'INCARCERATION%' OR ses.compartment_level_1 LIKE 'SUPERVISION%')
            AND sen.date_imposed < COALESCE(DATE_SUB(ses.end_date_exclusive, INTERVAL 1 DAY), CURRENT_DATE('US/Eastern'))
        QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code, person_id, external_id, charge_id, sentence_type
            ORDER BY ABS(sentence_to_session_offset_days) ASC, ses.session_id ASC) = 1
    
        UNION ALL
    
        SELECT
            *
            EXCEPT(
                total_program_credits,
                total_behavior_credits,
                total_ppsc_credits,
                total_ged_credits,
                total_literary_credits,
                total_drug_alcohol_credits,
                total_education_attendance_credits,
                total_treatment_credits
                )
        FROM `{{project_id}}.sessions.us_tn_sentences_preprocessed_materialized`
    ),
    /*
    Collect all discharge/release dates to liberty as inferred sentence completion dates
    */
    inferred_completion_dates AS (
        SELECT
            state_code,
            person_id,
            end_date_exclusive AS completion_date
        FROM `{{project_id}}.sessions.compartment_sessions_materialized`
        WHERE outflow_to_level_1 IN ("LIBERTY", "DEATH")
            -- Do not infer sentence completion dates for temporary releases to liberty
            AND COALESCE(end_reason, "EXTERNAL_UNKNOWN") != "TEMPORARY_RELEASE"
            -- Don't infer sentence completion dates for some states
            AND state_code NOT IN ({{states_without_inferred_completion_date}})
    ),
    /*
    Use the next successful supervision termination date following the effective date
    as the sentence completion date in order to bypass sentence completion date
    hydration issues
    */
    sentences_with_inferred_completion_date AS (
        SELECT
            sen.* EXCEPT (completion_date),
            COALESCE(sen.completion_date, comp.completion_date) AS completion_date,
            sen.completion_date IS NULL AND comp.completion_date IS NOT NULL AS is_completion_date_inferred,
        FROM sentences_with_session_id_imposed sen
        LEFT JOIN inferred_completion_dates comp
            ON sen.state_code = comp.state_code
            AND sen.person_id = comp.person_id
            AND {nonnull_start_date_clause("sen.effective_date")} < comp.completion_date
            AND {nonnull_start_date_clause("COALESCE(sen.date_imposed, sen.effective_date)")} < comp.completion_date
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY sen.state_code, sen.person_id, sen.sentence_id, sen.charge_id
            ORDER BY comp.completion_date ASC
        ) = 1
    )
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY state_code, person_id
            ORDER BY date_imposed, effective_date, external_id, sentence_id, charge_id
        ) AS sentences_preprocessed_id,
        *,
    FROM sentences_with_inferred_completion_date
"""

SENTENCES_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SENTENCES_PREPROCESSED_VIEW_NAME,
    view_query_template=SENTENCES_PREPROCESSED_QUERY_TEMPLATE,
    description=SENTENCES_PREPROCESSED_VIEW_DESCRIPTION,
    special_states=list_to_query_string(
        string_list=STATES_WITH_SEPARATE_SENTENCES_PREPROCESSED,
        quoted=True,
    ),
    incarceration_special_states=list_to_query_string(
        string_list=STATES_WITH_SEPARATE_INCARCERATION_SENTENCES_PREPROCESSED,
        quoted=True,
    ),
    states_without_inferred_completion_date=list_to_query_string(
        string_list=STATES_WITHOUT_INFERRED_SENTENCE_COMPLETION_DATE,
        quoted=True,
    ),
    v2_non_migrated_states=list_to_query_string(
        string_list=STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
        quoted=True,
    ),
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCES_PREPROCESSED_VIEW_BUILDER.build_and_print()
