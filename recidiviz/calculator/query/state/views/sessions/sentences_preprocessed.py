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
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCES_PREPROCESSED_VIEW_NAME = "sentences_preprocessed"

SENTENCES_PREPROCESSED_VIEW_DESCRIPTION = """Processed Sentencing Data"""

# List of states that have separate sentence preprocessed views
SENTENCES_PREPROCESSED_SPECIAL_STATES = ["US_TN"]

# TODO(#13746): Investigate whether completion_date in state agnostic sentences preprocessed should allow for a date in the future
SENTENCES_PREPROCESSED_QUERY_TEMPLATE = """
    /*{description}*/
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
        JSON_EXTRACT_SCALAR(sis.sentence_metadata, '$.CONSECUTIVE_SENTENCE_ID') AS consecutive_sentence_external_id,
        sis.start_date AS effective_date,
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
        charge.charge_id,
        charge.offense_date,
        charge.is_violent,
        charge.classification_type,
        charge.classification_subtype,
        charge.description,
        charge.offense_type,
        charge.ncic_code,
        charge.statute,
        charge.judicial_district,
    FROM `{project_id}.{state_base_dataset}.state_incarceration_sentence` AS sis
    LEFT JOIN `{project_id}.{state_base_dataset}.state_charge_incarceration_sentence_association` assoc
        ON assoc.state_code = sis.state_code
        AND assoc.incarceration_sentence_id = sis.incarceration_sentence_id
    LEFT JOIN `{project_id}.{sessions_dataset}.charges_preprocessed` charge
        ON charge.state_code = assoc.state_code
        AND charge.charge_id = assoc.charge_id
    WHERE sis.external_id IS NOT NULL
        AND sis.state_code NOT IN ('{special_states}')

    UNION ALL

    SELECT
        sss.person_id,
        sss.state_code,
        sss.supervision_sentence_id AS sentence_id,
        sss.external_id AS external_id,
        'SUPERVISION' AS sentence_type,
        JSON_EXTRACT_SCALAR(sss.sentence_metadata, '$.CONSECUTIVE_SENTENCE_ID') AS consecutive_sentence_external_id,
        sss.start_date AS effective_date,
        -- TODO(#14091): hydrate `date_imposed` for US_MO supervision sentences
        IF(sss.state_code = 'US_MO', COALESCE(sss.date_imposed, sss.start_date), sss.date_imposed) AS date_imposed,
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
        charge.charge_id,
        charge.offense_date,
        charge.is_violent,
        charge.classification_type,
        charge.classification_subtype,
        charge.description,
        charge.offense_type,
        charge.ncic_code,
        charge.statute,
        charge.judicial_district,
    FROM `{project_id}.{state_base_dataset}.state_supervision_sentence` AS sss
    LEFT JOIN `{project_id}.{state_base_dataset}.state_charge_supervision_sentence_association` assoc
        ON assoc.state_code = sss.state_code
        AND assoc.supervision_sentence_id = sss.supervision_sentence_id
    LEFT JOIN `{project_id}.{sessions_dataset}.charges_preprocessed` charge
        ON charge.state_code = assoc.state_code
        AND charge.charge_id = assoc.charge_id
    WHERE sss.external_id IS NOT NULL
        AND sss.state_code NOT IN ('{special_states}')
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
        sen.judicial_district,
        sen.consecutive_sentence_external_id,
        sen.effective_date,
        sen.date_imposed,
        sen.completion_date,
        sen.status,
        sen.status_raw_text,
        sen.parole_eligibility_date,
        sen.projected_completion_date_min,
        sen.projected_completion_date_max,
        sen.initial_time_served_days,
        sen.life_sentence,
        sen.min_length_days AS min_sentence_length_days_calculated,
        sen.max_length_days AS max_sentence_length_days_calculated,
        sen.charge_id,
        sen.offense_date,
        sen.is_violent,
        COALESCE(sen.classification_type, 'EXTERNAL_UNKNOWN') AS classification_type,
        COALESCE(sen.classification_subtype, 'EXTERNAL_UNKNOWN') AS classification_subtype,
        sen.description,
        sen.offense_type,
        sen.ncic_code,
        sen.statute,
        COALESCE(offense_type_ref.offense_type_short,'UNCATEGORIZED') AS offense_type_short,
        consecutive_sentence.sentence_id AS consecutive_sentence_id,
        -- Set the session_id_imposed if the sentence date imposed matches the session start date
        IF(ses.start_date = sen.date_imposed, ses.session_id, NULL) AS session_id_imposed,
        ses.session_id AS session_id_closest,
        DATE_DIFF(ses.start_date, sen.date_imposed, DAY) AS sentence_to_session_offset_days,
    FROM sentences_cte sen
    -- TODO(#13012): Revisit join logic condition to see if we can improve hydration of imposed session id
    LEFT JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized` ses
        ON ses.person_id = sen.person_id
        AND ses.state_code = sen.state_code
        -- Join to all incarceration/supervision sessions and then pick the closest one to the date imposed
        AND (ses.compartment_level_1 LIKE 'INCARCERATION%' OR ses.compartment_level_1 LIKE 'SUPERVISION%')
        AND sen.date_imposed < COALESCE(ses.end_date, CURRENT_DATE('US/Eastern'))
    LEFT JOIN sentences_cte consecutive_sentence
        ON sen.state_code = consecutive_sentence.state_code
        AND sen.person_id = consecutive_sentence.person_id
        AND sen.consecutive_sentence_external_id = consecutive_sentence.external_id
        -- TODO(#13829): Investigate options for consecutive sentence relationship where supervision sentences are consecutive to incarceration sentences
        AND sen.sentence_type = consecutive_sentence.sentence_type
    LEFT JOIN `{project_id}.{analyst_dataset}.offense_type_mapping_materialized` offense_type_ref
        ON sen.state_code = offense_type_ref.state_code
        AND COALESCE(sen.offense_type, sen.description) = offense_type_ref.offense_type
    QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code, person_id, external_id, charge_id, sentence_type
        ORDER BY ABS(sentence_to_session_offset_days) ASC) = 1

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
            total_treatment_credits)
    FROM `{project_id}.{sessions_dataset}.us_tn_sentences_preprocessed_materialized`
"""

SENTENCES_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SENTENCES_PREPROCESSED_VIEW_NAME,
    view_query_template=SENTENCES_PREPROCESSED_QUERY_TEMPLATE,
    description=SENTENCES_PREPROCESSED_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    state_base_dataset=STATE_BASE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    special_states="', '".join(SENTENCES_PREPROCESSED_SPECIAL_STATES),
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCES_PREPROCESSED_VIEW_BUILDER.build_and_print()
