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
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_COMPARTMENT_SENTENCES_VIEW_NAME = "us_tn_compartment_sentences"

US_TN_COMPARTMENT_SENTENCES_VIEW_DESCRIPTION = (
    """State-specific preprocessing for TN raw sentence data"""
)

US_TN_COMPARTMENT_SENTENCES_QUERY_TEMPLATE = """
    /*{description}*/   
    --TODO(#10746): Remove sentencing pre-processing when TN sentences are ingested
    WITH primary_sentences AS
    (
    SELECT 
        ss.person_id,
        ss.primary_sentence_id,
        ss.sentence_group_id,
        ss.most_severe_sentence_level AS sentence_level,
        ss.state_code,
        #TODO(#11364): Use `last_sentence_id` to capture expiration date associated with latest child sentences
        sms.projected_completion_date_min,
        sms.projected_completion_date_max,
        sp.date_imposed,
        sp.effective_date,
        sms.description,
        sms.completion_date AS expiration_date,
        sms.classification_subtype AS conviction_class,
        sms.max_sentence_length_days_calculated,
        ss.total_max_sentence_length_days_calculated,
    FROM `{project_id}.{sessions_dataset}.us_tn_sentence_summary_materialized` ss
    -- Retrieves attributes of the longest parent sentence in the sentence group
    JOIN `{project_id}.{sessions_dataset}.us_tn_sentences_preprocessed_materialized` sp
        ON ss.person_id = sp.person_id
        AND ss.primary_sentence_id = sp.sentence_id
    -- Retrieves attributes of the longest (most severe) sentence in the sentence group
    JOIN `{project_id}.{sessions_dataset}.us_tn_sentences_preprocessed_materialized` sms
        ON ss.person_id = sms.person_id
        AND ss.most_severe_sentence_id = sms.sentence_id
    )
    ,
    primary_sentences_with_session_id AS 
    (
    SELECT DISTINCT
        sentences.*,
        sessions.session_id,
        sessions.last_day_of_data,
        offense_type_ref.offense_type_short,
        RANK() OVER(PARTITION BY sessions.person_id, session_id 
            ORDER BY ABS(date_diff(date_imposed, sessions.start_date, DAY)) ASC, sentences.sentence_level, sentences.effective_date, sentences.sentence_group_id) AS date_proximity_rank
    FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized`  sessions
    JOIN primary_sentences sentences 
        ON sessions.person_id = sentences.person_id
        AND sessions.start_date < COALESCE(sentences.projected_completion_date_max, '9999-01-01')
        -- Sentence start date (or date imposed for ID) must be before the session end date
        AND sentences.date_imposed < COALESCE(sessions.end_date, '9999-01-01')
        AND REGEXP_CONTAINS(sessions.compartment_level_1, '(INCARCERATION|SUPERVISION)')
    LEFT JOIN `{project_id}.{analyst_dataset}.offense_type_mapping_materialized` offense_type_ref
        ON offense_type_ref.state_code = sentences.state_code
        AND offense_type_ref.offense_type = sentences.description
    WHERE TRUE 
    QUALIFY date_proximity_rank = 1
    ORDER by session_id ASC, date_proximity_rank ASC
    )
    SELECT 
        p.person_id,
        p.state_code,
        p.session_id,
        p.primary_sentence_id AS sentence_id,
        p.sentence_group_id,
        p.sentence_level,
        CAST(NULL AS STRING) AS sentence_data_source,
        p.date_imposed AS sentence_date_imposed,
        p.effective_date AS sentence_start_date,
        p.expiration_date AS sentence_completion_date,
        p.projected_completion_date_min,
        p.projected_completion_date_max,
        CAST(NULL AS DATE) AS parole_eligibility_date,
        CAST(NULL AS BOOLEAN) AS life_sentence,
        CAST(NULL AS INT64) AS offense_count,
        CAST(NULL AS BOOLEAN) AS most_severe_is_violent,
        CAST(NULL AS STRING) AS most_severe_classification_type,
        p.conviction_class AS most_severe_felony_class,
        p.description AS most_severe_offense_type,
        p.offense_type_short AS most_severe_offense_type_short,
        CAST(NULL AS INT64) AS sentence_length_days,
        CAST(NULL AS INT64) AS min_projected_sentence_length,
        p.max_sentence_length_days_calculated AS max_projected_sentence_length,
        p.total_max_sentence_length_days_calculated,
        COUNT(1) AS sentence_count,
        --TODO(#11174): Update arrays to be structs
        ARRAY_AGG('MISSING') AS classification_type,
        ARRAY_AGG(DISTINCT COALESCE(all_sentences.classification_subtype, 'MISSING')) AS felony_class,
        ARRAY_AGG(DISTINCT COALESCE(all_sentences.description, 'MISSING')) AS description,
        ARRAY_AGG('MISSING') AS ncic_code,
        ARRAY_AGG(DISTINCT COALESCE(all_sentences.description, 'MISSING')) AS offense_type,
        ARRAY_AGG(DISTINCT COALESCE(offense_type_ref.offense_type_short, 'MISSING')) AS offense_type_short,
    FROM primary_sentences_with_session_id AS p
    JOIN `{project_id}.{sessions_dataset}.sentence_relationship_materialized` lk
        ON p.person_id = lk.person_id
        AND p.sentence_group_id = lk.sentence_group_id
    JOIN `{project_id}.{sessions_dataset}.us_tn_sentences_preprocessed_materialized` all_sentences
        ON all_sentences.person_id = lk.person_id
        AND all_sentences.sentence_id = lk.sentence_id 
    LEFT JOIN `{project_id}.{analyst_dataset}.offense_type_mapping_materialized` offense_type_ref
        ON offense_type_ref.state_code = all_sentences.state_code
        AND offense_type_ref.offense_type = all_sentences.description
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
"""

US_TN_COMPARTMENT_SENTENCES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_TN_COMPARTMENT_SENTENCES_VIEW_NAME,
    view_query_template=US_TN_COMPARTMENT_SENTENCES_QUERY_TEMPLATE,
    description=US_TN_COMPARTMENT_SENTENCES_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_COMPARTMENT_SENTENCES_VIEW_BUILDER.build_and_print()
