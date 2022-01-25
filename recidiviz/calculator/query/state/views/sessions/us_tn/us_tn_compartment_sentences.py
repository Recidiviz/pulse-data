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
    WITH sentences_with_session_id AS (
    SELECT DISTINCT
        sentences.*,
        sessions.session_id,
        sessions.last_day_of_data,
        offense_type_ref.offense_type_short,
        RANK() OVER(PARTITION BY sessions.person_id, session_id ORDER BY ABS(date_diff(sentence_effective_date, sessions.start_date, DAY)) ASC, sentences.sentence_effective_date) as date_proximity_rank
    FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized`  sessions
    JOIN `{project_id}.{sessions_dataset}.us_tn_sentences_preprocessed_materialized` sentences 
        ON sessions.person_id = sentences.person_id
        -- no completion date here, so we don't have a restriction that the session start date is before the projected max completion date
        -- Sentence start date must be before the session end date
        AND sentence_effective_date < COALESCE(sessions.end_date, '9999-01-01')
        AND sessions.compartment_level_1 IN ('INCARCERATION','SUPERVISION')
    LEFT JOIN `{project_id}.{analyst_dataset}.offense_type_mapping_materialized` offense_type_ref
        ON offense_type_ref.state_code = sentences.state_code
        AND offense_type_ref.offense_type = sentences.description
    WHERE TRUE 
    QUALIFY date_proximity_rank = 1
    ORDER by session_id ASC, date_proximity_rank ASC
    )
    SELECT 
        person_id,
        state_code,
        session_id,
        sentence_id,
        CAST(NULL AS STRING) AS sentence_data_source,
        sentence_effective_date AS sentence_date_imposed,
        sentence_effective_date AS sentence_start_date,
        CAST(NULL AS DATE) AS sentence_completion_date,
        CAST(NULL AS DATE) AS projected_completion_date_min,
        CAST(NULL AS DATE) AS projected_completion_date_max,
        CAST(NULL AS DATE) AS parole_eligibility_date,
        CAST(NULL AS BOOLEAN) AS life_sentence,
        CAST(NULL AS INT64) AS offense_count,
        CAST(NULL AS BOOLEAN) AS most_severe_is_violent,
        CAST(NULL AS STRING) AS most_severe_classification_type,
        ARRAY_AGG('MISSING') OVER(PARTITION BY person_id, session_id) AS classification_type,
        ARRAY_AGG(description) OVER(PARTITION BY person_id, session_id) AS description,
        ARRAY_AGG('MISSING') OVER(PARTITION BY person_id, session_id) AS ncic_code,
        ARRAY_AGG('MISSING') OVER(PARTITION BY person_id, session_id) AS offense_type,
        ARRAY_AGG(offense_type_short) OVER(PARTITION BY person_id, session_id) AS offense_type_short,
        CAST(NULL AS INT64) AS sentence_length_days,
        CAST(NULL AS INT64) AS min_projected_sentence_length,
        CAST(NULL AS INT64) AS max_projected_sentence_length
    FROM sentences_with_session_id
    ORDER by person_id ASC, session_id ASC, sentence_start_date ASC
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
