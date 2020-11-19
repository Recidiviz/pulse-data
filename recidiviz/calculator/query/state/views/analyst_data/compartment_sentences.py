# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Sentences associated with each compartment session"""
# pylint: disable=trailing-whitespace
# pylint: disable=line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import STATE_BASE_DATASET, ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPARTMENT_SENTENCES_VIEW_NAME = 'compartment_sentences'

COMPARTMENT_SENTENCES_VIEW_DESCRIPTION = \
    """Sentences associated with each compartment session. Joined based on sentence start date proximity to session start date"""

COMPARTMENT_SENTENCES_QUERY_TEMPLATE = \
    """
    /*{description}*/
    /*
    Union together supervision and incarceration sentence data. Join to state charge datasets to get the description and
    classification of the offense(s). 
    
    Supervision data has only one projected completion date whereas incarceration has a min and max projected. For 
    supervision, set both the min and max to the same projected completion date.
    */
    WITH supervision_sentences_cte AS 
    (
    SELECT 
        sss.person_id,
        sss.state_code,
        date_imposed,
        supervision_sentence_id AS sentence_id,
        start_date,
        projected_completion_date AS projected_completion_date_min,
        projected_completion_date AS projected_completion_date_max,
        completion_date,
        DATE(NULL) AS parole_eligibility_date,
        classification_type,
        description,
        'SUPERVISION' AS data_source
    FROM `{project_id}.{base_dataset}.state_supervision_sentence` AS sss
    LEFT JOIN `{project_id}.{base_dataset}.state_charge_supervision_sentence_association`
        USING (supervision_sentence_id)
    LEFT JOIN `{project_id}.{base_dataset}.state_charge`
        USING (charge_id)
    )
    ,
    incarceration_sentences_cte AS 
    (
    SELECT 
      sis.person_id,
      sis.state_code,
      date_imposed,
      incarceration_sentence_id AS sentence_id,
      start_date,
      projected_min_release_date AS projected_completion_date_min,
      projected_max_release_date AS projected_completion_date_max,
      completion_date,
      parole_eligibility_date,
      classification_type,
      description,
      'INCARCERATION' AS data_source
    FROM `{project_id}.{base_dataset}.state_incarceration_sentence` AS sis
    LEFT JOIN `{project_id}.{base_dataset}.state_charge_incarceration_sentence_association`
        USING (incarceration_sentence_id)
    LEFT JOIN `{project_id}.{base_dataset}.state_charge`
        USING (charge_id)
    )
    ,
    unioned_sentences_cte AS (
    SELECT * FROM supervision_sentences_cte
    UNION ALL
    SELECT * FROM incarceration_sentences_cte
    )
    ,
    deduped_sentence_id_cte AS 
    /*
    Dedup cases where there are multiple offenses associated with the same sentence. Create an array of offense 
    classifications and descriptions.
    */
    (
    SELECT 
        person_id,
        state_code,
        date_imposed,
        sentence_id,
        start_date,
        projected_completion_date_min,
        projected_completion_date_max,
        completion_date,
        parole_eligibility_date,
        data_source,
        COUNT(1) as offense_count,
        ARRAY_AGG(COALESCE(classification_type, 'MISSING')) classification_type,
        ARRAY_AGG(COALESCE(description, 'MISSING')) description
    FROM unioned_sentences_cte
    WHERE start_date is not null
    GROUP BY 1,2,3,4,5,6,7,8,9,10
    ORDER BY 1 ASC, 2 ASC, 3 ASC, 4 ASC, 5 ASC, 6 ASC, 7 ASC, 8 ASC, 9 ASC, 10 ASC
    )
    ,
    sentences_with_session_id AS 
    /*
    Join sessions and sentences. At this point, all of a person's unique sessions get joined to all of a person's unique
    sentences. Calculate the difference between the session start date and the sentence start date, which is ultimately 
    used to identify the best matched sentence.
    
    There is a criterion on the join that the session start date needs to be less than the sentence completion date. This is 
    obvious but there were a few edge cases where this was not the case, so here it is prevented from happening upfront.
    With additional validation, it is possible we discover other edge cases like this and the join logic can be further 
    refined.
    */
    (
    SELECT 
        sentences.*,
        sessions.session_id,
        sessions.last_day_of_data,
        RANK() OVER(PARTITION BY sessions.person_id, session_id ORDER BY ABS(date_diff(sentences.start_date, sessions.start_date, DAY)) ASC) as date_proximity_rank
    FROM `{project_id}.{analyst_dataset}.compartment_sessions_materialized`  sessions
    JOIN deduped_sentence_id_cte sentences 
        ON sessions.person_id = sentences.person_id
        AND sessions.start_date < COALESCE(sentences.completion_date, '9999-01-01')
        AND sessions.compartment_level_1 in ('INCARCERATION','SUPERVISION') 
    ORDER by session_id ASC, date_proximity_rank ASC
    )
    ,
    final_pre_deduped_date_proximity_cte AS 
    /*
    This cte selects the sentence(s) that have a start date closest in time to the start date of a session. Note that 
    this was calculated using a 'rank' so that two sentences with the same start date (something that happens) will 
    both still be included. These get deduped at the end, but it could potentially make sense to include all of these in 
    the output table.
    
    This cte also creates a set of fields that can be used to dedup the final output in the case described above. These 
    are indicators for (1) first sentence (based on date imposed and sentence_id), (2) last sentence, (3) shortest projected
    sentence, (4) longest projected sentence. 
    */
    (
    SELECT *,
        DATE_DIFF(completion_date, start_date, DAY) AS sentence_length_days,
        DATE_DIFF(projected_completion_date_min, start_date, DAY) AS min_projected_sentence_length,
        DATE_DIFF(projected_completion_date_max, start_date, DAY) AS max_projected_sentence_length,
        ROW_NUMBER() OVER(PARTITION BY person_id, session_id ORDER BY date_imposed ASC, sentence_id) AS first_sentence,
        ROW_NUMBER() OVER(PARTITION BY person_id, session_id ORDER BY date_imposed DESC, sentence_id DESC) AS last_sentence, 
        ROW_NUMBER() OVER(PARTITION BY person_id, session_id ORDER BY projected_completion_date_max ASC, sentence_id) AS shortest_projected_sentence,
        ROW_NUMBER() OVER(PARTITION BY person_id, session_id ORDER BY projected_completion_date_max DESC, sentence_id) AS longest_projected_sentence
    FROM sentences_with_session_id
    WHERE date_proximity_rank = 1
    ORDER by person_id ASC, session_id ASC, start_date ASC
    )
    /*
    The final output (at this point) dedups so that one sentence ties to each session based on the longest projected 
    sentence, in cases where there are more than one sentence with the same start date. The only session info that is 
    included in this table is the session_id. All other session info can be obtained by left joining this table to the 
    sessions table.
    */
    SELECT 
        person_id,
        state_code,
        session_id,
        sentence_id,
        data_source AS sentence_data_source,
        date_imposed AS sentence_date_imposed,
        start_date AS sentence_start_date,
        CASE WHEN completion_date < last_day_of_data THEN completion_date END AS sentence_completion_date,
        projected_completion_date_min,
        projected_completion_date_max,
        parole_eligibility_date,
        offense_count,
        classification_type,
        description,
        CASE WHEN completion_date < last_day_of_data THEN sentence_length_days END AS sentence_length_days,
        min_projected_sentence_length,
        max_projected_sentence_length
    FROM final_pre_deduped_date_proximity_cte
    WHERE longest_projected_sentence = 1
    ORDER by person_id ASC, session_id ASC, sentence_start_date ASC
    """

COMPARTMENT_SENTENCES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.ANALYST_VIEWS_DATASET,
    view_id=COMPARTMENT_SENTENCES_VIEW_NAME,
    view_query_template=COMPARTMENT_SENTENCES_QUERY_TEMPLATE,
    description=COMPARTMENT_SENTENCES_VIEW_DESCRIPTION,
    base_dataset=STATE_BASE_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_SENTENCES_VIEW_BUILDER.build_and_print()
