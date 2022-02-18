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
"""Sentences associated with each compartment session"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPARTMENT_SENTENCES_VIEW_NAME = "compartment_sentences"

COMPARTMENT_SENTENCES_VIEW_DESCRIPTION = """
## Overview

The table `compartment_sentences_materialized` joins to `compartment_sessions` based on `person_id` and `session_id` and is used to associate a sentence with a given stay within a compartment. This table is unique on `person_id` and `session_id`, but not every `session_id` in `compartment_sessions` will be associated with a sentence. Additionally, the same `sentence_id` can be associated with  more than one `session_id`. 

Sentencing data is significantly messier and less-validated than sessions data with state-specific quirks that we do not yet know the full extent of. Nonetheless, sentences are matched to sessions based on the proximity of the session `start_date` to the `sentence_start_date`, which is the approach that we feel works most consistently. 

## Field Definitions

|	Field	|	Description	|
|	--------------------	|	--------------------	|
|	person_id	|	Unique person identifier	|
|	state_code	|	State	|
|	session_id	|	Session ID that the sentence best matches to. Each session only has a single sentence associated with it, but multiple sessions can map to the same sentence. Sentences should only map to incarceration and supervision sessions.	|
|	sentence_id	|	The sentence id of the sentence that best matches the session. This value can be duplicated within a person - more than 1 session can be associated with the same sentence.	|
|	sentence_data_source	|	Sentencing data source. This will be `SUPERVISION` if it came from `state_supervision_sentence` and `INCARCERATION` if it came from `state_incarceration_sentence`.<br><br>There is currently no condition enforcing that supervision sentences need to map to supervision sessions and incarceration sentences need to map to incarceration sessions. This is because in some states, the parole "sentence" is part of the incarceration sentence from which they were released onto parole.	|
|	sentence_date_imposed	|	The date that the sentence was imposed. This is pulled directly from the state sentencing tables.	|
|	sentence_start_date	|	The date that the sentence starts. This is also pulled directly from the state sentencing tables. This is generally the field that is compared to the session start date in determining matches (except in ID where the date imposed is used)	|
|	sentence_completion_date	|	Sentence completion date. This is null for sentences that are currently being served.	|
|	projected_completion_date_min	|	Minumum projected completion date. Used to determine the sentence length. The minimum and maximum values will be the same for supervision sentences.	|
|	projected_completion_date_max	|	Maximum projected completion date. Used to determine the sentence length. The minimum and maximum values will be the same for supervision sentences.	|
|	parole_eligibility_date	|	Date that the person is eligible for parole. Will only be populated for incarceration sentences.	|
|	life_sentence	|	Boolean indicating whether the sentence is a life sentence. This will be FALSE for all supervision sentences.	|
|	offense_count	|	A single sentence can have more than one offense associated with it. This captures the number of offenses associated with the sentence.	|
|	most_severe_is_violent	|	Boolean that indicates whether the most severe offense associated with a sentence is a violent offense.	|
|	most_severe_classification_type	|	Value of the most severe classification associated with a sentence. Most common values are `FELONY` and `MISDEMEANOR`	|
|	classification_type	|	Array of all offense classification types associated with a sentence from which the most severe is chosen from.	|
|	description	|	Array of all offense descriptions associated with a sentence.	|
|	ncic_code	|	Array of all offense NCIC codes associated with a sentence	|
|	offense_type	|	Array of all offense types associated with a sentence	|
|	sentence_length_days	|	Difference between the sentence completion date and the estimated start date. The estimated start date is the lesser value between the sentence start date and the sentence date imposed	|
|	min_projected_sentence_length	|	Difference between the minimum projected completion date and the start date	|
|	max_projected_sentence_length	|	Difference between the maximum projected completion date and the start date	|


## Methodology

This view is constructed off of sentencing data in the `state` dataset, joined to `compartment_sessions`, and then deduped so that we have the best matched sentence for each session.
"""
INCLUDED_STATES = ["US_ID", "US_MO", "US_ND", "US_PA"]

COMPARTMENT_SENTENCES_QUERY_TEMPLATE = """
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
        min_length_days,
        max_length_days,
        completion_date,
        DATE(NULL) AS parole_eligibility_date,
        is_violent,
        classification_type,
        description,
        offense_type,
        ncic_code,
        FALSE AS life_sentence,
        'SUPERVISION' AS data_source,
    FROM `{project_id}.{base_dataset}.state_supervision_sentence` AS sss
    LEFT JOIN `{project_id}.{base_dataset}.state_charge_supervision_sentence_association`
        USING (state_code, supervision_sentence_id)
    LEFT JOIN `{project_id}.{base_dataset}.state_charge`
        USING (state_code, person_id, charge_id)
    WHERE sss.state_code IN ('{included_states}')
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
      min_length_days,
      max_length_days,
      completion_date,
      parole_eligibility_date,
      is_violent,
      classification_type,
      description,
      offense_type,
      ncic_code,
      COALESCE(sis.is_life, FALSE) AS life_sentence,
      'INCARCERATION' AS data_source
    FROM `{project_id}.{base_dataset}.state_incarceration_sentence` AS sis
    LEFT JOIN `{project_id}.{base_dataset}.state_charge_incarceration_sentence_association`
        USING (state_code, incarceration_sentence_id)
    LEFT JOIN `{project_id}.{base_dataset}.state_charge`
        USING (state_code, person_id, charge_id)
    WHERE sis.state_code IN ('{included_states}')
    )
    ,
    unioned_sentences_cte AS (
    /*
    For each supervision or incarceration sentence, pulls the most severe non-null is_violent flag and classification 
    type from associated charges.
    */
    SELECT *,
        FIRST_VALUE(is_violent IGNORE NULLS) OVER (
            PARTITION BY state_code, person_id, sentence_id 
            ORDER BY IF(is_violent, 1, 2)
        ) as most_severe_is_violent,
        FIRST_VALUE(classification_type IGNORE NULLS) OVER (
            PARTITION BY state_code, person_id, sentence_id 
            ORDER BY CASE classification_type WHEN 'FELONY' THEN 1 WHEN 'MISDEMEANOR' THEN 2 ELSE 3 END
        ) as most_severe_classification_type,
    FROM 
        (
        SELECT * FROM supervision_sentences_cte
        UNION ALL
        SELECT * FROM incarceration_sentences_cte
        )
    LEFT JOIN `{project_id}.{analyst_dataset}.offense_type_mapping_materialized` offense_type_ref
        USING(state_code, offense_type)
    )
    ,
    deduped_sentence_id_cte AS 
    /*
    Dedup cases where there are multiple offenses associated with the same sentence. Create an array of offense 
    classifications and descriptions. Exclude rows that are missing start dates and/or projected end dates unless it
    is a life sentence.
    */
    (
    SELECT 
        person_id,
        state_code,
        date_imposed,
        sentence_id,
        start_date,
        -- ID has sentences with unreasonable start dates and the date imposed appears to be the real start date
        -- Use this value for computing the lag between sentence start and session start
        LEAST(start_date, COALESCE(date_imposed, start_date)) AS estimated_start_date,
        -- Some records have max but no min or vice versa, fill in the one that is missing with the other value
        COALESCE(projected_completion_date_min, projected_completion_date_max) AS projected_completion_date_min,
        COALESCE(projected_completion_date_max, projected_completion_date_min) AS projected_completion_date_max,
        -- Fill in an estimated end date for life sentences in order to determine the longest sentence later
        CASE WHEN life_sentence THEN '9999-01-01'
            ELSE COALESCE(projected_completion_date_max, projected_completion_date_min,completion_date)
        END AS estimated_end_date,
        completion_date,
        parole_eligibility_date,
        data_source,
        COUNT(1) as offense_count,
        ANY_VALUE(most_severe_is_violent) most_severe_is_violent,
        ANY_VALUE(most_severe_classification_type) most_severe_classification_type,
        --TODO(#11174): Update arrays to be structs
        ARRAY_AGG(DISTINCT COALESCE(classification_type, 'MISSING')) classification_type,
        ARRAY_AGG(DISTINCT COALESCE(description, 'MISSING')) description,
        ARRAY_AGG(DISTINCT COALESCE(ncic_code, 'MISSING')) ncic_code,
        ARRAY_AGG(DISTINCT COALESCE(offense_type, 'MISSING')) offense_type,
        ARRAY_AGG(DISTINCT COALESCE(offense_type_short, 'MISSING')) offense_type_short,
        LOGICAL_OR(life_sentence) AS life_sentence,
    FROM unioned_sentences_cte
    WHERE start_date IS NOT NULL
        AND (projected_completion_date_max IS NOT NULL 
            OR projected_completion_date_min IS NOT NULL
            OR min_length_days IS NOT NULL
            OR max_length_days IS NOT NULL
            OR completion_date IS NOT NULL 
            OR life_sentence)
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
    )
    ,
    sentences_with_session_id AS 
    /*
    Join sessions and sentences. At this point, all of a person's unique sessions get joined to all of a person's unique
    sentences. Calculate the difference between the session start date and the sentence start date, which is ultimately 
    used to identify the best matched sentence.
    
    There is a criterion on the join that the session must overlap with the sentence. The session start date needs to be
    less than the sentence completion date and the sentence start date needs to be less than the session end date.
    There are cases where a session is missing the corresponding sentence data and would join with another session's
    sentence if this logic is not included.
    */
    (
    SELECT 
        sentences.*,
        sessions.session_id,
        sessions.last_day_of_data,
        RANK() OVER(PARTITION BY sessions.person_id, session_id ORDER BY ABS(date_diff(estimated_start_date, sessions.start_date, DAY)) ASC) as date_proximity_rank
    FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized`  sessions
    JOIN deduped_sentence_id_cte sentences 
        ON sessions.person_id = sentences.person_id
        -- Session start date must be before the projected max completion date
        AND sessions.start_date < COALESCE(sentences.projected_completion_date_max, '9999-01-01')
        -- Sentence start date (or date imposed for ID) must be before the session end date
        AND estimated_start_date < COALESCE(sessions.end_date, '9999-01-01')
        AND REGEXP_CONTAINS(sessions.compartment_level_1, '(INCARCERATION|SUPERVISION)')
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
        DATE_DIFF(completion_date, estimated_start_date, DAY) AS sentence_length_days,
        DATE_DIFF(projected_completion_date_min, start_date, DAY) AS min_projected_sentence_length,
        DATE_DIFF(projected_completion_date_max, start_date, DAY) AS max_projected_sentence_length,
        ROW_NUMBER() OVER(PARTITION BY person_id, session_id ORDER BY estimated_end_date DESC, sentence_id) AS longest_projected_sentence
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
        CAST(NULL AS INT64) AS sentence_group_id,
        CAST(NULL AS INT64) AS sentence_level,
        data_source AS sentence_data_source,
        date_imposed AS sentence_date_imposed,
        start_date AS sentence_start_date,
        CASE WHEN completion_date < last_day_of_data THEN completion_date END AS sentence_completion_date,
        projected_completion_date_min,
        projected_completion_date_max,
        parole_eligibility_date,
        life_sentence,
        offense_count,
        most_severe_is_violent,
        most_severe_classification_type,
        CAST(NULL AS STRING) AS most_severe_felony_class,
        CAST(NULL AS STRING) AS most_severe_offense_type,
        CAST(NULL AS STRING) AS most_severe_offense_type_short,
        CASE WHEN completion_date < last_day_of_data THEN sentence_length_days END AS sentence_length_days,
        min_projected_sentence_length,
        max_projected_sentence_length,
        CAST(NULL AS INT64) AS total_max_sentence_length_days_calculated,
        CAST(NULL AS INT64) AS sentence_count,
        classification_type,
        ARRAY_AGG('MISSING') OVER(PARTITION BY person_id, session_id) AS felony_class,
        description,
        ncic_code,
        offense_type,
        offense_type_short,
    FROM final_pre_deduped_date_proximity_cte
    WHERE longest_projected_sentence = 1
    
    UNION ALL
    --TODO(#10746): Remove sentencing pre-processing when TN sentences are ingested
    SELECT
        *
    FROM `{project_id}.{sessions_dataset}.us_tn_compartment_sentences_materialized` 

    
    """

COMPARTMENT_SENTENCES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.SESSIONS_DATASET,
    view_id=COMPARTMENT_SENTENCES_VIEW_NAME,
    view_query_template=COMPARTMENT_SENTENCES_QUERY_TEMPLATE,
    description=COMPARTMENT_SENTENCES_VIEW_DESCRIPTION,
    base_dataset=STATE_BASE_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    included_states="', '".join(INCLUDED_STATES),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_SENTENCES_VIEW_BUILDER.build_and_print()
