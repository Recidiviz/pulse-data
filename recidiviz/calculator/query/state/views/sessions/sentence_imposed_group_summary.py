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
"""View that reduces sentence groups to 1 record and summarizes information about that sentence group"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_NAME = "sentence_imposed_group_summary"

SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_DESCRIPTION = """
    View that reduces sentence groups to 1 record and summarizes information about that sentence group. 
    Groups in this context are sentences that are imposed on the same date. This view takes into account consecutive sentences,
    but only in cases when the consecutive sentence is imposed on the same date as its parent sentence.
    """

SENTENCE_IMPOSED_GROUP_SUMMARY_QUERY_TEMPLATE = """
    /*{description}*/
    WITH cte AS 
    (
    SELECT 
        lk.*,
        sentence.min_sentence_length_days_calculated,
        sentence.max_sentence_length_days_calculated,
        sentence.effective_date,
        sentence.completion_date,
        sentence.parole_eligibility_date,
        sentence.projected_completion_date_max,
        sentence.projected_completion_date_min,
        sentence.initial_time_served_days,
        sentence.charge_id,
        sentence.is_violent,
        sentence.classification_type,
        sentence.classification_subtype,
        sentence.description,
        sentence.offense_type,
        sentence.ncic_code,
        sentence.statute,
        sentence.offense_type_short,
        sentence.offense_date,
        sentence.judicial_district,
        /*
        this field is used to calculate the total length. we need to know the longest sentence within each level so that
        we can sum up the longest sentences at each level. note that levels are not necessarily contained within
        sentence imposed groups in cases where a person has a sentence imposed at a later date to be consecutive to one
        that they are currently serving
        */
        ROW_NUMBER() OVER(PARTITION BY lk.person_id, lk.sentence_group_id, lk.sentence_level
            ORDER BY sentence.max_sentence_length_days_calculated DESC, lk.sentence_id) = 1 AS is_longest_in_level,
        
        /*
        This field is used to identify the longest sentence within an imposed group, regardless of its level. This is
        used to pull the most severe offense information
        */
        ROW_NUMBER() OVER(PARTITION BY lk.person_id, lk.date_imposed
            ORDER BY sentence.max_sentence_length_days_calculated DESC, lk.sentence_id) = 1 AS is_longest_in_imposed_group,
    
        /*
        This field is used to identify the first sentence within an imposed group. This generally should be the same as 
        the longest, unless a child has a longer sentence that its parent
        */   
        ROW_NUMBER() OVER(PARTITION BY lk.person_id, lk.date_imposed
            ORDER BY sentence_level ASC, sentence.max_sentence_length_days_calculated DESC, lk.sentence_id) = 1 AS is_first_sentence,
            
        MAX(sentence_level) OVER(PARTITION BY lk.person_id, lk.date_imposed) - 
            MIN(sentence_level) OVER(PARTITION BY lk.person_id, lk.date_imposed) + 1 AS num_levels,
    FROM `{project_id}.{sessions_dataset}.sentence_relationship_materialized` lk
    JOIN `{project_id}.{sessions_dataset}.sentences_preprocessed_materialized` sentence
        USING(person_id, sentence_id, sentence_type, state_code)
    )
    ,
    /*
    This CTE is created to add a field that lets us identify cases where a sentence imposed group is consecutive to
    another sentence imposed group. This could be useful if we are interested in distinguishing groups of sentences
    that are imposed consecutive to sentences that a person is already serving because those types of sentences
    might have different characteristics. 
    */
    consecutive_imposed_group_cte AS
    (
    SELECT
        person_id,
        state_code,
        parent_sentence_date_imposed AS consecutive_sentence_group_date_imposed,
        MIN(IF(date_imposed!=parent_sentence_date_imposed, date_imposed, NULL)) AS date_imposed
    FROM `{project_id}.{sessions_dataset}.sentence_relationship_materialized` 
    GROUP BY 1,2,3
    )
    SELECT
        person_id,
        state_code,
        date_imposed,
        session_id_imposed,
        consecutive_sentence_group_date_imposed,
        MIN(effective_date) AS effective_date,
        MAX(completion_date) AS completion_date,
        MAX(parole_eligibility_date) AS parole_eligibility_date,
        MAX(projected_completion_date_min) AS projected_completion_date_min,
        MAX(projected_completion_date_max) AS projected_completion_date_max,
        SUM(IF(is_longest_in_level, min_sentence_length_days_calculated, NULL)) AS min_sentence_imposed_group_length_days,
        SUM(IF(is_longest_in_level, max_sentence_length_days_calculated, NULL)) AS max_sentence_imposed_group_length_days,
        
        /*
        The following "ANY_VALUE" aggregations all leverage either the `is_longest_in_imposed_group` or the 
        `is_first_sentence` booleans which are calculated with a partition over `person_id` and `date_imposed`.
        This means that within those groups there is only one record that is TRUE. Since we are aggregating by 
        those same two fields `person_id` and `date_imposed` we will have only 1 non-null value for each of the 
        fields calculated below.
        */
        ANY_VALUE(IF(is_longest_in_imposed_group, sentence_id, NULL)) AS most_severe_sentence_id,
        ANY_VALUE(IF(is_first_sentence, sentence_id, NULL)) AS parent_sentence_id,
        ANY_VALUE(IF(is_longest_in_imposed_group, offense_type_short, NULL)) AS most_severe_offense_type_short,
        ANY_VALUE(IF(is_longest_in_imposed_group, description, NULL)) AS most_severe_description,
        --assumption here is that the felony sentence will be more severe than the misdemeanor sentence
        ANY_VALUE(IF(is_longest_in_imposed_group, classification_type, NULL)) AS most_severe_classification_type,
        ANY_VALUE(IF(is_longest_in_imposed_group, classification_subtype, NULL)) AS most_severe_classification_subtype,
        ANY_VALUE(IF(is_first_sentence, sentence_type, NULL)) AS parent_sentence_type,
        ANY_VALUE(IF(is_first_sentence, judicial_district, NULL)) AS judicial_district,
        LOGICAL_OR(is_violent) AS any_is_violent,
        COUNT(1) AS number_of_sentences,
        ANY_VALUE(num_levels) AS num_levels,
        IF(date_imposed>=MAX(effective_date), DATE_DIFF(date_imposed, MAX(effective_date), DAY), 0) AS days_effective_prior_to_imposed, 
        DATE_DIFF(MAX(completion_date), MIN(effective_date), DAY) AS days_to_completion,
        ARRAY_AGG(
            STRUCT
                (
                offense_date,
                charge_id,
                is_violent,
                classification_type,
                classification_subtype,
                description,
                offense_type,
                ncic_code,
                statute,
                offense_type_short 
                )
            ORDER BY offense_date, charge_id
        ) AS offense_attributes
    FROM cte
    LEFT JOIN consecutive_imposed_group_cte consec
        USING(person_id, state_code, date_imposed)
    GROUP BY 1,2,3,4,5
"""

SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    view_id=SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_NAME,
    view_query_template=SENTENCE_IMPOSED_GROUP_SUMMARY_QUERY_TEMPLATE,
    description=SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.build_and_print()
