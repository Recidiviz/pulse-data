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

US_TN_SENTENCE_SUMMARY_VIEW_NAME = "us_tn_sentence_summary"

US_TN_SENTENCE_SUMMARY_VIEW_DESCRIPTION = """View that reduces sentence groups to 1 record and summarizes information about that sentence group"""

US_TN_SENTENCE_SUMMARY_QUERY_TEMPLATE = """
    /*{description}*/
    -- TODO(#10746): Remove sentencing pre-processing when TN sentences are ingested  
    WITH cte AS 
    (
    SELECT 
        lk.*,
        sentence.max_sentence_length_days_calculated,
        total_program_credits + total_behavior_credits + total_ppsc_credits + total_ged_credits
            + total_literary_credits + total_drug_alcohol_credits + total_education_attendance_credits 
            + total_treatment_credits AS total_credits_sentence_expiration,
        total_program_credits,
        total_behavior_credits,
        total_ppsc_credits,
        total_ged_credits + total_literary_credits + total_drug_alcohol_credits 
            + total_education_attendance_credits + total_treatment_credits 
            AS total_other_credits,
    FROM `{project_id}.{sessions_dataset}.us_tn_sentence_relationship_materialized` lk
    JOIN `{project_id}.{sessions_dataset}.us_tn_sentences_preprocessed_materialized` sentence
        ON lk.person_id = sentence.person_id
        AND lk.sentence_id = sentence.sentence_id
    WHERE TRUE
    --get the longest sentence within each sentence level
    QUALIFY ROW_NUMBER() OVER(PARTITION BY lk.person_id, lk.sentence_group_id, lk.sentence_level
        ORDER BY sentence.max_sentence_length_days_calculated DESC, lk.sentence_id)  = 1
    )
    /*
    The previous cte chooses the longest sentence within a sentence level. This query calculates the total sentence length across all of those levels
    and chooses the sentence id that is the longest across the entire group as the primary one. This presumably should be the first level unless
    a child sentence is longer than its parent. It looks like in the TN data, 95% of the time this is the case. 5% of "primary" sentences do come from
    a level other than level 1, which means that it is indeed a case of a child sentence being longer than the parent. In these cases, we are still calling
    the longest sentence the primary one.
    */
    -- TODO(#11161): Consider creating both a "primary" sentence id (based on level) and a "most severe" sentence level based on sentence length. This would allow us to handle cases where the parent sentence is not the longest  
    SELECT DISTINCT
        person_id,
        state_code,
        sentence_group_id,
        FIRST_VALUE(sentence_id) OVER(longest_sentence_within_group) AS primary_sentence_id,
        FIRST_VALUE(sentence_level) OVER(longest_sentence_within_group) AS sentence_level,
        #TODO(#11250): Add additional logic for handling credit accumulation from parents with different concurrent credits
        SUM(max_sentence_length_days_calculated) OVER(PARTITION BY person_id, sentence_group_id) AS total_max_sentence_length_days_calculated,
        SUM(total_credits_sentence_expiration) OVER(PARTITION BY person_id, sentence_group_id) AS total_credits_sentence_expiration,
        SUM(total_program_credits) OVER(PARTITION BY person_id, sentence_group_id) AS total_program_credits,
        SUM(total_behavior_creditS) OVER(PARTITION BY person_id, sentence_group_id) AS total_behavior_credits,
        SUM(total_ppsc_credits) OVER(PARTITION BY person_id, sentence_group_id) AS total_ppsc_credits,
        SUM(total_other_credits) OVER(PARTITION BY person_id, sentence_group_id) AS total_other_credits,
    FROM cte
    WINDOW longest_sentence_within_group AS (PARTITION BY person_id, sentence_group_id ORDER BY max_sentence_length_days_calculated DESC, sentence_level ASC)
    ORDER BY 1,2,3
"""

US_TN_SENTENCE_SUMMARY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    view_id=US_TN_SENTENCE_SUMMARY_VIEW_NAME,
    view_query_template=US_TN_SENTENCE_SUMMARY_QUERY_TEMPLATE,
    description=US_TN_SENTENCE_SUMMARY_VIEW_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_SENTENCE_SUMMARY_VIEW_BUILDER.build_and_print()
