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
"""View that summarizes the relationship of TN sentences and defines sentence groups"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_SENTENCE_RELATIONSHIP_VIEW_NAME = "us_tn_sentence_relationship"

US_TN_SENTENCE_RELATIONSHIP_VIEW_DESCRIPTION = """View that summarizes the relationship of TN sentences and defines sentence groups"""

US_TN_SENTENCE_RELATIONSHIP_QUERY_TEMPLATE = """
    /*{description}*/
    /*
    -- TODO(#10746): Remove sentencing pre-processing when TN sentences are ingested  
    */
    WITH cte AS
    /*
    This cte does a series of self-joins based on the consecutive sentence id field to create a view that has a record
    for every sentence "chain". Sentences are given a chain id which is used to calculate the sentence level, and only records
    that represent the level 1 parent sentence id (sentence that has no parent sentence id) are included.
    */
    (
    SELECT
        l1.person_id,
        l1.state_code,
        l1.sentence_id AS level_1_sentence_id,
        l1.sentence_imposed_date,
        l2.sentence_id AS level_2_sentence_id,
        l3.sentence_id AS level_3_sentence_id,
        l4.sentence_id AS level_4_sentence_id,
        l5.sentence_id AS level_5_sentence_id,
        l6.sentence_id AS level_6_sentence_id,
        ROW_NUMBER() OVER(PARTITION BY l1.person_id
            ORDER BY l1.sentence_id, l2.sentence_id, l3.sentence_id, l4.sentence_id, l5.sentence_id, l6.sentence_id) AS sentence_chain_id
    FROM `{project_id}.{sessions_dataset}.us_tn_sentences_preprocessed_materialized` l1
    --TODO(#11164): Replace self joins with recursive CTE
    LEFT JOIN `{project_id}.{sessions_dataset}.us_tn_sentences_preprocessed_materialized` l2
        ON l1.sentence_id = l2.consecutive_sentence_id
        AND l1.person_id = l2.person_id
    LEFT JOIN `{project_id}.{sessions_dataset}.us_tn_sentences_preprocessed_materialized` l3
        ON l2.sentence_id = l3.consecutive_sentence_id
        AND l2.person_id = l3.person_id
    LEFT JOIN `{project_id}.{sessions_dataset}.us_tn_sentences_preprocessed_materialized` l4
        ON l3.sentence_id = l4.consecutive_sentence_id
        AND l3.person_id = l4.person_id
    LEFT JOIN `{project_id}.{sessions_dataset}.us_tn_sentences_preprocessed_materialized` l5
        ON l4.sentence_id = l5.consecutive_sentence_id
        AND l4.person_id = l5.person_id
    LEFT JOIN `{project_id}.{sessions_dataset}.us_tn_sentences_preprocessed_materialized` l6
        ON l5.sentence_id = l6.consecutive_sentence_id
        AND l5.person_id = l6.person_id
    --indicates a parent sentence because it has no parent
    WHERE l1.consecutive_sentence_id IS NULL
    )
    ,
    sentence_chain_cte AS
    /*
    This cte unnests every sentence chain id, so that there is 1 record for every sentence id that makes up a sentence chain
    within a sentence group. 
    
    Two important fields are calculated in this step:
    1. Sentence Level - this is calculated with a row number partitioning within each person id and sentence chain id. A sentence with no child
    sentences will only have a sentence level value of "1" within that person and sentence id.
    2. Sentence Group ID - this is (for now) calculated based on the sentence date imposed, which is the field that best represents the 
    date at which a person was admitted to prison. We are therefore assuming that sentences imposed on the same day are related and part of the same 
    group. Additionally, there is an "implied" grouping, where child sentences are considered part of the same sentence group based on the fact
    that they a part of a sentence chain (even if a child sentence, for some reason had a different imposed date than its parent). This happens because
    we are starting with the set of sentences with parents and constructing groups from there. 
    */
    (
    SELECT
        person_id,
        state_code,
        level_1_sentence_id,
        sentence_imposed_date,
        sentence_chain_id,
        ROW_NUMBER() OVER(PARTITION BY person_id, sentence_chain_id
            ORDER BY level_1_sentence_id, level_2_sentence_id, level_3_sentence_id, level_4_sentence_id, level_5_sentence_id, level_6_sentence_id) AS sentence_level,
        --every case of a unique sentence imposed date represents a new sentence group
        DENSE_RANK() OVER(PARTITION BY person_id ORDER BY sentence_imposed_date) AS sentence_group_id,
        sentence_id,
    FROM cte,
    UNNEST([level_1_sentence_id, level_2_sentence_id, level_3_sentence_id, level_4_sentence_id, level_5_sentence_id, level_6_sentence_id]) AS sentence_id
    WHERE sentence_id IS NOT NULL
    )
    /*
    Now that we have sentence level and sentence group ids, we no longer care about the specific path of a sentence chain, but instead
    just the full set of sentences (concurrent and consecutive) that are part of the same sentence group, along with their level within that hierarchy.
    Therefore, the following query excludes the sentence_chain_id field and takes a distinct, so that we are left with 1 record for every sentence id within 
    each sentence group. 
    */
    SELECT DISTINCT 
        person_id,
        state_code,
        sentence_group_id,
        sentence_level,
        sentence_id,
    FROM sentence_chain_cte a
"""

US_TN_SENTENCE_RELATIONSHIP_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    view_id=US_TN_SENTENCE_RELATIONSHIP_VIEW_NAME,
    view_query_template=US_TN_SENTENCE_RELATIONSHIP_QUERY_TEMPLATE,
    description=US_TN_SENTENCE_RELATIONSHIP_VIEW_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_SENTENCE_RELATIONSHIP_VIEW_BUILDER.build_and_print()
