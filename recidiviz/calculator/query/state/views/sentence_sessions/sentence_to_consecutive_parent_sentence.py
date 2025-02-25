# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""
Creates a table with two columns:
    - sentence_id: The ID for a given sentence
    - parent_sentence_id: The ID for a sentence that is a direct consecutive
      parent to the sentence of |sentence_id|

A sentence may have multiple consecutive parents.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CONSECUTIVE_SENTENCE_VIEW_ID = "sentence_id_to_consecutive_sentence_id"

QUERY_TEMPLATE = """
    SELECT 
      child_sentences.sentence_id, 
      parent_sentences.sentence_id AS parent_sentence_id
    FROM 
      `{project_id}.normalized_state.state_sentence` AS child_sentences,
      UNNEST(SPLIT(child_sentences.parent_sentence_external_id_array)) AS parent_sentence_external_id
    JOIN 
      `{project_id}.normalized_state.state_sentence` AS parent_sentences
    ON 
      parent_sentences.external_id = parent_sentence_external_id
    AND
      child_sentences.state_code = parent_sentences.state_code
"""

CONSECUTIVE_SENTENCES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SENTENCE_SESSIONS_DATASET,
    view_id=CONSECUTIVE_SENTENCE_VIEW_ID,
    view_query_template=QUERY_TEMPLATE,
    description=__doc__,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CONSECUTIVE_SENTENCES_VIEW_BUILDER.build_and_print()
