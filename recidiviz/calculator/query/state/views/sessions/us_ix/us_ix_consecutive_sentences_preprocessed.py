# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""State-specific preprocessing for ID Atlas consecutive sentences"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_NAME = (
    "us_ix_consecutive_sentences_preprocessed"
)

US_IX_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_DESCRIPTION = (
    """State-specific preprocessing for ID (Atlas) consecutive sentences"""
)

US_IX_CONSECUTIVE_SENTENCES_PREPROCESSED_QUERY_TEMPLATE = """
    WITH sentences AS
    (
    SELECT
        person_id,
        state_code,
        incarceration_sentence_id AS sentence_id,
        "INCARCERATION" AS sentence_type,
        date_imposed,
        (relationship LIKE "%CONSECUTIVE%") AS is_consecutive_sentence,
        SPLIT(external_id, "-")[SAFE_OFFSET(1)] AS seq_num,
        -- Pull out the parent sequence number for each consecutive relationship
        IF(relationship LIKE "%CONSECUTIVE%", SPLIT(relationship, "_")[OFFSET(1)], NULL) AS consec_seq_num,
    FROM `{project_id}.{normalized_state_dataset}.state_incarceration_sentence`,
    UNNEST(
        SPLIT(JSON_EXTRACT_SCALAR(sentence_metadata, "$.relationships"),
            ","
        )
    ) AS relationship
    WHERE state_code = "US_IX"

    UNION ALL

    SELECT
        person_id,
        state_code,
        supervision_sentence_id AS sentence_id,
        "SUPERVISION" AS sentence_type,
        date_imposed,
        (relationship LIKE "%CONSECUTIVE%") AS is_consecutive_sentence,
        SPLIT(external_id, "-")[SAFE_OFFSET(1)] AS seq_num,
        -- Pull out the parent sequence number for each consecutive relationship
        IF(relationship LIKE "%CONSECUTIVE%", SPLIT(relationship, "_")[OFFSET(1)], NULL) AS consec_seq_num,
    FROM `{project_id}.{normalized_state_dataset}.state_supervision_sentence`,
    UNNEST(
        SPLIT(JSON_EXTRACT_SCALAR(sentence_metadata, "$.relationships"),
            ","
        )
    ) AS relationship
    WHERE state_code = "US_IX"
    )
    SELECT
        s.person_id,
        s.state_code,
        s.sentence_id,
        s.sentence_type,
        cs.sentence_id AS consecutive_sentence_id,
        -- Flag the earliest imposed parent if the sentence has more than 1
        -- TODO(#18011): determine the best sentence to use as a parent for multi-parent
        -- consecutive (amended) sentences
        ROW_NUMBER() OVER (
            PARTITION BY s.state_code, s.person_id, s.sentence_id
            ORDER BY cs.date_imposed, cs.sentence_id
        ) AS parent_sentence_order,
    FROM sentences s
    LEFT JOIN sentences cs
        ON s.state_code = cs.state_code
        AND s.person_id = cs.person_id
        AND s.consec_seq_num = cs.seq_num
        AND s.sentence_type = cs.sentence_type
    WHERE s.is_consecutive_sentence
"""

US_IX_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_IX_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_NAME,
    view_query_template=US_IX_CONSECUTIVE_SENTENCES_PREPROCESSED_QUERY_TEMPLATE,
    description=US_IX_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.build_and_print()
