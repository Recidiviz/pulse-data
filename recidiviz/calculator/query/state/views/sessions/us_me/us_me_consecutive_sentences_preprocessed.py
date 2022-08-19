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
"""State-specific preprocessing for ME consecutive sentences"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ME_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_NAME = (
    "us_me_consecutive_sentences_preprocessed"
)

US_ME_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_DESCRIPTION = (
    """State-specific preprocessing for ME consecutive sentences"""
)

US_ME_CONSECUTIVE_SENTENCES_PREPROCESSED_QUERY_TEMPLATE = """
    /*{description}*/
    WITH sentences AS 
    (
    SELECT
        person_id,
        state_code,
        external_id,
        incarceration_sentence_id AS sentence_id,
        'INCARCERATION' AS sentence_type,
        SPLIT(external_id, '-')[SAFE_OFFSET(2)] AS court_order_id,
        NULLIF(JSON_EXTRACT_SCALAR(sentence_metadata, '$.CONSECUTIVE_SENTENCE_ID'),'')  AS consec_court_order_id,
    FROM `{project_id}.{state_base_dataset}.state_incarceration_sentence`
    WHERE state_code = 'US_ME'
    
    UNION ALL
    
    SELECT
        person_id,
        state_code,
        external_id,
        supervision_sentence_id AS sentence_id,
        'SUPERVISION' AS sentence_type,
        SPLIT(external_id, '-')[SAFE_OFFSET(2)] AS court_order_id,
        NULLIF(JSON_EXTRACT_SCALAR(sentence_metadata, '$.CONSECUTIVE_SENTENCE_ID'),'')  AS consec_court_order_id,
    FROM `{project_id}.{state_base_dataset}.state_supervision_sentence`
    WHERE state_code = 'US_ME'
    )
    SELECT
        s.person_id,
        s.state_code,
        s.sentence_id,
        s.sentence_type,
        cs.sentence_id AS consecutive_sentence_id
    FROM sentences s
    LEFT JOIN sentences cs
        ON s.state_code = cs.state_code
        AND s.person_id = cs.person_id
        AND s.consec_court_order_id = cs.court_order_id
        -- TODO(#13829): Investigate options for consecutive sentence relationship where supervision sentences are consecutive to incarceration sentences
        AND s.sentence_type = cs.sentence_type
"""

US_ME_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_ME_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_NAME,
    view_query_template=US_ME_CONSECUTIVE_SENTENCES_PREPROCESSED_QUERY_TEMPLATE,
    description=US_ME_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_DESCRIPTION,
    state_base_dataset=STATE_BASE_DATASET,
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.build_and_print()
