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
"""State-specific preprocessing for TN consecutive sentences"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_NAME = (
    "us_tn_consecutive_sentences_preprocessed"
)

US_TN_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_DESCRIPTION = (
    """State-specific preprocessing for TN consecutive sentences"""
)

US_TN_CONSECUTIVE_SENTENCES_PREPROCESSED_QUERY_TEMPLATE = """
    WITH sentences AS 
    (
    SELECT
        person_id,
        state_code,
        incarceration_sentence_id AS sentence_id,
        'INCARCERATION' AS sentence_type,
        external_id,
        JSON_EXTRACT_SCALAR(sentence_metadata, '$.CONSECUTIVE_SENTENCE_ID') AS consecutive_sentence_external_id,
    FROM `{project_id}.{normalized_state_dataset}.state_incarceration_sentence`
    WHERE state_code = 'US_TN'
    
    UNION ALL 
    
    SELECT
        person_id,
        state_code,
        supervision_sentence_id AS sentence_id,
        'SUPERVISION' AS sentence_type,
        external_id,
        JSON_EXTRACT_SCALAR(sentence_metadata, '$.CONSECUTIVE_SENTENCE_ID') AS consecutive_sentence_external_id,
    FROM `{project_id}.{normalized_state_dataset}.state_supervision_sentence`
    WHERE state_code = 'US_TN'
    )
    ,
    sentences_dedup AS
    (
    /*
    Dedup to external_id prioritizing the incarceration sentence. This is the same dedup methodology that is implemented
    in `us_tn_sentences_preprocessed`. Without this dedup step, we will incorrectly drop sentence imposed groups when we
    have a supervision sentence and an incarceration sentence with the same external_id and additional supervision 
    sentences that marked as consecutive to that external id.
    */
    SELECT 
        * 
    FROM sentences
    QUALIFY ROW_NUMBER() OVER(PARTITION BY external_id ORDER BY IF(sentence_type = 'INCARCERATION',0,1))=1
    )
    SELECT
        s.person_id,
        s.state_code,
        s.sentence_id,
        s.sentence_type,
        cs.sentence_id AS consecutive_sentence_id
    FROM sentences_dedup s
    LEFT JOIN sentences_dedup cs
        ON s.consecutive_sentence_external_id = cs.external_id
        ---TODO(#13829): Investigate options for consecutive sentence relationship where supervision sentences are consecutive to incarceration sentences
        AND s.sentence_type = cs.sentence_type
"""

US_TN_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_TN_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_NAME,
    view_query_template=US_TN_CONSECUTIVE_SENTENCES_PREPROCESSED_QUERY_TEMPLATE,
    description=US_TN_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.build_and_print()
