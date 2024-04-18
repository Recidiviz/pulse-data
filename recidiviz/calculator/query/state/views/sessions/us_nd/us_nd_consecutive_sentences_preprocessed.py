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
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ND_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_NAME = (
    "us_nd_consecutive_sentences_preprocessed"
)

US_ND_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_DESCRIPTION = (
    """State-specific preprocessing for ND consecutive sentences"""
)

US_ND_CONSECUTIVE_SENTENCES_PREPROCESSED_QUERY_TEMPLATE = """
    WITH sentences_raw_cte AS
    (
    SELECT 
        SAFE_CAST(SAFE_CAST(REGEXP_REPLACE(OFFENDER_BOOK_ID, r',', '')  AS NUMERIC) AS STRING) AS booking_num,
        SAFE_CAST(SAFE_CAST(CHARGE_SEQ AS INT64) AS STRING) AS seq_num,
        IF(CONSECUTIVE_COUNT_FLAG = 'Y',
            SAFE_CAST(SAFE_CAST(CONSEC_TO_SENTENCE_SEQ AS INT64) AS STRING), NULL) AS consec_seq_num,
    FROM `{project_id}.{raw_dataset}.elite_offendersentences_latest`
    /*
    Very rarely (<0.03% of cases) are duplicated on booking number and sequence number. When this occurs, it is the case
    that one record will have a NULL consecutive sequence number value and another record will have a non-null value.
    The line below deduplicates and prioritizes the non-null value record.
    */
    QUALIFY ROW_NUMBER() OVER(PARTITION BY booking_num, seq_num ORDER BY IFNULL(consec_seq_num, '9999')) = 1
    )
    ,
    sentences_cte AS
    (
    SELECT 
        s.person_id,
        s.state_code,
        s.incarceration_sentence_id AS sentence_id,
        s.external_id,
        'INCARCERATION' AS sentence_type,
        r.booking_num,
        r.seq_num,
        r.consec_seq_num
    FROM `{project_id}.{normalized_state_dataset}.state_incarceration_sentence` s
    LEFT JOIN sentences_raw_cte AS r
        ON SPLIT(s.external_id,'-')[SAFE_OFFSET(0)] = r.booking_num
        AND SPLIT(s.external_id,'-')[SAFE_OFFSET(1)] = r.seq_num
    WHERE state_code = 'US_ND'
    )
    SELECT
        s.person_id,
        s.state_code,
        s.sentence_id,
        s.sentence_type,
        cs.sentence_id AS consecutive_sentence_id
    FROM sentences_cte s
    LEFT JOIN sentences_cte cs
        ON s.person_id = cs.person_id
        AND s.state_code = cs.state_code
        AND s.consec_seq_num = cs.seq_num
        AND s.booking_num = cs.booking_num
"""

US_ND_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_ND_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_NAME,
    view_query_template=US_ND_CONSECUTIVE_SENTENCES_PREPROCESSED_QUERY_TEMPLATE,
    description=US_ND_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_DESCRIPTION,
    raw_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.build_and_print()
