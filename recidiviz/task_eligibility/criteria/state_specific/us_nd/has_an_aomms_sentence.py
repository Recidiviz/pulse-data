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

"""Defines a criteria view that shows spans of time for which residents are serving an
Armed Offender Minimum Mandatory Sentence (AOMMS).
"""
from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_nd_query_fragments import reformat_ids
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ND_HAS_AN_AOMMS_SENTENCE"

_DESCRIPTION = """Defines a criteria view that shows spans of time for which residents are serving an
Armed Offender Minimum Mandatory Sentence (AOMMS).
"""

_QUERY_TEMPLATE = f"""
WITH cleaned_raw_data AS (
  SELECT 
    {reformat_ids('OFFENDER_BOOK_ID')} AS OFFENDER_BOOK_ID,
    {reformat_ids('ORDER_ID')} AS ORDER_ID,
    * EXCEPT (OFFENDER_BOOK_ID, ORDER_ID)
  FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.elite_offenderchargestable_latest`
  -- 2 conditions need to be met for a person to be considered for AOMMS
  -- 1. Minimum Mandatory Flag is 'Y'
  WHERE MIN_MAN_FLAG = 'Y'
  -- 2. There's a comment containing the reference to the minimum mandatory sentence
    AND COMMENT_TEXT IS NOT NULL
    AND REGEXP_CONTAINS(UPPER(COMMENT_TEXT), r'(MIN|MAN)') 
), 
connect_charge_to_sentence AS (
  SELECT 
    *
  FROM cleaned_raw_data
  LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.elite_offendersentences_latest` raw_sent
  ON(cleaned_raw_data.OFFENDER_BOOK_ID = {reformat_ids('raw_sent.OFFENDER_BOOK_ID')}
    AND cleaned_raw_data.CHARGE_SEQ = raw_sent.CHARGE_SEQ)
  LEFT JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` s
  ON(CONCAT({reformat_ids('raw_sent.OFFENDER_BOOK_ID')}, '-', raw_sent.SENTENCE_SEQ) = s.external_id)
)
SELECT
    span.state_code,
    span.person_id,
    span.start_date,
    span.end_date,
    FALSE AS meets_criteria,
    TO_JSON(STRUCT('Minimum Mandatory Sentence' AS ineligible_offenses)) AS reason,
    'Minimum Mandatory Sentence' AS ineligible_offenses,
FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
UNNEST (sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id
INNER JOIN connect_charge_to_sentence sent
    USING (state_code, person_id, sentences_preprocessed_id)
INNER JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sess
    ON span.state_code = sess.state_code
    AND span.person_id = sess.person_id
    -- Restrict to spans that overlap with particular compartment levels
    AND compartment_level_1 in ("INCARCERATION")
    -- Use strictly less than for exclusive end_dates
    AND span.start_date < IFNULL(sess.end_date_exclusive, "9999-12-31")
    AND sess.start_date < IFNULL(span.end_date_exclusive, "9999-12-31")
WHERE span.state_code = 'US_ND'
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_ND,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
        ),
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=False,
        reasons_fields=[
            ReasonsField(
                name="ineligible_offenses",
                type=bigquery.enums.SqlTypeNames.STRING,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
