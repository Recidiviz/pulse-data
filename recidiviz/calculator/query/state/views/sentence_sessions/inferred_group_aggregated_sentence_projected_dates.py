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
Creates a table where each row are the aggregated projected dates for all 
individual sentences in an inferred sentence group. 

Note that this is an aggregation of NormalizedStateSentenceLength values 
and does not include any NormalizedStateSentenceGroupLength data.

Related views are:
- inferred_sentence_group_aggregated_sentence_projected_dates
- sentence_inferred_group_projected_dates
- inferred_group_aggregated_projected_dates_validation

Rows can have null projected dates if the inferred sentence group consists
of only SUSPENDED sentences at the given inferred_group_update_datetime.

Output fields for this view are:

    - sentence_inferred_group_id: 
        The ID for the inferred sentence group. This can be used to link back to the
        constituent sentences and state provided sentence groups.

    - inferred_group_update_datetime:
        This is the datetime where the values in this row begin to be valid.

    - parole_eligibility_date:
        The maximum parole eligibility date across the NormalizedStateSentenceLength
        entities affiliated with this inferred group.

    - projected_parole_release_date:
        The maximum projected parole release date across the NormalizedStateSentenceLength
        entities affiliated with this inferred group. This is when we expect the affiliated 
        person to be released from incarceration to parole.

    - projected_full_term_release_date_min:
        The maximum projected completion date (min) across the NormalizedStateSentenceLength
        entities affiliated with this inferred group. This is earliest time we expect all sentences 
        of the affiliated person to be completed and that person to be released to liberty.

    - projected_full_term_release_date_max
        The maximum projected completion date (max) across the NormalizedStateSentenceLength
        entities affiliated with this inferred group. This is latest time we expect all sentences 
        of the affiliated person to be completed and that person to be released to liberty.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_ID = (
    "inferred_sentence_group_aggregated_sentence_projected_dates"
)

QUERY_TEMPLATE = """
WITH 
-- Statuses with a critical date to join to lengths
sentence_statuses AS (
    SELECT
        state_code,
        sentence_id,
        status_update_datetime AS critical_dt,
        status
    FROM `{project_id}.normalized_state.state_sentence_status_snapshot`
),
-- Lengths with a critical date to join to statuses
sentence_lengths AS (
    SELECT
        state_code,
        sentence_id,
        length_update_datetime AS critical_dt,
        parole_eligibility_date_external,
        projected_parole_release_date_external,
        projected_completion_date_min_external,
        projected_completion_date_max_external
    FROM `{project_id}.normalized_state.state_sentence_length`
),
-- Each row here is a sentence level critical date with its corresponding values
sentence_level_data AS (
    SELECT
        state_code,
        critical_dt,
        sentence_id,
        sentence_inferred_group_id,
        status,
        parole_eligibility_date_external,
        projected_parole_release_date_external,
        projected_completion_date_min_external,
        projected_completion_date_max_external
    FROM 
        sentence_statuses
    FULL JOIN 
        sentence_lengths
    USING 
        (state_code, sentence_id, critical_dt)
    JOIN 
        `{project_id}.normalized_state.state_sentence`
    USING
        (state_code, sentence_id)
),
-- We need this to get all group level critical dates to each sentence
_group_level AS (
    SELECT DISTINCT state_code, critical_dt, sentence_inferred_group_id
    FROM sentence_level_data
),
-- We need this to get all group level critical dates to each sentence
_sentences_by_group AS (
    SELECT DISTINCT state_code, sentence_id, sentence_inferred_group_id
    FROM sentence_level_data
),
-- This gives us the status and most recent projected dates for each group level critical date
-- for all sentences.
forward_filled_data AS (
    SELECT
        state_code,
        critical_dt,
        sentence_id,
        sentence_inferred_group_id,
        LAST_VALUE(status IGNORE NULLS) OVER sentence_window AS status,
        LAST_VALUE(parole_eligibility_date_external IGNORE NULLS) OVER sentence_window AS parole_eligibility_date_external,
        LAST_VALUE(projected_parole_release_date_external IGNORE NULLS) OVER sentence_window AS projected_parole_release_date_external,
        LAST_VALUE(projected_completion_date_min_external IGNORE NULLS) OVER sentence_window AS projected_completion_date_min_external,
        LAST_VALUE(projected_completion_date_max_external IGNORE NULLS) OVER sentence_window AS projected_completion_date_max_external
    FROM
        _group_level
    FULL JOIN
        _sentences_by_group
    USING
        (state_code, sentence_inferred_group_id)
    LEFT JOIN
        sentence_level_data
    USING
        (state_code, sentence_id, sentence_inferred_group_id, critical_dt)
    WINDOW sentence_window AS (PARTITION BY sentence_id ORDER BY critical_dt)
),
-- This nulls out projected dates for suspended sentence spans so they are not included in 
-- aggregation, and also removes all rows before the first projected date is hydrated.
pre_aggregated_sentence_projected_dates AS (
  SELECT
      state_code,
      sentence_inferred_group_id,
      sentence_id,
      critical_dt,
      IF(status != 'SUSPENDED', parole_eligibility_date_external, null) AS parole_eligibility_date_external,
      IF(status != 'SUSPENDED', projected_parole_release_date_external, null) AS projected_parole_release_date_external,
      IF(status != 'SUSPENDED', projected_completion_date_min_external, null) AS projected_completion_date_min_external,
      IF(status != 'SUSPENDED', projected_completion_date_max_external, null) AS projected_completion_date_max_external
  FROM 
      forward_filled_data
  WHERE (
      parole_eligibility_date_external IS NOT NULL
    OR
      projected_parole_release_date_external IS NOT NULL
    OR
      projected_completion_date_min_external IS NOT NULL
    OR
      projected_completion_date_max_external IS NOT NULL
  )
)
SELECT
    state_code,
    sentence_inferred_group_id,
    critical_dt AS inferred_group_update_datetime,
    MAX(parole_eligibility_date_external) as parole_eligibility_date,
    MAX(projected_parole_release_date_external) as projected_parole_release_date,
    MAX(projected_completion_date_min_external) as projected_full_term_release_date_min,
    MAX(projected_completion_date_max_external) as projected_full_term_release_date_max
FROM
    pre_aggregated_sentence_projected_dates
GROUP BY
    state_code, sentence_inferred_group_id, critical_dt
"""


INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_BUILDER = (
    SimpleBigQueryViewBuilder(
        dataset_id=SENTENCE_SESSIONS_DATASET,
        view_id=INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_ID,
        view_query_template=QUERY_TEMPLATE,
        description=__doc__,
        should_materialize=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_BUILDER.build_and_print()
