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
state provided sentence groups in an inferred sentence group. 

Note that this is an aggregation of NormalizedStateSentenceGroupLength values 
and does not include any NormalizedStateSentenceLength data.

Related views are:
- inferred_group_aggregated_sentence_projected_dates
- sentence_inferred_group_projected_dates
- TODO(#33498) Create validation for aggregated_sentence_projected_dates 
               and aggregated_sentence_group_projected_dates

Output fields for this view are:

    - sentence_inferred_group_id: 
        The ID for the inferred sentence group. This can be used to link back to the
        constituent sentences and state provided sentence groups.

    - inferred_group_update_datetime:
        This is the datetime where the values in this row begin to be valid.

    - parole_eligibility_date:
        The maximum parole eligibility date across the NormalizedStateSentenceGroupLength
        entities affiliated with this inferred group.

    - projected_parole_release_date:
        The maximum projected parole release date across the NormalizedStateSentenceGroupLength
        entities affiliated with this inferred group. This is when we expect the affiliated 
        person to be released from incarceration to parole.

    - projected_full_term_release_date_min:
        The maximum full term release date (min) across the NormalizedStateSentenceGroupLength
        entities affiliated with this inferred group. This is earliest time we expect all sentences 
        of the affiliated person to be completed and that person to be released to liberty.

    - projected_full_term_release_date_max
        The maximum full term release date (max) across the NormalizedStateSentenceGroupLength
        entities affiliated with this inferred group. This is latest time we expect all sentences 
        of the affiliated person to be completed and that person to be released to liberty.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_ID = (
    "inferred_sentence_group_aggregated_sentence_group_projected_dates"
)

QUERY_TEMPLATE = """
WITH
group_lengths AS (
    SELECT 
        group_update_datetime, 
        sentence_group_id,
        sentence_inferred_group_id,
        parole_eligibility_date_external,
        projected_parole_release_date_external,
        projected_full_term_release_date_min_external,
        projected_full_term_release_date_max_external
    FROM 
        `{project_id}.normalized_state.state_sentence_group_length`
    JOIN 
        `{project_id}.normalized_state.state_sentence_group`
    USING 
        (sentence_group_id)
),
-- We need this to get all critical dates
_inferred_group_level AS (
    SELECT DISTINCT group_update_datetime, sentence_inferred_group_id
    FROM group_lengths
),
-- We need this to get all critical dates
_external_groups_by_inferred_group AS (
    SELECT DISTINCT sentence_group_id, sentence_inferred_group_id
    FROM group_lengths
),
-- This gives us all external group projected dates for each inferred group critical date
forward_filled_data AS (
    SELECT
        group_update_datetime,
        sentence_group_id,
        sentence_inferred_group_id,
        LAST_VALUE(parole_eligibility_date_external IGNORE NULLS) OVER group_window AS parole_eligibility_date_external,
        LAST_VALUE(projected_parole_release_date_external IGNORE NULLS) OVER group_window AS projected_parole_release_date_external,
        LAST_VALUE(projected_full_term_release_date_min_external IGNORE NULLS) OVER group_window AS projected_full_term_release_date_min_external,
        LAST_VALUE(projected_full_term_release_date_max_external IGNORE NULLS) OVER group_window AS projected_full_term_release_date_max_external
    FROM
        _inferred_group_level
    FULL JOIN
        _external_groups_by_inferred_group
    USING
        (sentence_inferred_group_id)
    LEFT JOIN
        group_lengths
    USING
        (sentence_group_id, sentence_inferred_group_id, group_update_datetime)
    WINDOW group_window AS (PARTITION BY sentence_group_id ORDER BY group_update_datetime)
)
SELECT 
    sentence_inferred_group_id,
    group_update_datetime AS inferred_group_update_datetime,
    MAX(parole_eligibility_date_external) as parole_eligibility_date,
    MAX(projected_parole_release_date_external) as projected_parole_release_date,
    MAX(projected_full_term_release_date_min_external) as projected_full_term_release_date_min,
    MAX(projected_full_term_release_date_max_external) as projected_full_term_release_date_max
FROM
    forward_filled_data
GROUP BY
    sentence_inferred_group_id, group_update_datetime
"""


INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_BUILDER = (
    SimpleBigQueryViewBuilder(
        dataset_id=SENTENCE_SESSIONS_DATASET,
        view_id=INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_ID,
        view_query_template=QUERY_TEMPLATE,
        description=__doc__,
        should_materialize=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_BUILDER.build_and_print()
