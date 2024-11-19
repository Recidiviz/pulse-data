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
The set of sentences with a serving/active status type for each person and time span
formed from sub-sessionizing the sentence serving periods for all overlapping sentences
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

OVERLAPPING_SENTENCE_SERVING_PERIODS_VIEW_ID = "overlapping_sentence_serving_periods"

QUERY_TEMPLATE = f"""
WITH
serving_periods AS (
    -- Sentences that have been imposed and were being served during the span
    SELECT
      state_code,
      person_id,
      sentence_id,
      statuses.start_date,
      statuses.end_date_exclusive,
    FROM
        `{{project_id}}.{{sentence_sessions_dataset}}.sentence_serving_period_materialized` statuses
    LEFT JOIN `{{project_id}}.{{sentence_sessions_dataset}}.sentences_and_charges_materialized` sentences
        USING (state_code, person_id, sentence_id)
    WHERE
        -- Drop any status sessions that end before the sentence imposed date
        imposed_date <= {nonnull_end_date_clause("statuses.end_date_exclusive")}
),
{create_sub_sessions_with_attributes(
    table_name="serving_periods",
    end_date_field_name="end_date_exclusive",
)}
SELECT
    state_code,
    person_id,
    start_date,
    end_date_exclusive,
    ARRAY_AGG(
        DISTINCT sentence_id IGNORE NULLS ORDER BY sentence_id
    ) AS sentence_id_array,
FROM sub_sessions_with_attributes
GROUP BY 1, 2, 3, 4
"""

OVERLAPPING_SENTENCE_SERVING_PERIODS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SENTENCE_SESSIONS_DATASET,
    view_id=OVERLAPPING_SENTENCE_SERVING_PERIODS_VIEW_ID,
    view_query_template=QUERY_TEMPLATE,
    description=__doc__,
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
    sentence_sessions_dataset=SENTENCE_SESSIONS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OVERLAPPING_SENTENCE_SERVING_PERIODS_VIEW_BUILDER.build_and_print()
