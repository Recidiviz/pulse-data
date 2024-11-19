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
Representing the periods of time when a sentence had a serving-type sentence status
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_ID = "sentence_serving_period"

_INDEX_COLUMNS = [
    "state_code",
    "person_id",
    "sentence_id",
]

_VIEW_TEMPLATE = f"""
WITH raw_text_sessions AS (
  SELECT
      {list_to_query_string(_INDEX_COLUMNS)},
      start_date,
      end_date_exclusive,
  FROM `{{project_id}}.{{sentence_sessions_dataset}}.sentence_status_raw_text_sessions_materialized`
  WHERE is_serving_sentence_status
)
{aggregate_adjacent_spans(
    table_name="raw_text_sessions",
    index_columns=_INDEX_COLUMNS,
    end_date_field_name="end_date_exclusive",
)}
"""


SENTENCE_SERVING_PERIOD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SENTENCE_SESSIONS_DATASET,
    view_id=_VIEW_ID,
    view_query_template=_VIEW_TEMPLATE,
    description=__doc__,
    should_materialize=True,
    clustering_fields=_INDEX_COLUMNS,
    sentence_sessions_dataset=SENTENCE_SESSIONS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_SERVING_PERIOD_VIEW_BUILDER.build_and_print()
