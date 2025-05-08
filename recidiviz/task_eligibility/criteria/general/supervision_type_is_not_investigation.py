# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Defines a criteria span view that shows spans of time during which clients does not have
a supervision type of "INVESTIGATION" (used to refer to pre-trial investigation periods)"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "SUPERVISION_TYPE_IS_NOT_INVESTIGATION"

_DESCRIPTION = __doc__

_QUERY = f"""
WITH investigation_spans AS (
/* pull spans of time where a client has an investigation supervision type */
    SELECT state_code,
      person_id, 
      start_date,
      termination_date AS end_date,
      supervision_type_raw_text,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` sp
    WHERE supervision_type = 'INVESTIGATION'
      AND start_date IS DISTINCT FROM termination_date -- exclude 0-day spans
), 
/* sub-sessionize and aggregate for cases where a client has multiple investigation supervision periods at once */
{create_sub_sessions_with_attributes('investigation_spans')}
SELECT state_code,
  person_id, 
  start_date,
  end_date,
  False AS meets_criteria,
  ARRAY_AGG(DISTINCT supervision_type_raw_text ORDER BY supervision_type_raw_text) AS raw_supervision_types,
  TO_JSON(STRUCT(ARRAY_AGG(DISTINCT supervision_type_raw_text ORDER BY supervision_type_raw_text) AS raw_supervision_types)) AS reason,
FROM sub_sessions_with_attributes
GROUP BY 1, 2, 3, 4
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_QUERY,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="raw_supervision_types",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="Array of raw supervision type values for the given investigation supervision period",
        ),
    ],
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
