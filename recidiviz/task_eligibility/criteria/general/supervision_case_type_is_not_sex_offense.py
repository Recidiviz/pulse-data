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
a supervision case type of "SEX_OFFENSE"."""

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

_CRITERIA_NAME = "SUPERVISION_CASE_TYPE_IS_NOT_SEX_OFFENSE"

_DESCRIPTION = __doc__

_QUERY = f"""
WITH so_case_spans AS (
/* pull spans of time where a client has a sex offense case type */
    SELECT sp.state_code,
      sp.person_id, 
      sp.start_date,
      sp.termination_date AS end_date,
      sc.case_type_raw_text,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` sp
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_case_type_entry` sc 
      USING(person_id, supervision_period_id)
    WHERE sc.case_type = 'SEX_OFFENSE'
      AND start_date IS DISTINCT FROM termination_date -- exclude 0-day spans
), 
/* sub-sessionize and aggregate for cases where a client has multiple SO case types at once */
{create_sub_sessions_with_attributes('so_case_spans')}
SELECT state_code,
  person_id, 
  start_date,
  end_date,
  False AS meets_criteria,
  ARRAY_AGG(DISTINCT case_type_raw_text ORDER BY case_type_raw_text) AS raw_sex_offense_case_types,
  TO_JSON(STRUCT(ARRAY_AGG(DISTINCT case_type_raw_text ORDER BY case_type_raw_text) AS raw_sex_offense_case_types)) AS reason,
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
            name="raw_sex_offense_case_types",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="Array of raw case type values for all SEX_OFFENSE case types that a client is assigned during a given period.",
        ),
    ],
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
