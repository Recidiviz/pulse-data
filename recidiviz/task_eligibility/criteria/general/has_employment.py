# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Defines a criteria span view that shows spans of time during which someone has a 
stable employment
"""

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "HAS_EMPLOYMENT"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone has a stable
employment type
"""

_QUERY_TEMPLATE = f"""
WITH employment AS (
  SELECT 
    person_id,
    state_code,
    start_date,
    end_date, 
    CONCAT(IFNULL(employment_status, 'NULL'), '@@',
           IFNULL(employer_name, 'NULL'), '@@',
           CAST(start_date AS STRING)) AS status_employer_start_date
  FROM `{{project_id}}.{{normalized_state_dataset}}.state_employment_period`
  WHERE employment_status IN ('EMPLOYED','EMPLOYED_FULL_TIME','EMPLOYED_PART_TIME')
),

{create_sub_sessions_with_attributes('employment')}

SELECT 
  state_code,
  person_id, 
  start_date,
  end_date,
  TRUE AS meets_criteria,
  TO_JSON(STRUCT(ARRAY_AGG(DISTINCT status_employer_start_date) AS status_employer_start_date)) AS reason
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
