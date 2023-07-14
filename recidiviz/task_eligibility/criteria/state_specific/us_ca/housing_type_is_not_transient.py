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
"""Defines a criteria span view that shows spans of time during which someone has a stable
housing type (is not transient)
"""

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    NORMALIZED_STATE_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_CA_HOUSING_TYPE_IS_NOT_TRANSIENT"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone has a stable
housing type (is not transient)
"""

_QUERY_TEMPLATE = f"""
# TODO(#21353): switch to supervision periods and make a state agnostic criteria
WITH sustainable_housing AS (
  SELECT 
    peid.person_id,
    CAST(sp.start_date AS DATE) AS start_date,
    CAST(sp.end_date AS DATE) AS end_date, 
  FROM `{{project_id}}.{{analyst_dataset}}.us_ca_sustainable_housing_status_periods_materialized` sp
  LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
    ON peid.state_code = 'US_CA'
      AND sp.OffenderId = peid.external_id
  # Sustainable housing is defined as not homeless/transient.
  WHERE sustainable_housing = 1
)

SELECT 
  "US_CA" AS state_code,
  person_id, 
  start_date,
  end_date,
  TRUE AS meets_criteria,
  TO_JSON(STRUCT('Not transient' AS current_housing)) AS reason
FROM sustainable_housing
WHERE start_date != {nonnull_end_date_clause('end_date')}
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_CA,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        analyst_dataset=ANALYST_VIEWS_DATASET,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
