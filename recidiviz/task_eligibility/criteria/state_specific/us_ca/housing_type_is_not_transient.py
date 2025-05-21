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

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#22851): Consider rewording to "Not transient, absconded, or in custody"
_REASON_STRING = "Not transient"

_CRITERIA_NAME = "US_CA_HOUSING_TYPE_IS_NOT_TRANSIENT"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone has a stable
housing type (is not transient)
"""

_QUERY_TEMPLATE = f"""
# TODO(#21353): switch to supervision periods and make a state agnostic criteria
WITH sustainable_housing AS (
  SELECT 
    person_id,
    "US_CA" AS state_code,
    CAST(sp.start_date AS DATE) AS start_date,
    CAST(sp.end_date AS DATE) AS end_date, 
    sp.sustainable_housing
  FROM `{{project_id}}.{{analyst_dataset}}.us_ca_sustainable_housing_status_periods_materialized` sp
  # Sustainable housing is defined as not homeless/transient.
  WHERE sp.sustainable_housing = 1
),

{create_sub_sessions_with_attributes('sustainable_housing')}

SELECT 
  state_code,
  person_id, 
  start_date,
  end_date,
  LOGICAL_OR(TRUE) AS meets_criteria,
  TO_JSON(STRUCT('{{reason_string}}' AS current_housing)) AS reason,
  '{{reason_string}}' AS housing_status,
FROM sub_sessions_with_attributes
WHERE start_date < {nonnull_end_date_clause('end_date')}
GROUP BY 1,2,3,4
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_CA,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        analyst_dataset=ANALYST_VIEWS_DATASET,
        reason_string=_REASON_STRING,
        reasons_fields=[
            ReasonsField(
                name="housing_status",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Housing status of the client.",
            )
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
