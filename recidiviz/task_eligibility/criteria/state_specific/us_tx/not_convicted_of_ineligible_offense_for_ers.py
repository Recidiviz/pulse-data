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
"""
Defines a criteria view that shows spans of time when clients do not have any current or prior convictions for offenses
involving use of a child, sexual assault, or bodily injury.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TX_NOT_CONVICTED_OF_INELIGIBLE_OFFENSE_FOR_ERS"

_REASON_QUERY = f"""
WITH ineligible_spans AS (
  SELECT
    state_code,
    person_id,
    date_charged AS start_date,
    CAST(NULL AS DATE) AS end_date,
    FALSE AS meets_criteria,
    CONCAT(statute, ' – ', description) AS ineligible_offense
  FROM `{{project_id}}.{{normalized_state_dataset}}.state_charge_v2`
  WHERE state_code = 'US_TX'
    AND (
      TRIM(statute, '0') IN (
        '19.02','19.03',    -- Murder, Capital Murder
        '21.02','21.11',    -- Continuous Sexual Abuse, Indecency w/ Child
        '43.25','43.26',    -- Sexual Performance by Child, Child Pornography
        '481.140',          -- Use of Child in Drug Offense
        '22.011','22.021',  -- Sexual Assault, Aggravated Sexual Assault
        '22.02','29.03','20.04'  -- Aggravated Assault, Aggravated Robbery, Aggravated Kidnapping
      )
      OR (
        UPPER(description) LIKE '%ASSAULT%'
        OR UPPER(description) LIKE '%ASLT%'
        OR UPPER(description) LIKE '%AGG%'
        OR UPPER(description) LIKE '%BODILY INJURY%'
        OR UPPER(description) LIKE '%SEXUAL ASSAULT%'
        OR UPPER(description) LIKE '%SEXUAL CONDUCT%'
        OR UPPER(description) LIKE '%MURDER%'
      )
    )
),

{create_sub_sessions_with_attributes('ineligible_spans')}
SELECT * except (ineligible_offense),
     TO_JSON(STRUCT(ARRAY_AGG(DISTINCT ineligible_offense ORDER BY ineligible_offense) AS ineligible_offenses)) AS reason,
    ARRAY_AGG(DISTINCT ineligible_offense ORDER BY ineligible_offense) AS ineligible_offenses,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4,5
"""

VIEW_BUILDER = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    criteria_spans_query_template=_REASON_QUERY,
    state_code=StateCode.US_TX,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="ineligible_offenses",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="List of offenses that make a client ineligible (use of child, sexual assault, or bodily harm).",
        )
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
