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
# ============================================================================
"""Describes the spans of time during which someone in ND is not
in an orientation unit or has been in an orientation unit for 3 weeks. 
These are units that house people who have just arrived to the DOC and are not yet 
eligible for many opportunities because of this.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ND_NOT_IN_AN_ORIENTATION_UNIT"

_QUERY_TEMPLATE = f"""
SELECT 
    state_code,
    person_id,
    start_date,
    LEAST(
        {nonnull_end_date_clause('end_date_exclusive')},
        DATE_ADD(start_date, INTERVAL 3 WEEK)
    ) AS end_date,
    FALSE AS meets_criteria,
    TO_JSON(STRUCT(start_date AS housing_unit_start_date,
                   ANY_VALUE(housing_unit) AS housing_unit)) AS reason,
    start_date AS housing_unit_start_date,
    ANY_VALUE(housing_unit) AS housing_unit,
FROM `{{project_id}}.{{sessions_dataset}}.housing_unit_sessions_materialized`
WHERE state_code = 'US_ND'
    -- Folks on ORU in NDSP are in an orientation unit
    AND ((facility = 'NDSP' AND REGEXP_CONTAINS(housing_unit, r'ORU'))
    -- Folks on HZN-E in DWCRC are in an orientation unit
        OR (facility = 'DWCRC' AND REGEXP_CONTAINS(housing_unit, r'HZN-E')))
    AND start_date < {nonnull_end_date_exclusive_clause('end_date_exclusive')}
GROUP BY 1,2,3,4,5
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_ND,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=__doc__,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="housing_unit_start_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date when the person was placed in an orientation unit.",
            ),
            ReasonsField(
                name="housing_unit",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="The housing unit where the person was placed.",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
