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
in a minimimum security facility.
"""
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.task_eligibility.utils.us_nd_query_fragments import (
    MINIMUM_SECURITY_FACILITIES,
)
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
)

_CRITERIA_NAME = "US_ND_NOT_IN_MINIMUM_SECURITY_FACILITY"

_DESCRIPTION = """Describes the spans of time during which someone in ND is not
in a minimimum security facility."""

_QUERY_TEMPLATE = f"""
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    FALSE AS meets_criteria,
    TO_JSON(STRUCT(start_date AS minimum_facility_start_date)) AS reason,
FROM `{{project_id}}.{{sessions_dataset}}.location_sessions_materialized`
WHERE state_code = 'US_ND'
  AND facility IN {tuple(MINIMUM_SECURITY_FACILITIES)}
  AND start_date < {nonnull_end_date_clause('end_date')}
GROUP BY 1,2,3,4
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_ND,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
