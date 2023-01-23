# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Defines a criteria span view that shows spans of time for
which a client has a supervision type of NON IDAHO COMMITMENT.
"""

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_NOT_A_NON_IDAHO_COMMITMENT"

_DESCRIPTION = """Defines a criteria span view that shows spans of time
for which a client has a supervision type of a non idaho commitment """

_QUERY_TEMPLATE = f"""
WITH non_commitment_periods AS (
SELECT
    ssp.state_code,
    ssp.person_id,
    ssp.start_date,
    DATE_ADD(ssp.termination_date, INTERVAL 1 DAY) AS end_date,
    FALSE AS meets_criteria,
FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` ssp
WHERE state_code = "US_IX"
AND supervision_type_raw_text = "NON IDAHO COMMITMENT"
),
{create_sub_sessions_with_attributes('non_commitment_periods')}
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    LOGICAL_AND(meets_criteria) AS meets_criteria,
    TO_JSON(STRUCT("NON IDAHO COMMITMENT" AS supervision_type_raw_text)) AS reason,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4
"""
VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_IX,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
