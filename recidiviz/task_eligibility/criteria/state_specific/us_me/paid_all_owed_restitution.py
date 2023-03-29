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

"""
Defines a criteria view that shows spans of time for which clients don't have
any remaining balance in their restitution payments.
"""

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ME_PAID_ALL_OWED_RESTITUTION"

_DESCRIPTION = """
Defines a criteria view that shows spans of time for which clients don't have
any remaining balance in their restitution payments.
"""

_QUERY_TEMPLATE = f"""
WITH fines_fees AS (
    SELECT * 
    FROM `{{project_id}}.{{analyst_dataset}}.us_me_fines_fees_sessions_preprocessed_materialized` 
),

{create_sub_sessions_with_attributes('fines_fees')},

aggregated_fines_fees_per_client AS (
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        fee_type, 
        SUM(unpaid_balance) AS unpaid_balance
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4,5
)

SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    unpaid_balance=0 AS meets_criteria,
    TO_JSON(STRUCT(unpaid_balance AS amount_owed)) AS reason,
FROM aggregated_fines_fees_per_client
WHERE fee_type = 'RESTITUTION'
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_ME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        analyst_dataset=ANALYST_VIEWS_DATASET,
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
