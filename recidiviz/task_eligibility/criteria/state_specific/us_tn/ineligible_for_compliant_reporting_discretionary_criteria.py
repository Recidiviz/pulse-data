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
# ============================================================================
"""Describes the spans of time when a TN client may be eligible with discretion for compliant reporting.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.eligibility_spans.us_tn.transfer_to_compliant_reporting_no_discretion import (
    _REQUIRED_CRITERIA,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_INELIGIBLE_FOR_COMPLIANT_REPORTING_DISCRETIONARY_CRITERIA"

_DESCRIPTION = """
Describes the spans of time when a TN client may be eligible with discretion for compliant reporting. 
This means the criteria will be:
- False if a client is eligible without discretion
- False if a client is ineligible without discretion based on a required criteria
- True if a client is ineligible without discretion based on a discretionary criteria
"""


_QUERY_TEMPLATE = """
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        /*
         If the ineligible_criteria contains a required criteria, then this criteria takes on the same value as `is_eligible`
         from `transfer_to_compliant_reporting_no_discretion` (false). If all the required criteria is met,
         then either:
         - the person is eligible without discretion or
         - the person is ineligible without discretion
         So we take the negation of `is_eligible`         
        */
        CASE WHEN REGEXP_CONTAINS(ineligible_criteria2, "{required_criteria}" ) 
             THEN is_eligible 
             ELSE NOT is_eligible END AS meets_criteria,
        TO_JSON(STRUCT(
            ARRAY_AGG(ineligible_criteria2) AS ineligible_criteria
        )) AS reason
    FROM `{project_id}.{task_eligibility_dataset}.transfer_to_compliant_reporting_no_discretion_materialized` tes,
    UNNEST(ineligible_criteria) AS ineligible_criteria2
    WHERE
        tes.state_code = 'US_TN'
    GROUP BY
        1,2,3,4,5
    
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_TN,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
            StateCode.US_TN
        ),
        required_criteria="|".join(
            [criteria.VIEW_BUILDER.criteria_name for criteria in _REQUIRED_CRITERIA]
        ),
    )
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
