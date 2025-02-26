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
"""Describes the spans of time when a TN client may be eligible either with discretion or almost-eligible
 for a required criteria. This criteria is true if and only if a person is not already eligible without discretion
 or further action/requirement
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_INELIGIBLE_FOR_COMPLIANT_REPORTING_NO_FURTHER_REQUIREMENT"

_DESCRIPTION = """
Describes the spans of time when a TN client may be eligible either with discretion or almost-eligible
 for a required criteria. This criteria is true if and only if a person is not already eligible without discretion
 or further action/requirement
"""


_QUERY_TEMPLATE = """
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        NOT is_eligible AS meets_criteria,
        TO_JSON(STRUCT( (ineligible_criteria) AS ineligible_criteria )) AS reason
    FROM `{project_id}.{task_eligibility_dataset}.transfer_to_compliant_reporting_no_discretion_materialized` tes
    WHERE
        tes.state_code = 'US_TN'
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
    )
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
