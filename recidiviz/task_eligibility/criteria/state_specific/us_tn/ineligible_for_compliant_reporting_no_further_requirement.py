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

from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
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
        /* This purpose of this criteria is to be the inverse of transfer_to_compliant_reporting_no_discretion. When
        combined with the "required" criteria in the transfer_to_compliant_reporting_with_discretion view, this allows
        us to identify people who are almost eligible (1 criteria away) AND may require some discretion.
        
        There is added complexity in the determination of people who are overdue or coming up for full term discharge.
        If someone fails to meet the SUPERVISION_NOT_PAST_FULL_TERM_COMPLETION_DATE_OR_UPCOMING_90_DAYS criteria, 
        but also fails to meet HAS_ACTIVE_SENTENCE or US_TN_NO_ZERO_TOLERANCE_CODES_SPANS, 
        then they will show up as is_eligible = FALSE in transfer_to_compliant_reporting_no_discretion and
        is_eligible = TRUE in transfer_to_compliant_reporting_with_discretion. This is what we want, because
         we don't actually think they are overdue or coming up for discharge. Thus, they can then be considered eligible 
        for compliant reporting (with discretion, since failing to meet HAS_ACTIVE_SENTENCE or 
        US_TN_NO_ZERO_TOLERANCE_CODES_SPANS also suggests missing sentencing info that could be relevant).
        
        However, if someone fails to meet the SUPERVISION_NOT_PAST_FULL_TERM_COMPLETION_DATE_OR_UPCOMING_90_DAYS criteria,
        but meets both HAS_ACTIVE_SENTENCE AND US_TN_NO_ZERO_TOLERANCE_CODES_SPANS, then they will also show up as
        is_eligible = FALSE in transfer_to_compliant_reporting_no_discretion, which is correct because we think they are
        actually overdue for discharge/upcoming discharge. In that case, we also want is_eligible = FALSE in 
        transfer_to_compliant_reporting_with_discretion, which is why we make the exception below.
        
        */
        
        CASE WHEN "SUPERVISION_NOT_PAST_FULL_TERM_COMPLETION_DATE_OR_UPCOMING_90_DAYS" IN UNNEST(ineligible_criteria) 
             AND "HAS_ACTIVE_SENTENCE" NOT IN UNNEST(ineligible_criteria) 
             AND "US_TN_NO_ZERO_TOLERANCE_CODES_SPANS" NOT IN UNNEST(ineligible_criteria) 
        THEN FALSE
        ELSE NOT is_eligible END AS meets_criteria,
        TO_JSON(STRUCT( (ineligible_criteria) AS ineligible_criteria )) AS reason,
        ineligible_criteria,
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
        reasons_fields=[
            ReasonsField(
                name="ineligible_criteria",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
