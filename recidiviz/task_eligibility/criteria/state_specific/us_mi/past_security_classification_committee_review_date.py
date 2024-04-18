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
"""Defines a criteria span view that shows spans of time during which someone is eligible
for a security classification review"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    combining_several_criteria_into_one,
    extract_object_from_json,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MI_PAST_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_DATE"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone is eligible
for a security classification review. A resident is eligible for: 
    1. An SCC review in the first week they are in solitary confinement 
    2. AN SCC review every 30 days afterwards

Residents can be eligible based on the number of expected reviews being greater than the number of observed, OR 
because the last SCC review date was more than 30 days ago."""

_CRITERIA_QUERY_1 = """
    SELECT
        * EXCEPT (reason),
        NULL AS facility_solitary_start_date,
        NULL AS latest_scc_review_date,
        NULL AS number_of_expected_reviews,
        NULL AS number_of_reviews
    FROM `{project_id}.{task_eligibility_criteria_us_mi}.one_month_past_last_security_classification_committee_review_date_materialized`
    """

_CRITERIA_QUERY_2 = f"""
    SELECT
        * EXCEPT (reason),
        {extract_object_from_json(object_column = 'facility_solitary_start_date', 
                                  object_type = 'DATE')} AS facility_solitary_start_date,
        {extract_object_from_json(object_column = 'latest_scc_review_date', 
                                  object_type = 'DATE')} AS latest_scc_review_date,           
        {extract_object_from_json(object_column = 'number_of_expected_reviews', 
                                  object_type = 'INT64')} AS number_of_expected_reviews,
        {extract_object_from_json(object_column = 'number_of_reviews', 
                                  object_type = 'INT64')} AS number_of_reviews,             
    FROM `{{project_id}}.{{task_eligibility_criteria_us_mi}}.expected_number_of_security_classification_committee_reviews_greater_than_observed_materialized`
"""


_JSON_CONTENT = """MIN(facility_solitary_start_date) AS facility_solitary_start_date,
                    MAX(latest_scc_review_date) AS latest_scc_review_date,
                    MAX(number_of_expected_reviews) AS number_of_expected_reviews,
                    MAX(number_of_reviews) AS number_of_reviews,
                    IF(LOGICAL_OR(meets_criteria), start_date, NULL) AS next_scc_date"""

_QUERY_TEMPLATE = f"""
{combining_several_criteria_into_one(
        select_statements_for_criteria_lst=[_CRITERIA_QUERY_1,
                                             _CRITERIA_QUERY_2],
        meets_criteria="LOGICAL_OR(meets_criteria)",
        json_content=_JSON_CONTENT,
    )}
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    state_code=StateCode.US_MI,
    task_eligibility_criteria_us_mi=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_MI
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
