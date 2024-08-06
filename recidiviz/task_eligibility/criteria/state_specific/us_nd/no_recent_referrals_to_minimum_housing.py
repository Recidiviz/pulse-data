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
# =============================================================================

"""
This criterion checks whether a person has had a recent referral to minimum housing.
The criterion will be false if the person has had a referral and the next review date
is in the future. If there is no next review date, we assume a date based on the 
evaluation result: 3 months from the evaluation date for most cases, and 6 months 
if the evaluation result is 'Approved'.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.dataset_config import (
    completion_event_state_specific_dataset,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_nd_query_fragments import (
    get_minimum_housing_referals_query,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ND_NO_RECENT_REFERRALS_TO_MINIMUM_HOUSING"

_DESCRIPTION = """
This criterion checks whether a person has had a recent referral to minimum housing.
The criterion will be false if the person has had a referral and the next review date
is in the future. If there is no next review date, we assume a date based on the 
evaluation result: 3 months from the evaluation date for most cases, and 6 months 
if the evaluation result is 'Approved'.
"""

_QUERY_TEMPLATE = f"""
{get_minimum_housing_referals_query()},
{create_sub_sessions_with_attributes(table_name='min_referrals_with_external_id_and_ce')}

#TODO(##31660): Make sure we filter out ATP referrals
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    FALSE AS meets_criteria,
    TO_JSON(STRUCT(
        STRING_AGG(evaluation_result, ' - ') AS evaluation_result,
        STRING_AGG(assess_comment_text, ' - ') AS assess_comment_text,
        STRING_AGG(committee_comment_text, ' - ') AS committee_comment,
        MAX(next_review_date) AS next_review_date
    )) AS reason,
    STRING_AGG(evaluation_result, ' - ') AS evaluation_result,
    STRING_AGG(assess_comment_text, ' - ') AS assess_comment_text,
    STRING_AGG(committee_comment_text, ' - ') AS committee_comment_text,
    MAX(next_review_date) AS next_review_date,
FROM sub_sessions_with_attributes
WHERE start_date != end_date
GROUP BY 1,2,3,4,5
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    state_code=StateCode.US_ND,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_completion_events_dataset=completion_event_state_specific_dataset(
        StateCode.US_ND
    ),
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="evaluation_result",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Evaluation result for the referral to minimum housing.",
        ),
        ReasonsField(
            name="assess_comment_text",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Free-text field for comments during the officer assessment portion of the process.",
        ),
        ReasonsField(
            name="committee_comment_text",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Free-text field for comments during the committee approval portion of the process.",
        ),
        ReasonsField(
            name="next_review_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date when the next referral review is due",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
