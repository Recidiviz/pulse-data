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
"""US_MA resident metadata"""


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state.dataset_config import (
    SENTENCE_SESSIONS_DATASET,
    WORKFLOWS_VIEWS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MA_RESIDENT_METADATA_VIEW_NAME = "us_ma_resident_metadata"

US_MA_RESIDENT_METADATA_VIEW_DESCRIPTION = """
US_MA resident metadata
"""

US_MA_RESIDENT_METADATA_VIEW_QUERY_TEMPLATE = f"""
WITH dates_and_credits AS
(
SELECT
    person_id,
    start_date AS last_updated_date,
    group_parole_eligibility_date AS rts_date,
    group_projected_full_term_release_date_max AS adjusted_max_release_date,
    -- Get the original max release date from the first span
    FIRST_VALUE(group_projected_full_term_release_date_max) 
        OVER(PARTITION BY state_code, person_id ORDER BY start_date ASC) AS original_max_release_date,
    group_good_time_days AS total_completion_credit,
    group_earned_time_days AS total_state_credit,
FROM `{{project_id}}.{{sentence_sessions_dataset}}.person_projected_date_sessions_materialized`
WHERE state_code = 'US_MA'
-- Only include the current span for each person
QUALIFY CURRENT_DATE BETWEEN start_date AND {nonnull_end_date_exclusive_clause('end_date_exclusive')}
)
,
credit_activity AS
(
SELECT
    person_id,
    ARRAY_AGG(
        STRUCT(
            credit_date,
            credit_type,
            credits_earned,
            JSON_EXTRACT_SCALAR(activity_attributes, "$.activity_code") AS activity_code,
            JSON_EXTRACT_SCALAR(activity_attributes, "$.activity") AS activity,
            JSON_EXTRACT_SCALAR(activity_attributes, "$.activity_type") AS activity_type,
            JSON_EXTRACT_SCALAR(activity_attributes, "$.rating") AS rating
            )
    ORDER BY credit_date, JSON_EXTRACT_SCALAR(activity_attributes, "$.activity")
    ) AS credit_activity
FROM `{{project_id}}.analyst_data.earned_credit_activity_materialized`
WHERE state_code = 'US_MA'
GROUP BY 1
)
SELECT 
    person_id,
    last_updated_date,
    rts_date,
    adjusted_max_release_date,
    original_max_release_date,
    total_completion_credit,
    total_state_credit,
    DATE_DIFF(adjusted_max_release_date, rts_date, DAY) AS total_completion_credit_days_calculated,
    DATE_DIFF(original_max_release_date, adjusted_max_release_date, DAY) AS total_state_credit_days_calculated,
    credit_activity,
FROM dates_and_credits
LEFT JOIN credit_activity
    USING(person_id)
"""

US_MA_RESIDENT_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    view_id=US_MA_RESIDENT_METADATA_VIEW_NAME,
    view_query_template=US_MA_RESIDENT_METADATA_VIEW_QUERY_TEMPLATE,
    description=US_MA_RESIDENT_METADATA_VIEW_DESCRIPTION,
    should_materialize=True,
    sentence_sessions_dataset=SENTENCE_SESSIONS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MA_RESIDENT_METADATA_VIEW_BUILDER.build_and_print()
