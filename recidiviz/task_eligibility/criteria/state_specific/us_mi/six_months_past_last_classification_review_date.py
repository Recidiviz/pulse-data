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
"""Defines a criteria span view that shows spans of time during which someone is past
their subsequent classification review date.
"""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    completion_event_state_specific_dataset,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MI_SIX_MONTHS_PAST_LAST_CLASSIFICATION_REVIEW_DATE"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone is past
their subsequent classification review date, which is every 6 months after their initial review. 
"""

_QUERY_TEMPLATE = f"""
WITH critical_date_spans AS (
SELECT 
    ce.state_code,
    ce.person_id,
    ce.completion_event_date AS start_datetime,
    --end_datetime should be the next classification review date within one supervision super session 
    --if there is no next classification review date, end_datetime should be the end of that supervision super session
    COALESCE(LEAD(completion_event_date) OVER
             (PARTITION BY ce.person_id, sss.start_date ORDER BY completion_event_date), sss.end_date_exclusive) AS end_datetime,
    DATE_ADD(completion_event_date, INTERVAL 6 MONTH) AS critical_date
FROM `{{project_id}}.{{completion_dataset}}.supervision_classification_review_materialized` ce
INNER JOIN `{{project_id}}.{{sessions_dataset}}.supervision_super_sessions_materialized` sss
    ON sss.state_code = ce.state_code
    AND sss.person_id = ce.person_id 
    AND ce.completion_event_date BETWEEN sss.start_date AND {nonnull_end_date_exclusive_clause('sss.end_date_exclusive')}
WHERE ce.state_code = 'US_MI'
),
{critical_date_has_passed_spans_cte()}
SELECT
    cd.state_code,
    cd.person_id,
    cd.start_date,
    cd.end_date,
    cd.critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(
        cd.critical_date AS eligible_date
    )) AS reason,
FROM critical_date_has_passed_spans cd
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_MI,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        completion_dataset=completion_event_state_specific_dataset(StateCode.US_MI),
        sessions_dataset=SESSIONS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
