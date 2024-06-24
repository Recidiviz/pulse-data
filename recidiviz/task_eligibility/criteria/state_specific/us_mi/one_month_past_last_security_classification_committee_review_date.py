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
"""Defines a criteria span view that shows spans of time during which one month has passed
from the last security classification committee review.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = (
    "US_MI_ONE_MONTH_PAST_LAST_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_DATE"
)

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which one month has passed
from the last security classification committee (SCC) review. 
"""

_QUERY_TEMPLATE = f"""
WITH critical_date_spans AS (
SELECT 
    ce.state_code,
    ce.person_id,
    ce.completion_event_date AS start_datetime,
    --end_datetime should be the next SCC review date within one facility/solitary confinement collapsed session
    --if there is no next SCC review date, end_datetime should be the end of that housing session 
    COALESCE(LEAD(completion_event_date) OVER
             (PARTITION BY ce.person_id, ss.start_date ORDER BY completion_event_date), ss.end_date_exclusive) AS end_datetime,
    DATE_ADD(completion_event_date, INTERVAL 1 MONTH) AS critical_date
FROM `{{project_id}}.{{analyst_views_dataset}}.us_mi_security_classification_committee_review_materialized` ce
INNER JOIN `{{project_id}}.{{sessions_dataset}}.us_mi_facility_housing_unit_type_collapsed_solitary_sessions` ss
    ON ss.state_code = ce.state_code
    AND ss.person_id = ce.person_id 
    AND ce.completion_event_date BETWEEN ss.start_date AND {nonnull_end_date_exclusive_clause('ss.end_date_exclusive')}
WHERE ce.state_code = 'US_MI'
    AND ss.housing_unit_type_collapsed_solitary = 'SOLITARY_CONFINEMENT'
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
     cd.critical_date AS one_month_past_scc_review_date,
FROM critical_date_has_passed_spans cd
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_MI,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        analyst_views_dataset=ANALYST_VIEWS_DATASET,
        sessions_dataset=SESSIONS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="one_month_past_scc_review_date",
                type=bigquery.enums.SqlTypeNames.DATE,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
