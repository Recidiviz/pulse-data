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
"""Defines a criterion span view that shows spans of time during which there have been
no supervision sanctions since the latest time someone was on Unassigned supervision level
"""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.task_eligibility.utils.us_tn_query_fragments import (
    supervision_sanctions_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_NO_SUPERVISION_SANCTION_SINCE_UNASSIGNED_SUPERVISION_LEVEL"

_QUERY_TEMPLATE = f"""
WITH spans_between_unassigned_status AS (
    SELECT 
       state_code,
       person_id,
       start_date,
       -- Set the next unassigned start date as the end date 
       LEAD(start_date) OVER(PARTITION BY person_id ORDER BY start_date) AS end_date,
       start_date AS latest_unassigned_start_date,
    FROM `{{project_id}}.sessions.supervision_level_sessions_materialized` sl
    WHERE supervision_level = 'UNASSIGNED'
), 
supervision_sanctions AS (
    {supervision_sanctions_cte}
),
critical_date_spans AS (
    SELECT
        s.state_code,
        s.person_id,
        s.start_date AS start_datetime,
        s.end_date AS end_datetime,
        s.latest_unassigned_start_date,
        MIN(ss.sanction_date) AS critical_date
    FROM spans_between_unassigned_status s
    LEFT JOIN supervision_sanctions ss
        ON s.person_id = ss.person_id
        AND ss.sanction_date BETWEEN s.start_date AND {nonnull_end_date_exclusive_clause('s.end_date')}
    GROUP BY 1,2,3,4,5
),
{critical_date_has_passed_spans_cte(attributes=['latest_unassigned_start_date'])}
SELECT
    state_code,
    person_id, 
    start_date,
    end_date,
    latest_unassigned_start_date,
    NOT critical_date_has_passed AS meets_criteria,
    critical_date AS earliest_sanction_date,
    TO_JSON(STRUCT(critical_date AS earliest_sanction_date,
                    latest_unassigned_start_date AS latest_unassigned_start_date
                    )) AS reason,
FROM critical_date_has_passed_spans

"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    state_code=StateCode.US_TN,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    meets_criteria_default=False,
    reasons_fields=[
        ReasonsField(
            name="earliest_sanction_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date(s) when the first violation report occurred after supervision start",
        ),
        ReasonsField(
            name="latest_unassigned_start_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date of latest unassigned status start",
        ),
    ],
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
