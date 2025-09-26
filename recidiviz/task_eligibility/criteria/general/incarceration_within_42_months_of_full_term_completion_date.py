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
Defines a criteria span view that shows spans of time during which
someone is incarcerated within 42 months of their full term completion date.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#48612): Use is_past_completion_date_criteria_builder once discrepancy is fixed
_CRITERIA_NAME = "INCARCERATION_WITHIN_42_MONTHS_OF_FULL_TERM_COMPLETION_DATE"

_QUERY_TEMPLATE = f"""
WITH critical_date_spans AS (
    SELECT 
        state_code,
        person_id,
        start_date AS start_datetime, 
        end_date_exclusive AS end_datetime,
        projected_completion_date_max AS critical_date
    FROM `{{project_id}}.sessions.incarceration_projected_completion_date_spans_materialized`
),
{critical_date_has_passed_spans_cte(meets_criteria_leading_window_time = 42,
                                    date_part='MONTH')}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(critical_date AS full_term_completion_date)) AS reason,
    critical_date AS full_term_completion_date,
FROM critical_date_has_passed_spans
WHERE start_date != {nonnull_end_date_clause('end_date')}
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=__doc__,
        meets_criteria_default=False,
        reasons_fields=[
            ReasonsField(
                name="full_term_completion_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date when the critical date has passed",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
