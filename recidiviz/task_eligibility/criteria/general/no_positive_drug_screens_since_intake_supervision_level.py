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
no positive drug screens since the latest time someone started on the 'INTAKE'
supervision level.
"""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "NO_POSITIVE_DRUG_SCREENS_SINCE_INTAKE_SUPERVISION_LEVEL"

_QUERY_TEMPLATE = f"""
WITH spans_between_intake_status AS (
    SELECT 
       state_code,
       person_id,
       start_date,
       -- Set the next intake start date as the end date 
       LEAD(start_date) OVER(PARTITION BY person_id ORDER BY start_date) AS end_date,
       start_date AS latest_intake_start_date,
    FROM `{{project_id}}.sessions.supervision_level_sessions_materialized` sl
    WHERE supervision_level = 'INTAKE'
), 
critical_date_spans AS (
    SELECT
        s.state_code,
        s.person_id,
        s.start_date AS start_datetime,
        s.end_date AS end_datetime,
        s.latest_intake_start_date,
        MIN(d.drug_screen_date) AS critical_date
    FROM spans_between_intake_status s
    LEFT JOIN `{{project_id}}.sessions.drug_screens_preprocessed_materialized` d
        ON s.person_id = d.person_id
        AND d.is_positive_result
        AND d.drug_screen_date BETWEEN s.start_date AND {nonnull_end_date_exclusive_clause('s.end_date')}
    GROUP BY 1,2,3,4,5
),
{critical_date_has_passed_spans_cte(attributes=['latest_intake_start_date'])}
SELECT
    state_code,
    person_id, 
    start_date,
    end_date,
    NOT critical_date_has_passed AS meets_criteria,
    critical_date AS first_positive_screen_date,
    latest_intake_start_date,
    TO_JSON(STRUCT(
        critical_date AS first_positive_screen_date,
        latest_intake_start_date AS latest_intake_start_date
    )) AS reason,
FROM critical_date_has_passed_spans
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="first_positive_screen_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of first positive screen since starting supervision",
            ),
            ReasonsField(
                name="latest_intake_start_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of latest intake status start",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
