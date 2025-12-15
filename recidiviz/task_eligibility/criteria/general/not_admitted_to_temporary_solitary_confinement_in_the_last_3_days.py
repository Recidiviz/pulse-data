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
"""Defines a criteria span view that shows spans of time during which someone has
past 3 days since their admission from general to temporary solitary confinement.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "NOT_ADMITTED_TO_TEMPORARY_SOLITARY_CONFINEMENT_IN_THE_LAST_3_DAYS"

_QUERY_TEMPLATE = f"""
WITH housing_transitions AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        housing_unit_type,
        LAG(housing_unit_type) OVER (
            PARTITION BY state_code, person_id, date_gap_id
            ORDER BY start_date
        ) AS previous_housing_unit_type
    FROM `{{project_id}}.{{sessions_dataset}}.housing_unit_type_sessions_materialized`
),
critical_date_spans AS (
    SELECT
        state_code,
        person_id,
        start_date AS start_datetime,
        end_date_exclusive AS end_datetime,
        DATE_ADD(start_date, INTERVAL 3 DAY) AS critical_date,
    FROM housing_transitions
    WHERE 
        housing_unit_type = 'TEMPORARY_SOLITARY_CONFINEMENT'
        AND previous_housing_unit_type = 'GENERAL'
),
{critical_date_has_passed_spans_cte()}
    SELECT
        cd.state_code,
        cd.person_id,
        cd.start_date,
        cd.end_date,
        --this criteria is met once 3 consecutive days in temporary solitary confinement have passed
        cd.critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(
            cd.critical_date AS minimum_time_served_date
        )) AS reason,
        cd.critical_date AS minimum_time_served_date,
    FROM critical_date_has_passed_spans cd
"""
VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="minimum_time_served_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date of third consecutive day in temporary solitary confinement",
        ),
    ],
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
