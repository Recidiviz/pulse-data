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
"""Defines a criteria span view that displays periods when someone been a student
for at least 90 days
"""

from google.cloud import bigquery

from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    AGGREGATED_EMPLOYMENT_PERIODS_TABLE,
    status_for_at_least_x_time_criteria_query,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "CONTINUOUS_STUDENT_FOR_90_DAYS"

_QUERY_TEMPLATE = status_for_at_least_x_time_criteria_query(
    table_name=AGGREGATED_EMPLOYMENT_PERIODS_TABLE,
    # TODO(#38963): Remove the end_date < '3000-01-01' once we are enforcing that
    #  employment period end dates are reasonable and all exemptions have been
    #  resolved. This filter was added to avoid date overflow when adding time to
    #  dates close to the max date 9999-12-31.
    additional_where_clause="""
        AND employment_status = 'STUDENT'
        AND (end_date IS NULL OR end_date < '3000-01-01')""",
    date_interval=90,
    date_part="DAY",
    end_date="DATE_ADD(end_date, INTERVAL 1 DAY)",
    columns_for_reasons=[
        ("employer_name", "student_facility", "STRING"),
        ("start_date", "student_start_date", "DATE"),
    ],
    extra_columns_for_reasons=[
        ("critical_date_has_passed", _CRITERIA_NAME.lower()),
        ("critical_date", "eligible_date"),
    ],
)

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=__doc__,
    reasons_fields=[
        ReasonsField(
            name="student_facility",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="Name of the facility/school where the student is enrolled",
        ),
        ReasonsField(
            name="student_start_date",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="Student start date",
        ),
        ReasonsField(
            name=_CRITERIA_NAME.lower(),
            type=bigquery.enums.StandardSqlTypeNames.BOOL,
            description="If the client has been a student for the past 90 days or more",
        ),
        ReasonsField(
            name="eligible_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date when 90 consecutive days as a student will be reached",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
