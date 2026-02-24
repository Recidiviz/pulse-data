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
"""Defines a criteria span view that shows spans of time during which
someone is either in permanent housing for at least 3 months
"""

from google.cloud import bigquery

from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    status_for_at_least_x_time_criteria_query,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "SUPERVISION_HOUSING_IS_PERMANENT_FOR_3_MONTHS"

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=status_for_at_least_x_time_criteria_query(
        table_name="`{project_id}.normalized_state.state_person_housing_status_period`",
        additional_where_clause="AND housing_status_type IN ('PERMANENT_RESIDENCE')",
        date_interval=3,
        date_part="MONTH",
        start_date="housing_status_start_date",
        end_date="DATE_ADD(housing_status_end_date, INTERVAL 1 DAY)",
        columns_for_reasons=[
            ("housing_status_type", "housing_status_type", "STRING"),
            ("housing_status_start_date", "housing_status_start_date", "DATE"),
        ],
    ),
    description=__doc__,
    reasons_fields=[
        ReasonsField(
            name="housing_status_type",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="Housing status type",
        ),
        ReasonsField(
            name="housing_status_start_date",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="Housing status start date",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
