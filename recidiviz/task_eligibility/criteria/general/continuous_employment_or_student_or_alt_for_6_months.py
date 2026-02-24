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
"""Defines a criteria span view that displays periods when someone has maintained
continuous employment, been a student, or had an alternative income source for at
least 6 months.
"""

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    EMPLOYED_STATUS_VALUES,
    EMPLOYMENT_REASONS_FIELDS,
    status_for_at_least_x_time_criteria_query,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "CONTINUOUS_EMPLOYMENT_OR_STUDENT_OR_ALT_FOR_6_MONTHS"

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=status_for_at_least_x_time_criteria_query(
        table_name="`{project_id}.normalized_state.state_employment_period`",
        # TODO(#38963): Remove the end_date < '3000-01-01' once we are enforcing that
        #  employment period end dates are reasonable and all exemptions have been
        #  resolved. This filter was added to avoid date overflow when adding time to
        #  dates close to the max date 9999-12-31.
        additional_where_clause=f"""
            AND employment_status IN ({list_to_query_string(EMPLOYED_STATUS_VALUES, quoted=True, single_quote=True)})
            # If end_date is more than 3000-01-01, drop period. Don't drop NULL end_dates
            AND (end_date IS NULL OR end_date < '3000-01-01')""",
        date_interval=6,
        date_part="MONTH",
        end_date="DATE_ADD(end_date, INTERVAL 1 DAY)",
        columns_for_reasons=[
            ("employment_status", "employment_status", "STRING"),
            ("start_date", "employment_start_date", "DATE"),
        ],
    ),
    description=__doc__,
    reasons_fields=EMPLOYMENT_REASONS_FIELDS,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
