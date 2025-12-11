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
least three months.
"""

from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    employed_for_at_least_x_time_criteria_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    employed_for_at_least_x_time_criteria_builder(
        date_interval=3,
        date_part="MONTH",
        criteria_name="CONTINUOUS_EMPLOYMENT_OR_STUDENT_OR_ALT_FOR_3_MONTHS",
        description=__doc__,
        employment_status_values=[
            "EMPLOYED_UNKNOWN_AMOUNT",
            "EMPLOYED_FULL_TIME",
            "EMPLOYED_PART_TIME",
            "STUDENT",
            "ALTERNATE_INCOME_SOURCE",
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
