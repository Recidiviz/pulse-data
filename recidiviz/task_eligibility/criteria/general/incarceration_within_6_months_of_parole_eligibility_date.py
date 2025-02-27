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
# =============================================================================
"""
Defines a criteria span view that shows spans of time during which
someone is incarcerated within 6 months of their parole eligibility date.
"""
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    is_past_completion_date_criteria_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "INCARCERATION_WITHIN_6_MONTHS_OF_PAROLE_ELIGIBILITY_DATE"

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone is incarcerated within 6 months of their parole eligibility date.
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    is_past_completion_date_criteria_builder(
        compartment_level_1_filter="INCARCERATION",
        meets_criteria_leading_window_time=6,
        date_part="MONTH",
        critical_date_column="parole_eligibility_date",
        critical_date_name_in_reason="parole_eligibility_date",
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
