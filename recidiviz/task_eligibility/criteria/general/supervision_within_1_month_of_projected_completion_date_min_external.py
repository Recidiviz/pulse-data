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
"""
Defines a criterion span view that shows spans of time during which
someone under supervision is within 1 month of their minimum projected completion date.
"""
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    is_past_completion_date_criteria_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "SUPERVISION_WITHIN_1_MONTH_OF_PROJECTED_COMPLETION_DATE_MIN_EXTERNAL"


VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    is_past_completion_date_criteria_builder(
        compartment_level_1_filter="SUPERVISION",
        meets_criteria_leading_window_time=1,
        date_part="MONTH",
        critical_date_name_in_reason="projected_completion_date_min_external",
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
