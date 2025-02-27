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
"""Defines a criteria span view that shows spans of time during which
someone is past their supervision full term completion date (projected max completion
date) or within 90 days of their full term completion date.
"""
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    is_past_completion_date_criteria_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "SUPERVISION_PAST_FULL_TERM_COMPLETION_DATE_OR_UPCOMING_90_DAYS"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone is past their supervision full term completion date (projected max completion
date) or within 90 days of their full term completion date"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    is_past_completion_date_criteria_builder(
        meets_criteria_leading_window_time=90,
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
