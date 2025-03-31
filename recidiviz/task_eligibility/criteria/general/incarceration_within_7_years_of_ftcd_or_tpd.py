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
# ============================================================================
"""
Defines a criteria span view that shows spans of time during which
someone is incarcerated within 7 years of their full term completion date
or tentative parole date.
"""

from recidiviz.task_eligibility.criteria.general import (
    incarceration_within_7_years_of_full_term_completion_date,
    projected_parole_release_date_within_7_years,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    OrTaskCriteriaGroup,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone is incarcerated within 7 years of their full term completion date 
or tentative parole date.
"""

VIEW_BUILDER = OrTaskCriteriaGroup(
    criteria_name="INCARCERATION_WITHIN_7_YEARS_OF_FTCD_OR_TPD",
    sub_criteria_list=[
        incarceration_within_7_years_of_full_term_completion_date.VIEW_BUILDER,
        projected_parole_release_date_within_7_years.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[],
).as_criteria_view_builder

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
