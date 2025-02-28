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
Defines a criteria span view that shows spans of time during which
someone is incarcerated and their full term completion date is not within 20  years.
"""
from recidiviz.task_eligibility.criteria.general import (
    incarceration_within_20_years_of_full_term_completion_date,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    InvertedTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "NOT_INCARCERATION_WITHIN_20_YEARS_OF_FULL_TERM_COMPLETION_DATE"

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone is incarcerated and their full term completion date is not within 20  years.
"""

VIEW_BUILDER = InvertedTaskCriteriaBigQueryViewBuilder(
    sub_criteria=incarceration_within_20_years_of_full_term_completion_date.VIEW_BUILDER,
).as_criteria_view_builder

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
