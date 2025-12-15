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
someone is NOT within 3 years of their projected parole release date.
Projected parole release dates in the past  will not satisfy this requirement.
"""
from recidiviz.task_eligibility.criteria.general import (
    incarceration_within_3_years_of_projected_parole_release_date,
)
from recidiviz.task_eligibility.inverted_task_criteria_big_query_view_builder import (
    StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone is NOT within 3 years of their projected parole release date.
Projected parole release dates in the past  will not satisfy this requirement.
"""

VIEW_BUILDER = StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder(
    sub_criteria=incarceration_within_3_years_of_projected_parole_release_date.VIEW_BUILDER,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
