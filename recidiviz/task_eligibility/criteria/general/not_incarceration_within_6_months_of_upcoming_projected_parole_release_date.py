# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
someone is NOT within 6 months of their tentative parole date.
Past projected parole release dates AND NULL projected parole release dates 
will satisfy this requirement.
"""
from recidiviz.task_eligibility.criteria.general import (
    incarceration_within_6_months_of_upcoming_projected_parole_release_date,
)
from recidiviz.task_eligibility.inverted_task_criteria_big_query_view_builder import (
    StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = (
    "NOT_INCARCERATION_WITHIN_6_MONTHS_OF_UPCOMING_PROJECTED_PAROLE_RELEASE_DATE"
)

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone is NOT within 6 months of their tentative parole date.
Past projected parole release dates AND NULL projected parole release dates 
will satisfy this requirement.
"""

VIEW_BUILDER = StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder(
    sub_criteria=incarceration_within_6_months_of_upcoming_projected_parole_release_date.VIEW_BUILDER,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
