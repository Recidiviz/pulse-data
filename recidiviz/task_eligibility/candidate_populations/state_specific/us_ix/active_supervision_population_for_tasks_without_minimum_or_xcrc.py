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
This script selects all spans of time in which a person is a candidate for active supervision
for tasks in the state of Idaho, but it excludes MINIMUM supervision levels and XCRC case
types. Specifically:
- Active supervision: probation, parole, or dual
- Case types: GENERAL, SEX_OFFENSE, or MENTAL_HEALTH_COURT
- Supervision levels: MEDIUM, or HIGH
"""

from recidiviz.task_eligibility.utils.us_ix_query_fragments import (
    us_ix_active_supervision_population_view_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = us_ix_active_supervision_population_view_builder(
    population_name="US_IX_ACTIVE_SUPERVISION_POPULATION_FOR_TASKS_WITHOUT_MINIMUM_OR_XCRC",
    description=__doc__,
    case_types=["GENERAL", "SEX_OFFENSE", "MENTAL_HEALTH_COURT"],
    supervision_levels=["MEDIUM", "HIGH"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
