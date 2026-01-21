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
for tasks in the state of Idaho AND is able to work. Specifically, it selects people with the
following conditions:
- Active supervision: probation, parole, or dual
- Case types: GENERAL, SEX_OFFENSE, XCRC, or MENTAL_HEALTH_COURT
- Supervision levels: MINIMUM, MEDIUM, HIGH, or XCRC
- Not marked as unable to work (meets is_able_to_work criteria)
"""

from recidiviz.task_eligibility.utils.us_ix_query_fragments import (
    us_ix_active_supervision_population_view_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# CTE that returns spans where someone is able to work (meets_criteria = TRUE)
_ABLE_TO_WORK_CTE = """able_to_work AS (
    -- Get spans where someone is able to work by joining with the is_able_to_work criteria.
    -- The is_able_to_work criteria has meets_criteria_default=True, so we only need to
    -- include spans where meets_criteria = TRUE.
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
    FROM `{project_id}.task_eligibility_criteria_us_ix.is_able_to_work_materialized`
    WHERE meets_criteria = TRUE
)"""

VIEW_BUILDER = us_ix_active_supervision_population_view_builder(
    population_name="US_IX_ACTIVE_SUPERVISION_POPULATION_FOR_TASKS_ABLE_TO_WORK",
    description=__doc__,
    case_types=["GENERAL", "SEX_OFFENSE", "XCRC", "MENTAL_HEALTH_COURT"],
    supervision_levels=["MINIMUM", "MEDIUM", "HIGH", "XCRC"],
    additional_cte=_ABLE_TO_WORK_CTE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
