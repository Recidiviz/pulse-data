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
someone has a mandatory override and is already in MEDIUM or MINIMUM custody.
"""
from recidiviz.task_eligibility.criteria.general import (
    custody_level_is_minimum_or_medium,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    mandatory_overrides_for_reclassification,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    AndTaskCriteriaGroup,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone has a mandatory override and is already in MEDIUM or MINIMUM custody.
"""

VIEW_BUILDER = AndTaskCriteriaGroup(
    criteria_name="US_IX_ALREADY_ON_LOWEST_ELIGIBLE_CUSTODY_LEVEL",
    sub_criteria_list=[
        mandatory_overrides_for_reclassification.VIEW_BUILDER,
        custody_level_is_minimum_or_medium.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[],
).as_criteria_view_builder

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
