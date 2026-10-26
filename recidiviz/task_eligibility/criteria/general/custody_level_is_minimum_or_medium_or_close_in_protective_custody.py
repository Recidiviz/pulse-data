# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
Defines a criteria span view identifying periods when an individual
meets at least one of the following conditions:

1. Has a custody level of minimum or medium
2. Has a custody level of close and is housed in protective custody

"""

from recidiviz.task_eligibility.criteria.general import (
    custody_level_is_close,
    custody_level_is_minimum_or_medium,
    housing_unit_type_is_protective_custody,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateAgnosticTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CUSTODY_LEVEL_IS_CLOSE_AND_IN_PROTECTIVE_CUSTODY_VIEW_BUILDER = (
    StateAgnosticTaskCriteriaGroupBigQueryViewBuilder(
        criteria_name="CUSTODY_LEVEL_IS_CLOSE_AND_IN_PROTECTIVE_CUSTODY",
        logic_type=TaskCriteriaGroupLogicType.AND,
        sub_criteria_list=[
            custody_level_is_close.VIEW_BUILDER,
            housing_unit_type_is_protective_custody.VIEW_BUILDER,
        ],
    )
)

# A person has only one custody level at a time, so at most one of the two
# custody level sub-criteria has non-null custody level reasons on any day.
VIEW_BUILDER = StateAgnosticTaskCriteriaGroupBigQueryViewBuilder(
    criteria_name="CUSTODY_LEVEL_IS_MINIMUM_OR_MEDIUM_OR_CLOSE_IN_PROTECTIVE_CUSTODY",
    logic_type=TaskCriteriaGroupLogicType.OR,
    sub_criteria_list=[
        custody_level_is_minimum_or_medium.VIEW_BUILDER,
        _CUSTODY_LEVEL_IS_CLOSE_AND_IN_PROTECTIVE_CUSTODY_VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=["custody_level", "custody_level_start_date"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
