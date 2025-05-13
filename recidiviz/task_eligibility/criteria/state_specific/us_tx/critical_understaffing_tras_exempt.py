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
"""Defines a criteria view that shows spans of time for which supervision clients
are exempt from TRAS because they are located in a critically understaffed location
and are on low or low medium supervision.
"""
from recidiviz.task_eligibility.criteria.general import (
    supervision_level_is_medium_or_minimum,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tx import (
    supervision_officer_in_critically_understaffed_location,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    AndTaskCriteriaGroup,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.types import assert_type

_CRITERIA_NAME = "US_TX_CRITICAL_UNDERSTAFFING_TRAS_EXEMPT"

_DESCRIPTION = """Defines a criteria view that shows spans of time for which supervision clients
are exempt from TRAS because they are located in a critically understaffed location
and are on low or low medium supervision.
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = assert_type(
    AndTaskCriteriaGroup(
        criteria_name=_CRITERIA_NAME,
        sub_criteria_list=[
            supervision_officer_in_critically_understaffed_location.VIEW_BUILDER,
            supervision_level_is_medium_or_minimum.VIEW_BUILDER,
        ],
        allowed_duplicate_reasons_keys=[],
    ).as_criteria_view_builder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
