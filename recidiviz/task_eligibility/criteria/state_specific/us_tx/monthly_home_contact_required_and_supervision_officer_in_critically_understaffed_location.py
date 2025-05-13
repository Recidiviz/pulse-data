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
Defines a criteria span view that shows spans of time during which a client
requires monthly schedule home contacts and is associated with a critically
understaffed location.
"""

from recidiviz.task_eligibility.criteria.state_specific.us_tx import (
    monthly_home_contact_required,
    supervision_officer_in_critically_understaffed_location,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TX_MONTHLY_HOME_CONTACT_REQUIRED_AND_SUPERVISION_OFFICER_IN_CRITICALLY_UNDERSTAFFED_LOCATION"

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which a client
requires monthly schedule home contacts and is associated with a critically
understaffed location.
"""

VIEW_BUILDER = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.AND,
    criteria_name=_CRITERIA_NAME,
    sub_criteria_list=[
        monthly_home_contact_required.VIEW_BUILDER,
        supervision_officer_in_critically_understaffed_location.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[],
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
