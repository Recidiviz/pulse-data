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
requires a scheduled home contact and is NOT associated with the critical
understaffing policy, which has separate requirements for quarterly home visits.
"""

from recidiviz.task_eligibility.criteria.state_specific.us_tx import (
    needs_scheduled_home_contact_standard_policy,
    not_quarterly_home_contact_required_and_supervision_officer_in_critically_understaffed_location,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    AndTaskCriteriaGroup,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = (
    "US_TX_NEEDS_SCHEDULED_HOME_CONTACT_NOT_QUARTERLY_CRITICAL_UNDERSTAFFING"
)


VIEW_BUILDER = AndTaskCriteriaGroup(
    criteria_name=_CRITERIA_NAME,
    sub_criteria_list=[
        needs_scheduled_home_contact_standard_policy.VIEW_BUILDER,
        not_quarterly_home_contact_required_and_supervision_officer_in_critically_understaffed_location.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=["frequency", "type_of_contact"],
).as_criteria_view_builder

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
