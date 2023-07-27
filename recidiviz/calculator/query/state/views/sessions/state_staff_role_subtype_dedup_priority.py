# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Dedup priority for state staff role subtypes"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.state.state_staff_role_period import (
    StateStaffRoleSubtype,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

STATE_STAFF_ROLE_SUBTYPE_DEDUP_PRIORITY_VIEW_NAME = (
    "state_staff_role_subtype_dedup_priority"
)

STATE_STAFF_ROLE_SUBTYPE_DEDUP_PRIORITY_VIEW_DESCRIPTION = """
This view defines a prioritized ranking for state staff role subtypes. This view is ultimately used to deduplicate 
state staff role periods so that there is only one role subtype per staff person per officer attribute session
"""

ROLE_SUBTYPE_ORDERED_PRIORITY = [
    StateStaffRoleSubtype.SUPERVISION_STATE_LEADERSHIP,
    StateStaffRoleSubtype.SUPERVISION_REGIONAL_MANAGER,
    StateStaffRoleSubtype.SUPERVISION_DISTRICT_MANAGER,
    StateStaffRoleSubtype.SUPERVISION_OFFICER_SUPERVISOR,
    StateStaffRoleSubtype.SUPERVISION_OFFICER,
    StateStaffRoleSubtype.INTERNAL_UNKNOWN,
    StateStaffRoleSubtype.EXTERNAL_UNKNOWN,
]

STATE_STAFF_ROLE_SUBTYPE_DEDUP_PRIORITY_QUERY_TEMPLATE = """
    SELECT 
        role_subtype,
        role_subtype_priority,
    FROM UNNEST([{prioritized_subtypes}]) AS role_subtype
    WITH OFFSET AS role_subtype_priority    
    """

STATE_STAFF_ROLE_SUBTYPE_PRIORITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=STATE_STAFF_ROLE_SUBTYPE_DEDUP_PRIORITY_VIEW_NAME,
    view_query_template=STATE_STAFF_ROLE_SUBTYPE_DEDUP_PRIORITY_QUERY_TEMPLATE,
    description=STATE_STAFF_ROLE_SUBTYPE_DEDUP_PRIORITY_VIEW_DESCRIPTION,
    should_materialize=False,
    prioritized_subtypes=(
        "\n,".join([f"'{subtype.value}'" for subtype in ROLE_SUBTYPE_ORDERED_PRIORITY])
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        STATE_STAFF_ROLE_SUBTYPE_PRIORITY_VIEW_BUILDER.build_and_print()
