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
"""View that represents attributes about every primary user (line staff) 
of the Workflows tool, with spans reflecting historical product roster
information where available"""


from datetime import datetime
from typing import Dict, List

from recidiviz.calculator.query.state.views.analyst_data.product_roster_archive_sessions_utils import (
    get_provisioned_user_registration_sessions_view_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.types import WorkflowsSystemType

PRIMARY_USER_ROLE_TYPES_BY_SYSTEM_TYPE: Dict[WorkflowsSystemType, List[str]] = {
    WorkflowsSystemType.INCARCERATION: [
        "facilities_line_staff",
        "facilities_staff",
    ],
    WorkflowsSystemType.SUPERVISION: [
        "supervision_line_staff",
        "supervision_officer",
        "supervision_staff",
    ],
}

# Date after which we consider product roster archive to reflect validated
# role and location information. We will backfill information starting at a user's
# first signup date and ending on this date, for users present in the roster on this
# date.
WORKFLOWS_PRODUCT_ROSTER_ARCHIVE_FIRST_VALIDATED_DATE = datetime(2024, 9, 26)


WORKFLOWS_PROVISIONED_USER_REGISTRATION_SESSIONS_VIEW_BUILDER = get_provisioned_user_registration_sessions_view_builder(
    product_name="WORKFLOWS",
    first_validated_roster_date=WORKFLOWS_PRODUCT_ROSTER_ARCHIVE_FIRST_VALIDATED_DATE,
    role_types_by_system_type_dict=PRIMARY_USER_ROLE_TYPES_BY_SYSTEM_TYPE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        WORKFLOWS_PROVISIONED_USER_REGISTRATION_SESSIONS_VIEW_BUILDER.build_and_print()
