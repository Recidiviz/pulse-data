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
"""Query containing facilities staff information.

For the time being, this is a placeholder that ingests the StateStaffExternalId with an 
empty attached StateStaff entity. We do not receive any identifying information about
facilities staff except for IDs.

Since these are currently only relevant in validations that ensure REFERRAL_STAFF_ID 
values match to state_staff values during ingest of state_program_assignment, the 
placeholder entities will be built using REFERRAL_STAFF_ID to ensure that a StateStaff
entity is created for each staff member who refers a resident to a program or course."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT DISTINCT 
    REPLACE(REPLACE(REFERRAL_STAFF_ID, '.00', ''), ',', '') AS REFERRAL_STAFF_ID,
FROM {elite_OffenderProgramProfiles}
WHERE REFERRAL_STAFF_ID IS NOT NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="elite_staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
