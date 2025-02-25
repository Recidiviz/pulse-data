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
WITH 
-- Collect all staff information from the base Elite staff table.
elite_staff AS (
SELECT DISTINCT 
    REPLACE(REPLACE(STAFF_ID, '.00', ''), ',', '') AS STAFF_ID,
    FIRST_NAME,
    MIDDLE_NAME,
    LAST_NAME
FROM {recidiviz_elite_staff_members}
WHERE PERSONNEL_TYPE IN ('STAFF', 'CON')
),
-- Collect any STAFF_IDs that are not included in elite_staff, but are included as a referral
-- source in programming data. As of 7/19/2024 there is only one such STAFF_ID.
program_referral_staff AS (
SELECT DISTINCT 
    REPLACE(REPLACE(REFERRAL_STAFF_ID, '.00', ''), ',', '') AS STAFF_ID,
    CAST(NULL AS STRING) AS FIRST_NAME,
    CAST(NULL AS STRING) AS MIDDLE_NAME,
    CAST(NULL AS STRING) AS LAST_NAME
FROM {elite_OffenderProgramProfiles}
WHERE REPLACE(REPLACE(REFERRAL_STAFF_ID, '.00', ''), ',', '') NOT IN (SELECT DISTINCT STAFF_ID FROM elite_staff)
)
SELECT * FROM elite_staff
UNION ALL 
SELECT * FROM program_referral_staff
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="elite_staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
