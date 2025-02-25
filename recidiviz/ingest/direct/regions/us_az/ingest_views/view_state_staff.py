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
"""Query containing information about facilities and community reentry staff.

This view will necessarily conflate staff members with the same name until they are
manually corrected in the RECIDIVIZ_REFERENCE_staff_id_override reference data."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
-- Get all IDs associated with a staff member with a particular full name.
staff_ids_by_name AS (
SELECT DISTINCT 
    PERSON_ID,
    NULLIF(UPPER(FIRST_NAME), 'NULL') AS FIRST_NAME,
    NULLIF(UPPER(SURNAME), 'NULL') AS SURNAME,
FROM {PERSON}
WHERE PERSON_TYPE_ID = '8473' -- DOC Staff
),
-- Pull in a manual reference sheet with overrides that separate IDs for people with
-- the same name. This sheet is populated based on feedback from users in AZ. 
manual_id_overrides AS (
    SELECT 
        FIRST_NAME,
        SURNAME,
        PERSON_ID,
        OVERRIDE_ID
    FROM {RECIDIVIZ_REFERENCE_staff_id_override}
)

SELECT FIRST_NAME, SURNAME, STRING_AGG(PERSON_ID, ',') AS PERSON_IDS
FROM staff_ids_by_name
LEFT JOIN manual_id_overrides
USING(FIRST_NAME, SURNAME, PERSON_ID)
GROUP BY FIRST_NAME, SURNAME, OVERRIDE_ID
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="state_staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
