# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Query containing supervision staff information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH staff_from_directory AS (
SELECT DISTINCT
    CAST(OFFICER AS INT64) AS OFFICER,
    UPPER(LastName) AS LastName,
    UPPER(FirstName) AS FirstName,
    email
FROM {RECIDIVIZ_REFERENCE_PP_directory}
), 
officers_with_recency_rank AS (
SELECT 
    OFFICER,
    UPPER(LNAME) AS LastName,
    UPPER(FNAME) AS FirstName,
    EMAIL 
FROM (
    SELECT 
        CAST(OFFICER AS INT64) AS OFFICER,
        LNAME,
        FNAME,
        -- Null out emails for entries like "Bismarck CS", which sometimes appear
        -- as terminating officers in supervision periods, but are not real people.
        IF(LOGINNAME LIKE '%% %%', NULL, CONCAT(LOWER(LOGINNAME), '@nd.gov')) AS EMAIL,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(OFFICER AS INT64)
            ORDER BY CAST(RecDate AS DATETIME) DESC) AS recency_rank
    FROM {docstars_officers}
    WHERE CAST(OFFICER AS INT64) NOT IN (
        SELECT DISTINCT OFFICER 
        FROM staff_from_directory)
    ) sub
WHERE recency_rank = 1
)
SELECT 
    OFFICER,
    LastName,
    FirstName,
    EMAIL
FROM (
    SELECT * FROM staff_from_directory
    UNION ALL
    SELECT * FROM officers_with_recency_rank
) sub
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="docstars_staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
