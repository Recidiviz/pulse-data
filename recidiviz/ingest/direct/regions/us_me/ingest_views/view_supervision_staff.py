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
"""Query containing supervision staff information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- Lower() and trim() the email.
cleaned AS (
  SELECT 
    * EXCEPT (Email_Tx),
    LOWER(TRIM(Email_Tx)) AS Email_Tx
  FROM {CIS_900_EMPLOYEE}
)
SELECT 
    STRING_AGG(DISTINCT Employee_Id) AS Employee_Ids,
    -- If there are multiple names per email, sort alphabetically and pick the most
    -- recently modified name. This will sometimes be the case when someone has
    -- multiple employee IDs, and someones name is slightly different in one version of
    -- the IDs.
    ARRAY_AGG(First_Name ORDER BY Modified_On_Date || First_Name || Last_Name DESC)[SAFE_OFFSET(0)] First_Name_sorted,
    ARRAY_AGG(Last_Name ORDER BY Modified_On_Date || First_Name || Last_Name DESC)[SAFE_OFFSET(0)] Last_Name_sorted,
    Email_Tx
FROM cleaned
-- Ignore invalid emails.
WHERE REGEXP_CONTAINS(Email_Tx, r"@")
AND NOT(REGEXP_CONTAINS(Email_Tx, r"[ (),:;<>[\\]\\\\]"))

-- Group by email and name because some people have multiple employee IDs. 
GROUP BY Email_Tx
"""


VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_me",
    ingest_view_name="supervision_staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
