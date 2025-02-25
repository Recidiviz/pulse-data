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
"""Query containing state Staff information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
    most_recent_staff_information AS (
    SELECT 
        *, 
        ROW_NUMBER() OVER (PARTITION BY StaffID ORDER BY update_datetime DESC) as RecencyRank
    FROM {Staff@ALL}
    ),
    most_recent_staff_email_information AS (
    SELECT *
    FROM  
        (SELECT 
        *, 
        ROW_NUMBER() OVER (PARTITION BY StaffID ORDER BY update_datetime DESC) as RecencyRank
        FROM {StaffEmailByAlias@ALL} ) AS a
    WHERE RecencyRank = 1
    ),  
    -- some people have changed IDs or names over time but are the same person with same email,
    -- this takes the most recent ID for email 
    most_recent_id_for_email AS (
        SELECT StaffID, OutlookEmail
        FROM most_recent_staff_email_information
        WHERE OutlookEmail is not null
        AND TRUE
        QUALIFY ROW_NUMBER() OVER (PARTITION BY OutlookEmail ORDER BY update_datetime DESC) = 1
    )
    SELECT 
        REGEXP_REPLACE(StaffID, r'[^A-Z0-9]', '') as StaffID, 
        LastName,
        FirstName,
        IF(OutlookEmail IS NOT NULL AND OutlookEmail NOT LIKE '%@%', '', OutlookEmail) AS OutlookEmail
    FROM most_recent_staff_information si
    LEFT JOIN most_recent_id_for_email sei USING (StaffID)
    WHERE StaffID IS NOT NULL AND si.RecencyRank = 1
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tn", ingest_view_name="Staff", view_query_template=VIEW_QUERY_TEMPLATE
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
