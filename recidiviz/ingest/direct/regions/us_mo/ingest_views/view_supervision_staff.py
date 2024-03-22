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
WITH APFX_ALL AS (
    SELECT *
    FROM (
        SELECT
            BDGNO,
            FNAME,
            MINTL,
            LNAME,
            DTEORD,
            update_datetime
        FROM {LBCMDATA_APFX90@ALL}
        UNION ALL (
            SELECT
                BDGNO,
                FNAME,
                MINTL,
                LNAME,
                DTEORD,
                update_datetime
            FROM {LBCMDATA_APFX91@ALL}
        )
    ) apfx_all
    WHERE BDGNO IS NOT NULL
)

SELECT
    BDGNO,
    FNAME,
    MINTL,
    LNAME
FROM (
    SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY BDGNO 
            ORDER BY 
                CAST(DTEORD AS INT64) DESC,
                update_datetime DESC,
                CONCAT(FNAME, IFNULL(MINTL,''), LNAME)
        ) AS RowNumber
    FROM APFX_ALL
) ranked
WHERE RowNumber = 1
-- Since badge numbers can have multiple names associated with them, this ranking prioritizes
-- names that correspond to active roles (DTEORD = 0). If there are still multiple candidates,
-- it picks the name provided in the most recent file, and then resorts to alphabetization 
-- if necessary. 
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mo",
    ingest_view_name="supervision_staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
