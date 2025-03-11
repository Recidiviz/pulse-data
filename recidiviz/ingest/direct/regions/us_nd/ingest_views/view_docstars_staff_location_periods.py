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
"""Ingest view for supervision staff employment location information from the Docstars system."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- This CTE gets the officer data directly from the Docstars system into the same format
-- as above, to the extent possible. In Docstars, RecDate is the date the row was created.
staff_from_docstars AS (
    SELECT DISTINCT
        -- This is safe because IDs from docstars_officers are exclusively numeric.
        CAST(CAST(OFFICER AS INT64) AS STRING) AS OFFICER,
        SITEID AS location,
        CAST(RecDate AS DATETIME) AS edge_date, 
        STATUS
    FROM {docstars_officers@ALL} 
),
-- This CTE creates windows where particular statuses and locations were active.
critical_dates AS (
SELECT
    OFFICER,
    location,
    edge_date AS start_date,
    LEAD(edge_date) OVER person_window AS end_date,
    STATUS,
FROM staff_from_docstars cd
WINDOW person_window AS (PARTITION BY OFFICER ORDER BY edge_date)
)

SELECT 
    OFFICER,
    location,
    start_date,
    end_date,
    ROW_NUMBER() OVER (PARTITION BY OFFICER ORDER BY start_date) AS period_seq_num,
FROM critical_dates
-- if a person is no longer employed by DOCR, there should not be a period starting on the date of that transition
WHERE STATUS != '0'
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="docstars_staff_location_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
