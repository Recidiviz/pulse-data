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
"""Query containing staff location information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    WITH
    -- Thic CTE creates location periods by:
    --   - looking at all versions of the IA_DOC_MAINT_StaffWorkUnits file we've received,
    --   - logging instances where we see work unit or active status change, 
    --   - and then creating periods out of those
    final_periods AS (
        SELECT
            StaffId,
            StaffWorkUnitId,
            WorkUnitId,
            EnteredDt AS start_date,
            LEAD(EnteredDt) OVER(PARTITION BY StaffWorkUnitId ORDER BY EnteredDt) AS end_date,
            Active
        FROM (
            SELECT
                StaffId,
                StaffWorkUnitId,
                RegionId,
                WorkUnitId,
                WorkUnitNm,
                Active,
                CAST(EnteredDt AS DATETIME) AS EnteredDt,
                LAG(WorkUnitId)  OVER w AS prev_WorkUnitId,
                LAG(Active)      OVER w AS prev_Active
            FROM {IA_DOC_MAINT_StaffWorkUnits@ALL}
            WINDOW w AS (PARTITION BY StaffWorkUnitId ORDER BY EnteredDt)
        )
        -- Only keep start dates where at least one of these fields changed from the previous record
        WHERE (
            WorkUnitId IS DISTINCT FROM prev_WorkUnitId
            OR Active IS DISTINCT FROM prev_Active
        )
        -- And only keep start dates where key attribute field is not null
        AND WorkUnitId IS NOT NULL
    )

    SELECT * EXCEPT(Active),
        ROW_NUMBER() OVER(PARTITION BY StaffWorkUnitId ORDER BY start_date) AS seq_no
    FROM final_periods
    -- Only keep start dates for spans of time when the staff member was active
    WHERE Active = "1"

"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ia",
    ingest_view_name="state_staff_location_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
