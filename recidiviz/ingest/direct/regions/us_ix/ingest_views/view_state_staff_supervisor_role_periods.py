# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Query that generates state staff supervisor periods information."""

from recidiviz.ingest.direct.regions.us_ix.ingest_views.query_fragments import (
    STATE_STAFF_SUPERVISOR_PERIODS_CTES,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
    WITH {STATE_STAFF_SUPERVISOR_PERIODS_CTES},
    -- compile all relevant supervisor role dates together into all_dates
    all_start_dates as (
        SELECT 
            DISTINCT
            supervisor_EmployeeId,
            start_date,
        FROM final_supervisor_periods
    ),
    all_end_dates as (
        SELECT 
            DISTINCT
            supervisor_EmployeeId,
            COALESCE(end_date, DATE(9999,9,9)) as start_date
        FROM final_supervisor_periods
    ),
    all_dates as (
        SELECT * from all_start_dates
        UNION DISTINCT
        SELECT * from all_end_dates
    ),
    -- create periods off of all these dates
    all_role_periods as (
        SELECT 
            supervisor_EmployeeId,
            start_date,
            LEAD(start_date) OVER(PARTITION BY supervisor_EmployeeId ORDER BY start_date) as end_date
        FROM all_dates
    ),
    -- keep only periods that overlap with a supervisor period and valid dates
    final_role_periods as (
        SELECT
          DISTINCT 
          a.supervisor_EmployeeId,
          a.start_date,
          CASE WHEN a.end_date = DATE(9999,9,9) 
              THEN NULL ELSE a.end_date 
              END AS end_date,
        FROM all_role_periods a
        -- merge on overlapping supervisor periods
        LEFT JOIN final_supervisor_periods b
            ON a.supervisor_EmployeeId = b.supervisor_EmployeeId 
                and b.start_date <= a.start_date and a.end_date <= COALESCE(b.end_date, DATE(9999,9,9))
        WHERE 
        -- keep only periods that have an end date (since we've coalesced end dates with 9-9-9999)
        a.end_date IS NOT NULL 
        -- keep only periods that have an overlapping supervisor period
        and b.officer_EmployeeId IS NOT NULL
    )

    SELECT
        *, 
        ROW_NUMBER() OVER(PARTITION BY supervisor_EmployeeId ORDER BY start_date, end_date NULLS LAST) as period_id,
    FROM final_role_periods
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="state_staff_supervisor_role_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
