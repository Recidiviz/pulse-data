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
"""Query containing staff role and supervisor information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    WITH
    -- Thic CTE creates role and supervisor periods by:
    --   - looking at all versions of the IA_DOC_Staff file we've received,
    --   - logging instances where we see role, supervisor, or active status change, 
    --   - and then creating periods out of those
    final_periods AS (
        SELECT
            StaffId,
            JobTitle,
            AuthorityType,
            SupervisorStaffId,
            EnteredDt AS start_date,
            LEAD(EnteredDt) OVER(PARTITION BY StaffId ORDER BY EnteredDt) as end_date,
            Active
        FROM (
            SELECT
                StaffId,
                -- We'll default to working job title where available since that's more precise
                COALESCE(WorkingJobTitle, StateJobTitle) AS JobTitle,
                AuthorityType,
                SupervisorStaffId,
                Active,
                CAST(EnteredDt AS DATETIME) AS EnteredDt,
                LAG(COALESCE(WorkingJobTitle, StateJobTitle)) OVER(w) AS prev_JobTitle,
                LAG(AuthorityType) OVER(w) AS prev_AuthorityType,
                LAG(SupervisorStaffId) OVER(w) AS prev_SupervisorStaffId,
                LAG(Active) OVER(w) AS prev_Active
            FROM {IA_DOC_MAINT_Staff@ALL} s
            WINDOW w AS (PARTITION BY StaffId ORDER BY CAST(EnteredDt AS DATETIME))
        )
        WHERE (JobTitle IS DISTINCT FROM prev_JobTitle
                OR AuthorityType IS DISTINCT FROM prev_AuthorityType
                OR SupervisorStaffId IS DISTINCT FROM prev_SupervisorStaffId
                OR Active IS DISTINCT FROM prev_Active
            )
            -- only keep start dates where we have information about at least one attribute
            AND (JobTitle IS NOT NULL
                OR AuthorityType IS NOT NULL
                OR SupervisorStaffID IS NOT NULL)
    )

    SELECT * EXCEPT(Active),
        ROW_NUMBER() OVER(PARTITION BY StaffId ORDER BY start_date) AS seq_no
    FROM final_periods
    -- Only keep start dates for spans of time when the staff member was active
    WHERE Active = "1"
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ia",
    ingest_view_name="state_staff_role_supervisor_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
