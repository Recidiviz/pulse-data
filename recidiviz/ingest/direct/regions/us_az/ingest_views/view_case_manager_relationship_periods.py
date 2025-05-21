# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Query that generates relationship periods for case manager relationships in AZ."""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
-- Selects relevant data from the AZ_CM_INMATE_ASSIGNMENT table, casting date columns to
-- DATE types and filtering out some buggy rows.
case_manager_assignments AS (
    SELECT 
        INMATE_PERSON_ID,
        AGENT_PERSON_ID,
        DATE(CAST(DATE_ASSIGNED AS DATETIME)) AS DATE_ASSIGNED,
        DATE(CAST(DATE_DEASSIGNED AS DATETIME)) AS DATE_DEASSIGNED,
        CAST(UPDT_DTM AS DATETIME) AS UPDT_DTM,
        ACTIVE_FLAG,
        LEAD(
            DATE(CAST(DATE_ASSIGNED AS DATETIME))
        ) OVER (
            PARTITION BY INMATE_PERSON_ID 
            ORDER BY CAST(DATE_ASSIGNED AS DATETIME), CAST(UPDT_DTM AS DATETIME)
        ) AS next_DATE_ASSIGNED
    FROM {AZ_CM_INMATE_ASSIGNMENT}
    WHERE
        -- A small set of assignments have no assignment date and give us no meaningful
        -- info - filter these out. 
        DATE_ASSIGNED IS NOT NULL
),
-- Corrects the case manager assignment data to account for the fact that AZ stopped
-- explicitly hydrating the termination date for assignments in 2019.
case_manager_assignments_with_corrected_deassign_date AS (
    SELECT 
        INMATE_PERSON_ID, AGENT_PERSON_ID,
        DATE_ASSIGNED, 
        CASE
            -- The DATE_DEASSIGNED field is hydrated for assignments that started before
            -- the AZ data migration of Nov 2019. Post 2019-11-29 this field is always 
            -- null.
            WHEN DATE_DEASSIGNED IS NOT NULL THEN DATE_DEASSIGNED
            -- For newer, active assignments, we assume the previous assignment is ended
            -- when the row is updated with a new assignment
            WHEN next_DATE_ASSIGNED IS NOT NULL THEN next_DATE_ASSIGNED
            -- For inactive assignments, we must use the update datetime on the 
            -- assignment row to determine when the assignment ends.
            WHEN ACTIVE_FLAG = 'N' THEN DATE(UPDT_DTM)
            ELSE NULL
        END AS DATE_DEASSIGNED_corrected
    FROM case_manager_assignments
)
SELECT 
    INMATE_PERSON_ID,
    AGENT_PERSON_ID,
    DATE_ASSIGNED,
    DATE_DEASSIGNED_corrected,
    -- We don't expect any overlaps so all relationship_priority are 1
    1 AS relationship_priority
FROM case_manager_assignments_with_corrected_deassign_date
WHERE
    -- Filter out zero-day assignments 
    DATE_DEASSIGNED_corrected IS NULL OR DATE_ASSIGNED != DATE_DEASSIGNED_corrected
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="case_manager_relationship_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
