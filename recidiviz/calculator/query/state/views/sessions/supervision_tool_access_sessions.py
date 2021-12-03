# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Sessionized view of each individual, with respect to the Recidiviz line staff tools their
current supervising officer has access to.

To generate the BQ view, run:
python -m recidiviz.calculator.query.state.views.sessions.supervision_tool_access_sessions
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    SESSIONS_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_TOOL_ACCESS_SESSIONS_VIEW_NAME = "supervision_tool_access_sessions"

SUPERVISION_TOOL_ACCESS_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of each individual,
with respect to the Recidiviz line staff tools their current supervising officer has access to.
Sessions are only populated when a supervising officer has some level of access; gaps between
sessions may indicate gaps in supervision or periods supervised by officers with no access.
"""

SUPERVISION_TOOL_ACCESS_SESSIONS_QUERY_TEMPLATE = """
/*{description}*/
WITH 
# sessionized view of each tool access type. currently access is granted but never revoked.
# we use magic dates rather than NULLs to indicate open starts and ends to make intermediate comparisons easier,
# and will convert them back to NULLs at the end
po_report_access_sessions AS (
    SELECT
        state_code,
        officer_external_id,
        received_access AS start_date,
        '9999-01-01' AS end_date,
        TRUE AS has_po_report_access,
    FROM
        `{project_id}.{static_reference_dataset}.po_report_recipients` 
    UNION ALL
    SELECT 
        state_code,
        officer_external_id,
        '0001-01-01' AS start_date,
        DATE_SUB(received_access, INTERVAL 1 DAY) AS end_date,
        FALSE AS has_po_report_access,
    FROM
        `{project_id}.{static_reference_dataset}.po_report_recipients`
)
, case_triage_access_sessions AS (
    SELECT
        state_code,
        officer_external_id,
        received_access AS start_date,
        '9999-01-01' AS end_date,
        TRUE AS has_case_triage_access,
    FROM
        `{project_id}.{static_reference_dataset}.case_triage_users`
    UNION ALL
    SELECT 
        state_code,
        officer_external_id,
        '0001-01-01' AS start_date,
        DATE_SUB(received_access, INTERVAL 1 DAY) AS end_date,
        FALSE AS has_case_triage_access,
    FROM
        `{project_id}.{static_reference_dataset}.case_triage_users`
)
# joins supervision officer sessions with that officer's access dates.
# produces a separate row per session per tool
, tool_access_sessions_overlapping AS (
    SELECT 
        supervision_officer_sessions.state_code,
        supervision_officer_sessions.person_id,
        supervision_officer_sessions.supervising_officer_external_id,
        GREATEST(
            supervision_officer_sessions.start_date, 
            COALESCE(po_report_access_sessions.start_date, case_triage_access_sessions.start_date)
        ) AS start_date,
        LEAST(
            COALESCE(supervision_officer_sessions.end_date, '9999-01-01'),
            COALESCE(po_report_access_sessions.end_date, case_triage_access_sessions.end_date)
        ) AS end_date,
        MIN(has_po_report_access) AS has_po_report_access,
        MIN(has_case_triage_access) AS has_case_triage_access,
    FROM
        `{project_id}.{sessions_dataset}.supervision_officer_sessions_materialized` supervision_officer_sessions
    LEFT JOIN po_report_access_sessions ON (
        supervision_officer_sessions.state_code = po_report_access_sessions.state_code 
        AND supervision_officer_sessions.supervising_officer_external_id = po_report_access_sessions.officer_external_id 
        AND (
            supervision_officer_sessions.start_date BETWEEN po_report_access_sessions.start_date AND po_report_access_sessions.end_date
            OR COALESCE(supervision_officer_sessions.end_date, '9999-01-01') BETWEEN po_report_access_sessions.start_date AND po_report_access_sessions.end_date
        )
    )
    LEFT JOIN case_triage_access_sessions ON (
        supervision_officer_sessions.state_code = case_triage_access_sessions.state_code 
        AND supervision_officer_sessions.supervising_officer_external_id = case_triage_access_sessions.officer_external_id 
        AND (
            supervision_officer_sessions.start_date BETWEEN case_triage_access_sessions.start_date AND case_triage_access_sessions.end_date
            OR COALESCE(supervision_officer_sessions.end_date, '9999-01-01') BETWEEN case_triage_access_sessions.start_date AND case_triage_access_sessions.end_date
        )
    )
    GROUP BY 1, 2, 3, 4, 5
)
# harmonize contiguous access sessions with the same officer by carrying TRUEs forward once they appear
, tool_access_sessions_dedup_1 AS (
     SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        IFNULL(GREATEST(has_po_report_access, IFNULL(LAG(has_po_report_access) OVER officer_window, FALSE)), FALSE) AS has_po_report_access,
        IFNULL(GREATEST(has_case_triage_access, IFNULL(LAG(has_case_triage_access) OVER officer_window, FALSE)), FALSE) AS has_case_triage_access,
     FROM tool_access_sessions_overlapping 
     WINDOW officer_window AS (
         PARTITION BY state_code, person_id, supervising_officer_external_id 
         ORDER BY start_date
     )
)
# Merges neighboring periods with same access level, still potentially overlapping
, access_sliced AS (
    SELECT
        state_code,
        person_id,
        has_po_report_access,
        has_case_triage_access,
        MIN(start_date) AS start_date,
        MAX(end_date) AS end_date,
    FROM (
        # get IDs of contiguous islands
        SELECT
            *,
            SUM (IF(DATE_ADD(prev_end_date, INTERVAL 1 DAY) >= start_date, 0, 1)) OVER (
                PARTITION BY state_code, person_id, has_po_report_access, has_case_triage_access ORDER BY start_date, end_date) AS island_id,
        FROM (
            # get max end_date in preceding rows
            SELECT
                state_code,
                person_id,
                has_po_report_access,
                has_case_triage_access,
                start_date,
                end_date,
                MAX(end_date) OVER (PARTITION BY state_code, person_id, has_po_report_access, has_case_triage_access ORDER BY start_date, end_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_end_date,
            FROM
                tool_access_sessions_dedup_1
            # discard sessions of officers with no access (NULL dates indicate absence from all access tables)
            WHERE start_date IS NOT NULL
            AND (has_po_report_access OR has_case_triage_access)
        )
    )
    GROUP BY 1, 2, 3, 4, island_id
)
# to represent transitions between access levels across overlapping officer sessions,
# we have to disassemble the sessions based on officer assignment and then reconstruct them
# based only on the maximum access level across any overlapping sessions.
, all_boundaries AS (
    SELECT 
        state_code,
        person_id,
        start_date AS boundary_date,
        'start' AS boundary_type,
    FROM access_sliced 
    UNION ALL 
    SELECT 
        state_code, 
        person_id,
        end_date AS boundary_date,
        'end' AS boundary_type,
    FROM access_sliced
)
# join each boundary with all overlapping sessions to aggregate the access levels
, maximum_access_at_boundaries AS (
    SELECT
        all_boundaries.state_code,
        all_boundaries.person_id,
        boundary_date,
        boundary_type,
        # if any officer has access, that takes precedence over those who don't
        MAX(access_sliced.has_case_triage_access) as has_case_triage_access,
        MAX(access_sliced.has_po_report_access) AS has_po_report_access
    FROM all_boundaries 
    LEFT JOIN access_sliced ON (
        all_boundaries.state_code = access_sliced.state_code
        AND all_boundaries.person_id = access_sliced.person_id
        AND boundary_date BETWEEN access_sliced.start_date AND access_sliced.end_date
    )
    GROUP BY 1, 2, 3, 4
)
# collect the boundaries into distinct sessions, delineated by gap in supervision or change in access level
, maximum_grouped AS (
    SELECT 
    *,
    SUM(IF(date_gap OR new_session, 1, 0)) OVER person_window as access_session_id,
    FROM (

        SELECT 
            *,
            COALESCE(LAG(new_session_string) OVER person_window) != COALESCE(new_session_string,'') AS new_session,
        FROM (
            SELECT 
                *,
                # any changes to these values indicate a new session. concat to string for ease of comparison
                CONCAT(has_case_triage_access, has_po_report_access) AS new_session_string,
                # starts after non-contiguous ends should trigger new sessions regardless of access levels
                boundary_type = 'start'
                    AND LAG(boundary_type) OVER person_window = 'end'
                    AND LAG(boundary_date) OVER person_window != DATE_SUB(boundary_date, INTERVAL 1 DAY)
                AS date_gap,
            FROM maximum_access_at_boundaries 
            WINDOW person_window AS (
                PARTITION BY state_code, person_id ORDER BY boundary_date ASC, boundary_type DESC
            )
        )
        WINDOW person_window AS (
            PARTITION BY state_code, person_id ORDER BY boundary_date ASC, boundary_type DESC
        )
    )
    WINDOW person_window AS (
        PARTITION BY state_code, person_id ORDER BY boundary_date ASC, boundary_type DESC
    )
)
# create the actual sessions by grouping on access_session_id
, tool_access_sessions AS (
    SELECT 
        *,
        IF(
            first_boundary.boundary_type = 'start', 
            first_boundary.boundary_date, 
            # if the first boundary is an end, start one day after the preceding session;
            # this happens after an overlapping period with different access has ended
            DATE_ADD(LAG(last_boundary.boundary_date) OVER sessions_window, INTERVAL 1 DAY)
        ) AS start_date,

        IF(
            last_boundary.boundary_type = 'end',
            last_boundary.boundary_date,
            # if the last boundary is a start, end one day before the following session;
            # this happens when an overlapping period with different access is about to start
            DATE_SUB(LEAD(first_boundary.boundary_date) OVER sessions_window, INTERVAL 1 DAY)
        ) AS end_date,
    FROM (
        SELECT 
            state_code,
            person_id,
            access_session_id,
            has_case_triage_access,
            has_po_report_access,
            # in cases of overlap, start and end dates may need to be calculated relative to an adjacent session;
            # to determine this we will need to know the first and last entry in each session
            ARRAY_AGG(STRUCT(boundary_date, boundary_type) ORDER BY boundary_date ASC, boundary_type DESC)[OFFSET(0)] as first_boundary,
            # sort 'start' before 'end' for first, and vice versa for last
            ARRAY_AGG(STRUCT(boundary_date, boundary_type) ORDER BY boundary_date DESC, boundary_type ASC)[OFFSET(0)] as last_boundary,
        FROM maximum_grouped 
        GROUP BY 1, 2, 3, 4, 5
    )
    WINDOW sessions_window AS (
        PARTITION BY state_code, person_id
        ORDER BY access_session_id
    )
)
# final interface and cleanup
SELECT
    state_code,
    person_id,
    access_session_id,
    start_date,
    # convert magic date back to NULL for open periods
    IF(end_date = '9999-01-01', NULL, end_date) as end_date,
    has_case_triage_access,
    has_po_report_access,
FROM tool_access_sessions
ORDER BY 1, 2, 3
"""

SUPERVISION_TOOL_ACCESS_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SUPERVISION_TOOL_ACCESS_SESSIONS_VIEW_NAME,
    view_query_template=SUPERVISION_TOOL_ACCESS_SESSIONS_QUERY_TEMPLATE,
    description=SUPERVISION_TOOL_ACCESS_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TOOL_ACCESS_SESSIONS_VIEW_BUILDER.build_and_print()
