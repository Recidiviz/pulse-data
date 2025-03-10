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
"""Query that generates supervision periods in Utah."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
-- Collect all legal status updates and the dates they were made.
legal_status AS (
    SELECT
        ofndr_num,
        lgl_stat_cd,
        lgl_stat_desc,
        CAST((LEFT(stat_beg_dt, 10) || ' ' || stat_beg_tm) AS DATETIME) AS stat_beg_datetime,
        lgl_stat_chg_cd,
        lgl_stat_chg_desc,
    FROM {ofndr_lgl_stat}
    LEFT JOIN {lgl_stat_cd}
        USING (lgl_stat_cd)
    LEFT JOIN {lgl_stat_chg_cd}
        USING (lgl_stat_chg_cd)
),
-- Create legal status periods using all legal status updates. A status is assumed to 
-- apply until the next status begins; the native stat_end_dt field is not used.
legal_status_periods AS (
    SELECT *
    FROM (
        SELECT
            ofndr_num,
            stat_beg_datetime AS start_date,
            LEAD(stat_beg_datetime) OVER (
                PARTITION BY ofndr_num
                ORDER BY stat_beg_datetime
            ) AS end_date,
            lgl_stat_desc,
            lgl_stat_chg_desc AS legal_status_start_reason,
            LEAD(lgl_stat_chg_desc) OVER (
                PARTITION BY ofndr_num
                ORDER BY stat_beg_datetime
            ) AS legal_status_end_reason,
        FROM legal_status
    )
    WHERE lgl_stat_desc LIKE '%PAROLE%' OR lgl_stat_desc LIKE '%PROBATION%'
),
-- Collect all location updates and the dates they were made.
location AS (
    SELECT
        ofndr_num,
        body_loc_cd,
        body_loc_desc,
        loc_typ_cd,
        CAST((LEFT(assgn_dt, 10) || ' ' || assgn_tm) AS DATETIME) AS assgn_datetime,
        assgn_rsn_cd,
        assgn_rsn_desc,
        CAST(end_dt AS DATETIME) AS end_dt,
    FROM {ofndr_loc_hist}
    LEFT JOIN {body_loc_cd}
        USING (body_loc_cd)
    LEFT JOIN {assgn_rsn_cd}
        USING (assgn_rsn_cd)
),
-- Create location periods using all location updates. A location is assumed to 
-- apply until the next location is assigned; the native end_dt field is not used.
location_periods AS (
    SELECT
        ofndr_num,
        assgn_datetime AS start_date,
        -- if there is not a next assignment and there is an end date, close the period
        COALESCE(LEAD(assgn_datetime) OVER (
            PARTITION BY ofndr_num
            ORDER BY assgn_datetime
        ) , end_dt) AS end_date,
        body_loc_desc,
        assgn_rsn_desc AS location_start_reason,
        LEAD(assgn_rsn_desc) OVER (
              PARTITION BY ofndr_num
              ORDER BY assgn_datetime
        ) AS location_end_reason,
    FROM location
),
-- Collect all supervision level updates and the dates they were made.
supervision_level AS (
    SELECT DISTINCT
        ofndr_num,
        cd.sprvsn_lvl_desc,
        'Supervision Level Change' as reason,
        CAST(strt_dt AS DATETIME) AS start_date,
        CAST(IF(end_dt = '(null)', NULL, end_dt) AS DATETIME) AS end_date,
    FROM {ofndr_sprvsn}
    LEFT JOIN {sprvsn_lvl_cd} cd
    USING(sprvsn_lvl_cd)
),
-- Create supervision level periods using all supervision level updates.
supervision_level_periods AS (
    SELECT
        ofndr_num,
        start_date,
        end_date,
        reason,
        sprvsn_lvl_desc,
    FROM (
        SELECT
            ofndr_num,
            session_id,
            date_gap_id,
            MIN(start_date) OVER w AS start_date,
            IF(MAX(end_date) OVER w = "9999-12-31", NULL, MAX(end_date) OVER w) AS end_date,
            sprvsn_lvl_desc,
            reason
        FROM (
            SELECT
                *,
                SUM(IF(session_boundary, 1, 0)) OVER (PARTITION BY ofndr_num, sprvsn_lvl_desc ORDER BY start_date, IFNULL(end_date, "9999-12-31")) AS session_id,
                SUM(IF(date_gap, 1, 0)) OVER (PARTITION BY ofndr_num ORDER BY start_date, IFNULL(end_date, "9999-12-31")) AS date_gap_id,
            FROM (
                SELECT
                    ofndr_num,
                    start_date,
                    IFNULL(end_date, "9999-12-31") AS end_date,
                    -- Define a session boundary if there is no prior adjacent span with the same attribute columns
                    COALESCE(LAG(end_date) OVER (PARTITION BY ofndr_num, sprvsn_lvl_desc ORDER BY start_date, IFNULL(end_date, "9999-12-31")) != start_date, TRUE) AS session_boundary,
                    -- Define a date gap if there is no prior adjacent span, regardless of attribute columns
                    COALESCE(LAG(end_date) OVER (PARTITION BY ofndr_num ORDER BY start_date, IFNULL(end_date, "9999-12-31")) != start_date, TRUE) AS date_gap,
                    sprvsn_lvl_desc,
                    reason
                FROM supervision_level
            )
        )
        -- TODO(goccy/go-zetasqlite#123): Workaround emulator unsupported QUALIFY without WHERE/HAVING/GROUP BY clause
        WHERE TRUE
        QUALIFY ROW_NUMBER() OVER w = 1
        WINDOW w AS (PARTITION BY ofndr_num, session_id, sprvsn_lvl_desc)
    )
),
-- Collect all client <> agent assignments and the dates they were made.
ofndr_agnt AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ofndr_num ORDER BY start_date, end_date, agnt_id) AS dedup_id
    FROM (
        SELECT
            ofndr_num,
            agnt_id,
            -- agcy_id,
            -- usr_typ_cd,
            CAST(agnt_strt_dt AS DATETIME) AS start_date,
            CAST(IF(end_dt = '(null)', NULL, end_dt) AS DATETIME) AS end_date,
            'Supervisor Change' AS reason,
        FROM {ofndr_agnt}
        -- TODO(#37223): Refine logic used to deduplicate
        WHERE usr_typ_cd IN ("A") -- Most common type, maybe active?
            AND UPPER(agnt_id) != 'NONE' -- Eliminates some duplicates, doesn't seem like useful information
    )
    WHERE start_date != IFNULL(end_date, '9999-01-01') -- Eliminates some duplicates
),
-- Create client <> agent assignment periods using all relationship updates.
supervising_officer_periods AS (
    SELECT *
    FROM ofndr_agnt a
    -- Eliminates periods that are entirely contained within another period
    LEFT JOIN (
        SELECT DISTINCT b.ofndr_num, b.dedup_id
        FROM ofndr_agnt a
        LEFT JOIN ofndr_agnt b
            ON a.ofndr_num = b.ofndr_num
            AND a.dedup_id != b.dedup_id
            AND b.start_date BETWEEN a.start_date AND IFNULL(a.end_date, '9999-01-01')
            AND IFNULL(b.end_date, '9999-01-01') BETWEEN a.start_date AND IFNULL(a.end_date, '9999-01-01')
        WHERE b.ofndr_num IS NOT NULL
    ) b
    USING (ofndr_num, dedup_id)
    WHERE b.ofndr_num IS NULL
),

-- UNION together all relevant CTEs that indicate some type of status change
transitions_union AS (
    -- Legal Status
    SELECT DISTINCT
        ofndr_num,
        start_date AS transition_date,
    FROM legal_status_periods

    UNION DISTINCT

    SELECT DISTINCT
        ofndr_num,
        end_date,
    FROM legal_status_periods

    UNION DISTINCT

    -- Location
    SELECT DISTINCT
        ofndr_num,
        start_date,
    FROM location_periods

    UNION DISTINCT

    SELECT DISTINCT
        ofndr_num,
        end_date,
    FROM location_periods

    UNION DISTINCT

    -- Supervision Level
    SELECT DISTINCT
        ofndr_num,
        start_date,
    FROM supervision_level_periods

    UNION DISTINCT

    SELECT DISTINCT
        ofndr_num,
        end_date,
    FROM supervision_level_periods

    UNION DISTINCT

    -- Supervising Officer
    SELECT DISTINCT
        ofndr_num,
        start_date,
    FROM supervising_officer_periods

    UNION DISTINCT

    SELECT DISTINCT
        ofndr_num,
        end_date,
    FROM supervising_officer_periods
),

-- Create proto-periods by using LEAD
periods_cte AS (
    SELECT
        ofndr_num,
        transition_date AS start_date,
        LEAD(transition_date) OVER (PARTITION BY ofndr_num ORDER BY transition_date) AS end_date,
    FROM transitions_union
),

-- Join back to all original CTEs to get attributes for each session
periods_with_attributes AS (
    SELECT DISTINCT
        p.ofndr_num,
        p.start_date,
        p.end_date,

        -- Only populate legal status change reasons if this period matches the legal status change date
        -- This prevents us from improperly setting the admission reason for subsequent
        -- periods due to some other attribute changing
        IF(p.start_date = ls.start_date, ls.legal_status_start_reason, NULL) AS legal_status_start_reason,
        IF(p.end_date = ls.end_date, ls.legal_status_end_reason, NULL) AS legal_status_end_reason,

        ls.lgl_stat_desc,

        -- Only populate location change reasons if this period matches the location change date
        -- This prevents us from improperly setting the admission reason for subsequent
        -- periods due to some other attribute changing
        IF(p.start_date = loc.start_date, loc.location_start_reason, NULL) AS location_start_reason,
        IF(p.end_date = loc.end_date, loc.location_end_reason, NULL) AS location_end_reason,

        loc.body_loc_desc,

        -- Only populate supervision level change reasons if this period matches the 
        -- supervision level change date
        -- This prevents us from improperly setting the admission reason for subsequent
        -- periods due to some other attribute changing
        IF(p.start_date = sl.start_date, sl.reason, NULL) AS level_start_reason,
        IF(p.end_date = sl.end_date, sl.reason, NULL) AS level_end_reason,

        sl.sprvsn_lvl_desc,

        -- Only populate agent change reasons if this period matches the agent change date
        -- This prevents us from improperly setting the admission reason for subsequent
        -- periods due to some other attribute changing
        IF(p.start_date = sop.start_date, sop.reason, NULL) AS agent_start_reason,
        IF(p.end_date = sop.end_date, sop.reason, NULL) AS agent_end_reason,

        sop.agnt_id,

    FROM periods_cte p

    LEFT JOIN legal_status_periods ls
        ON ls.ofndr_num = p.ofndr_num
        AND p.start_date >= ls.start_date
        AND IFNULL(p.end_date, '9999-01-01') <= IFNULL(ls.end_date, '9999-01-01')

    LEFT JOIN location_periods loc
        ON loc.ofndr_num = p.ofndr_num
        AND p.start_date >= loc.start_date
        AND IFNULL(p.end_date, '9999-01-01') <= IFNULL(loc.end_date, '9999-01-01')

    LEFT JOIN supervision_level_periods sl
        ON sl.ofndr_num = p.ofndr_num
        AND p.start_date >= sl.start_date
        AND IFNULL(p.end_date, '9999-01-01') <= IFNULL(sl.end_date, '9999-01-01')

    LEFT JOIN supervising_officer_periods sop
        ON sop.ofndr_num = p.ofndr_num
        AND p.start_date >= sop.start_date
        AND IFNULL(p.end_date, '9999-01-01') <= IFNULL(sop.end_date, '9999-01-01')
),
-- Create periods reflecting all changes to any attribute while a person was on 
-- supervision. If multiple status changes happened on the same day, prioritize the
-- change reasons in this order: location change reason, legal status change reason, 
-- supervision level change reason, supervision officer change end reason.
-- This maximizes information in these fields that is most useful for our mappings.
supervision_periods AS (
    SELECT 
    ofndr_num,
    start_date,
    end_date,
    -- When there are both legal status changes and location changes on the same date, the reasons are most often identical.
    -- When they are not, the location start reason is more descriptive. 
    COALESCE(location_start_reason, legal_status_start_reason, level_start_reason, agent_start_reason) AS start_reason,
    COALESCE(location_end_reason, legal_status_end_reason, level_end_reason, agent_end_reason) AS end_reason,
    lgl_stat_desc,
    body_loc_desc,
    sprvsn_lvl_desc,
    agnt_id,
    FROM periods_with_attributes
    LEFT JOIN {lgl_stat_chg_cd} start_reason
        ON (legal_status_start_reason = lgl_stat_chg_desc
        OR location_start_reason = lgl_stat_chg_desc)
    -- Do not allow period start reasons to be types of exit from supervision.
    WHERE start_reason.sprvsn_exit_typ_id IS NULL 
    AND (lgl_stat_desc LIKE '%PAROLE%' OR lgl_stat_desc LIKE '%PROBATION%')
)
SELECT *, 
ROW_NUMBER() OVER (
            PARTITION BY ofndr_num
            ORDER BY start_date, end_date
        ) AS period_id,
FROM supervision_periods
WHERE (date(start_date) != date(end_date) or end_date is null)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ut",
    ingest_view_name="supervision_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
