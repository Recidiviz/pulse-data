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
"""Query that collects information about incarceration periods UT."""

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.ingest.direct.regions.us_ut.ingest_views.common_code_constants import (
    INCARCERATION_LEGAL_STATUS_CODES,
    INCARCERATION_LOCATION_TYPE_CODES,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
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
    FROM {{ofndr_lgl_stat}}
    LEFT JOIN {{lgl_stat_cd}}
        USING (lgl_stat_cd)
    LEFT JOIN {{lgl_stat_chg_cd}}
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
            lgl_stat_cd,
            lgl_stat_desc,
            lgl_stat_chg_desc AS legal_status_start_reason,
            LEAD(lgl_stat_chg_desc) OVER (
                PARTITION BY ofndr_num
                ORDER BY stat_beg_datetime
            ) AS legal_status_end_reason,
        FROM legal_status
    )
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
    FROM {{ofndr_loc_hist}}
    LEFT JOIN {{body_loc_cd}}
        USING (body_loc_cd)
    LEFT JOIN {{assgn_rsn_cd}}
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
        loc_typ_cd,
        body_loc_desc,
        assgn_rsn_desc AS location_start_reason,
        LEAD(assgn_rsn_desc) OVER (
              PARTITION BY ofndr_num
              ORDER BY assgn_datetime
        ) AS location_end_reason,
    FROM location
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

        ls.lgl_stat_cd,
        ls.lgl_stat_desc,

        -- Only populate location change reasons if this period matches the location change date
        -- This prevents us from improperly setting the admission reason for subsequent
        -- periods due to some other attribute changing
        IF(p.start_date = loc.start_date, loc.location_start_reason, NULL) AS location_start_reason,
        IF(p.end_date = loc.end_date, loc.location_end_reason, NULL) AS location_end_reason,

        loc.loc_typ_cd,
        loc.body_loc_desc,

    FROM periods_cte p

    LEFT JOIN legal_status_periods ls
        ON ls.ofndr_num = p.ofndr_num
        AND p.start_date >= ls.start_date
        AND IFNULL(p.end_date, '9999-01-01') <= IFNULL(ls.end_date, '9999-01-01')

    LEFT JOIN location_periods loc
        ON loc.ofndr_num = p.ofndr_num
        AND p.start_date >= loc.start_date
        AND IFNULL(p.end_date, '9999-01-01') <= IFNULL(loc.end_date, '9999-01-01')
),
-- Create periods reflecting all changes to any attribute while a person was incarcerated.
-- If multiple status changes happened on the same day, prioritize the
-- location change reason before the legal status change reason.
-- This maximizes information in these fields that is most useful for our mappings.
incarceration_periods AS (
  SELECT * FROM (
        SELECT DISTINCT
            ofndr_num,
            start_date,
            end_date,
            -- When there are both legal status changes and location changes on the same date, the reasons are most often identical.
            -- When they are not, the location start reason is more descriptive. 
            COALESCE(location_start_reason, legal_status_start_reason) AS start_reason,
            COALESCE(location_end_reason, legal_status_end_reason) AS end_reason,
            lgl_stat_desc,
            lgl_stat_cd,
            body_loc_desc,
        FROM periods_with_attributes
        LEFT JOIN lgl_stat_chg_cd_generated_view start_reason
            ON (legal_status_start_reason = lgl_stat_chg_desc
            OR location_start_reason = lgl_stat_chg_desc)
        WHERE (
            lgl_stat_cd in ({list_to_query_string(INCARCERATION_LEGAL_STATUS_CODES, quoted=True)})
            OR loc_typ_cd in ({list_to_query_string(INCARCERATION_LOCATION_TYPE_CODES, quoted=True)})
        )
    )
    -- Do not allow period start reasons to be types of exit from incarceration.
    WHERE START_REASON NOT LIKE "%DISCHARGED%"
    AND START_REASON NOT LIKE "%DIED%" 
    AND START_REASON NOT LIKE '%EXPIRATION%'
    AND START_REASON NOT LIKE '%COMMUTATION%'
    AND (START_REASON NOT LIKE '%PAROLE%' OR START_REASON = 'PAROLE REVOKED')
    AND START_REASON NOT LIKE '%NOT SUPVSD AFTER SCREENING%'
    AND START_REASON NOT LIKE '%PARDON%' AND
    (START_REASON NOT LIKE "%REL%" OR START_REASON = "JAIL RELEASE")
    -- Exclude legal statuses that we cannot expect to be related to charges. 
    AND lgl_stat_desc NOT IN ('UNSENTENCED', 'PRE-CONVICT DIVERSN')
)

SELECT *, 
ROW_NUMBER() OVER (
            PARTITION BY ofndr_num
            ORDER BY start_date, end_date
        ) AS period_id,
FROM incarceration_periods
-- Keep zero-day periods only if they have distinct start and end reasons
WHERE NOT(date(start_date) = date(end_date) AND start_reason = end_reason) 
    OR end_date IS NULL 
    OR end_reason IS NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ut",
    ingest_view_name="incarceration_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
