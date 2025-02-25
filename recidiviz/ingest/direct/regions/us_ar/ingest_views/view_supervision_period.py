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
"""Query containing supervision period information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
cleaned_se AS (
    SELECT 
        *,
        CONCAT(
            SPLIT(SUPVEVNTDATE, ' ')[OFFSET(0)],
            ' ',
            COALESCE(SUPVEVNTTIME,'00:00:00')
        ) AS SUPVEVNTDATETIME
    FROM {SUPERVISIONEVENT}
    WHERE 
        REGEXP_CONTAINS(OFFENDERID, r'^[[:digit:]]+$') AND
        SUPVEVNTDATE IS NOT NULL AND 
        SUPVEVENT IS NOT NULL
),
tl_periods AS (
    -- Transitional living status is indicated with start/stop events (S27 and S28),
    -- but data between these events lack an indicator. Therefore, the start/stop events
    -- are used to construct periods, used in the following CTE to set SPECIAL_STATUS to
    -- TRANSITIONAL_LIVING for events falling within these periods.
    SELECT
        OFFENDERID,
        start_date,
        IFNULL(end_date,'9999-01-01 00:00:00') AS end_date,
    FROM (
        SELECT *
        FROM (
            SELECT 
                OFFENDERID,
                SUPVEVNTDATETIME AS start_date,
                ROW_NUMBER() OVER (PARTITION BY OFFENDERID ORDER BY SUPVEVNTDATETIME) AS rn
            FROM cleaned_se
            WHERE SUPVEVENT = 'S27'
        ) starts
        LEFT JOIN (
            SELECT 
                OFFENDERID,
                SUPVEVNTDATETIME AS end_date,
                ROW_NUMBER() OVER (PARTITION BY OFFENDERID ORDER BY SUPVEVNTDATETIME) AS rn
            FROM cleaned_se
            WHERE SUPVEVENT = 'S28'
        ) ends
        USING(OFFENDERID, rn)
    ) tl
),
se_unique_dates AS (
-- All the candidate dates for a given person that could constitute a period boundary,
-- depending on whether or not a meaningful status change occurred on that date (which is
-- determined over the following CTEs). 
    SELECT DISTINCT 
        se.OFFENDERID,
        SUPVEVNTDATETIME,
        SUPVTYPE,
        SUPVCOUNTY,
        PPOFFICE,
        PPOFFICERID,
        CASE 
            WHEN SUPVSTATUS = 'IA' THEN 'INACTIVE'
            WHEN SUPVSTATUS = 'AB' THEN 'ABSCONDED'
            WHEN SUPVSTATUS = 'IN' THEN 'INCARCERATED'
            WHEN SUPVSTATUS = 'EM' THEN 'ELECTRONIC_MONITORING' 
            WHEN SUPVSTATUS = 'US' THEN 'UNSUPERVISED'
            WHEN tl.OFFENDERID IS NOT NULL THEN 'TRANSITIONAL_LIVING'
            ELSE 'NONE'
        END AS SPECIAL_STATUS
    FROM cleaned_se se
    LEFT JOIN tl_periods tl
    ON
        se.OFFENDERID = tl.OFFENDERID AND 
        se.SUPVEVNTDATETIME >= tl.start_date AND 
        se.SUPVEVNTDATETIME < tl.end_date AND 
        tl.end_date > tl.start_date
),

-- Period bounds are identified by taking the dates on which a meaningful status change occurs.

type_changes AS (
-- Changes in supervision type (SUPVTYPE). This includes some changes within the same 
-- SUPVCATEGORY: for instance, if someone has a SUPVCATEGORY of PRO but transitions from
-- SUPVTYPE 11 (regular probation) to SUPVTYPE 12 (probation plus), that constitutes a 
-- type change. SUPVTYPE indicates someone's overarching supervision assignment, but doesn't
-- account for status changes like absconsions or incarcerations.
    SELECT 
        OFFENDERID,
        SUPVEVNTDATETIME
    FROM (
        SELECT 
            *, 
            LAG(SUPVTYPE) OVER (PARTITION BY OFFENDERID ORDER BY SUPVEVNTDATETIME) AS PREV_SUPVTYPE
        FROM (
            SELECT DISTINCT
                OFFENDERID,
                SUPVEVNTDATETIME,
                SUPVTYPE 
            FROM se_unique_dates
        ) type_dates
    ) lagged_type_dates
    WHERE SUPVTYPE != PREV_SUPVTYPE 
        OR PREV_SUPVTYPE IS NULL
        -- This CTE checks for nulls as well as inequalities so that period starts are included
        -- in the status change dates. Doing this here avoids false starts since SUPVTYPE is 
        -- never null, aside from in broken XML rows.
),
loc_changes AS (
-- Changes in county of supervision.
    SELECT 
        OFFENDERID, 
        SUPVEVNTDATETIME
    FROM (
        SELECT 
            *,
            LAG(SUPVCOUNTY) OVER (PARTITION BY OFFENDERID ORDER BY SUPVEVNTDATETIME) AS PREV_COUNTY
        FROM (
            SELECT DISTINCT
                OFFENDERID, 
                SUPVEVNTDATETIME,
                SUPVCOUNTY
            FROM se_unique_dates
        ) loc_dates
    ) lagged_loc_dates
    WHERE SUPVCOUNTY != PREV_COUNTY
),
supervisory_changes AS (
-- Changes in the office/officer a supervisee reports to.
    SELECT 
        OFFENDERID, 
        SUPVEVNTDATETIME
    FROM (
        SELECT 
            *,
            LAG(PPOFFICE) OVER (PARTITION BY OFFENDERID ORDER BY SUPVEVNTDATETIME) AS PREV_OFFICE,
            LAG(PPOFFICERID) OVER (PARTITION BY OFFENDERID ORDER BY SUPVEVNTDATETIME) AS PREV_STAFF
        FROM (
            SELECT DISTINCT
                OFFENDERID, 
                SUPVEVNTDATETIME,
                PPOFFICE,
                PPOFFICERID
            FROM se_unique_dates
        ) sup_dates
    ) lagged_sup_dates
    WHERE PPOFFICE != PREV_OFFICE OR PPOFFICERID != PREV_STAFF
),
special_status_changes AS (
-- Changes in incarceration/absconsion status.
    SELECT 
        OFFENDERID, 
        SUPVEVNTDATETIME
    FROM (
        SELECT 
            *,
            LAG(SPECIAL_STATUS) OVER (PARTITION BY OFFENDERID ORDER BY SUPVEVNTDATETIME) AS PREV_SPECIAL_STATUS 
        FROM (
            SELECT DISTINCT
                OFFENDERID, 
                SUPVEVNTDATETIME,
                SPECIAL_STATUS
            FROM se_unique_dates
        ) spec_status_dates
    ) lagged_spec_status_dates
    WHERE SPECIAL_STATUS != PREV_SPECIAL_STATUS
),
-- All the status changes identified above are unioned and used to construct periods.
unioned_status_changes AS (
    SELECT 
        OFFENDERID,
        SUPVEVNTDATETIME
    FROM type_changes
    UNION DISTINCT (
        SELECT * 
        FROM loc_changes
    ) 
    UNION DISTINCT (
        SELECT * 
        FROM special_status_changes
    ) 
    UNION DISTINCT (
        SELECT * 
        FROM supervisory_changes
    ) 
),
constructed_periods AS (
    SELECT 
        OFFENDERID,
        SUPVEVNTDATETIME,
        LEAD(SUPVEVNTDATETIME) OVER (PARTITION BY OFFENDERID ORDER BY SUPVEVNTDATETIME) AS END_DATE 
    FROM unioned_status_changes
),
periods_with_info AS (
-- Join the event details (along with EM/TL/US subperiods) back onto the constructed 
-- periods to get period information. 
    SELECT 
        cp.*,
        se.SUPVTYPE,
        se.SUPVCOUNTY,
        se.PPOFFICE,
        se.PPOFFICERID,
        se.SPECIAL_STATUS,
        ROW_NUMBER() OVER (PARTITION BY cp.OFFENDERID ORDER BY SUPVEVNTDATETIME) AS SEQ,
    FROM constructed_periods cp
    LEFT JOIN se_unique_dates se
    USING(OFFENDERID, SUPVEVNTDATETIME)
    WHERE se.SPECIAL_STATUS != 'INACTIVE'
)

SELECT 
  p.*,
  -- Admission/termination reasons are parsed using the event(s) occurring on a given period
  -- start/end, which are concatenated together.
  STRING_AGG(DISTINCT starts.SUPVEVENT,'') AS START_REASON,
  NULLIF(STRING_AGG(DISTINCT COALESCE(ends.SUPVEVENT,''),'-'),'') AS END_REASON,
FROM periods_with_info p
LEFT JOIN cleaned_se starts
USING(OFFENDERID, SUPVEVNTDATETIME)
LEFT JOIN cleaned_se ends
ON 
    P.OFFENDERID = ends.OFFENDERID AND 
    p.END_DATE = ends.SUPVEVNTDATETIME
GROUP BY 
    p.OFFENDERID,
    p.SUPVEVNTDATETIME,
    p.END_DATE,
    p.SUPVTYPE,
    p.SUPVCOUNTY,
    p.PPOFFICE,
    p.PPOFFICERID,
    p.SPECIAL_STATUS,
    p.SEQ
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ar",
    ingest_view_name="supervision_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
