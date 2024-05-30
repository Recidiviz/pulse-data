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
"""Query containing supervision period information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- This CTE collects all dates on which any relevant action or status change took place.
-- These dates will later be used to construct periods. 
-- Each row contains one critical date, and the actions that were taken or attributes that
-- changed for a given person on that date. 
critical_dates AS (
SELECT DISTINCT
-- critical dates from movements table
  NULLIF(doc_episode.PERSON_ID, 'NULL') AS PERSON_ID,
  DOC_ID,
  NULLIF(doc_episode.DPP_ID, 'NULL') AS DPP_ID,
  -- Do not include timestamp because same-day movements are often logged out of order.
  CAST(CAST(MOVEMENT_DATE AS DATETIME) AS DATE) AS CRITICAL_DATE,
  mvmt_codes.MOVEMENT_DESCRIPTION,
  CAST(NULL AS STRING) AS supervision_level,
  action_lookup.OTHER_2 AS sup_period_action,
FROM {AZ_DOC_INMATE_TRAFFIC_HISTORY} traffic
LEFT JOIN {DOC_EPISODE} doc_episode
USING (DOC_ID)
LEFT JOIN {AZ_DOC_MOVEMENT_CODES} mvmt_codes
USING(MOVEMENT_CODE_ID) 
LEFT JOIN {LOOKUPS} action_lookup
-- This field establishes what the right course of action is regarding the period as a 
-- result of this movement (close, open, or reopen)
ON(PRSN_CMM_SUPV_EPSD_LOGIC_ID = action_lookup.LOOKUP_ID)
WHERE traffic.MOVEMENT_DATE IS NOT NULL
AND MOVEMENT_CODE_ID IS NOT NULL AND MOVEMENT_CODE_ID != 'NULL'
-- Only include rows with some supervision-related action (Create, Close, or Re-Open)
AND (action_lookup.OTHER_2 IS NOT NULL
OR action_lookup.OTHER IS NOT NULL)

UNION ALL 

-- critical dates from DPP episode table (supervision level tracking)
-- TODO(#30235): Understand how changes or updates to supervision levels are tracked
-- to make sure they are all accounted for in this view. 
SELECT DISTINCT
  NULLIF(dpp_episode.PERSON_ID, 'NULL') AS PERSON_ID,
  CAST(NULL AS STRING) AS DOC_ID,
  NULLIF(DPP_ID, 'NULL') AS DPP_ID,
  -- Do not include timestamp because same-day movements are often logged out of order.
  CAST(CAST(SUPERVISION_LEVEL_STARTDATE AS DATETIME) AS DATE) AS CRITICAL_DATE,
  'Supervision Level Change' AS MOVEMENT_DESCRIPTION,
  level_lookup.DESCRIPTION AS supervision_level,
  'Maintain' AS sup_period_action
FROM {DPP_EPISODE@ALL} dpp_episode
LEFT JOIN {LOOKUPS} level_lookup
ON(dpp_episode.SUPERVISION_LEVEL_ID = LOOKUP_ID)
),
-- This CTE uses the critical dates from the critical_dates CTE to create periods
-- in which specific sets of attributes were true. The output contains a specific set of 
-- movements that started and ended a period, the start and end dates themselves, and the
-- attributes that were true for a given person during that period. 
periods AS (
SELECT DISTINCT
  PERSON_ID, 
  DOC_ID,
  DPP_ID,
  CRITICAL_DATE AS START_DATE,
  LEAD(CRITICAL_DATE) OVER person_window AS END_DATE,
  sup_period_action AS start_action,
  LEAD(sup_period_action) OVER person_window AS end_action,
  MOVEMENT_DESCRIPTION AS START_REASON,
  LEAD(MOVEMENT_DESCRIPTION) OVER person_window AS END_REASON,
  supervision_level
FROM critical_dates
WINDOW person_window AS (PARTITION BY PERSON_ID ORDER BY CRITICAL_DATE, DPP_ID, 
    CASE 
      -- When any or all of these movements happen on the same date, we want them to 
      -- be sorted in this order for the results to make the most logical sense.
      WHEN MOVEMENT_DESCRIPTION = 'Community Supervision End Date' THEN 100
      WHEN MOVEMENT_DESCRIPTION = 'TIS Release Return' THEN 3
      WHEN MOVEMENT_DESCRIPTION = 'Temporary Placement' THEN 2
      WHEN MOVEMENT_DESCRIPTION = 'Supervision Level Change' THEN 1
    END, 
    MOVEMENT_DESCRIPTION)
),
-- This CTE takes all the attributes that were true on a given date and carries them
-- forward in a given person's supervision stint, until they change. As of 2024-05-29, the 
-- only attribute being tracked in this way by this view is supervision level.
carry_forward_attributes AS (
  SELECT DISTINCT
    PERSON_ID,
    START_DATE,
    END_DATE,
    start_action,
    end_action,
    start_reason,
    end_reason,
    LAST_VALUE(supervision_level IGNORE NULLS) OVER (PARTITION BY PERSON_ID, DPP_ID ORDER BY START_DATE, IFNULL(END_DATE, CAST('9999-01-01' AS DATE))) AS supervision_level
    FROM periods
)
-- The final subquery in this view filters the existing queries to exclude those that
-- start with a transition to liberty, but include those that represent a period of 
-- absconsion or a period spent in custody. It also prevents supervision periods from 
-- being opened before a person is released from prison just because their supervision
-- level was assigned for their later release. 
SELECT DISTINCT
  PERSON_ID, 
  START_DATE,
  END_DATE,
  START_REASON,
  END_REASON,
  SUPERVISION_LEVEL,
  ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY START_DATE, IFNULL(END_DATE, CAST('9999-01-01' AS DATE)), START_REASON, IFNULL(END_REASON, 'ZZZ')) AS period_seq
FROM carry_forward_attributes
WHERE PERSON_ID IS NOT NULL
-- only keep zero-day periods if they are overall period admissions or releases, to preserve
-- admission and release reasons
AND (start_date != end_date 
  OR (start_date=end_date AND start_action IN ("Create", "Re-Open"))
  OR (start_date=end_date AND end_action = "Close")
  OR end_date IS NULL)
-- Only allow periods to start with a "close" action if they reflect situations that we consider
-- a part of a supervision period rather than an incarceration (investigations & absconsions)
-- NOTE: only do this when the in-custody period falls after the start of a supervision period
AND (start_action != 'Close' OR (start_action = 'Close' AND start_reason IN ('Releasee Abscond', 'Temporary Placement', 'In Custody - Other')))
-- exclude periods that begin with a supervision level being assigned and end with a supervision period starting, 
-- since these would open a supervision period before the person was actually released from prison. 
AND (NOT (start_action = 'Maintain' AND end_action = 'Create') OR end_action IS NULL)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="supervision_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
