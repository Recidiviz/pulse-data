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
"""Query containing supervision period information from the following tables:
RCDVZ_PRDDTA_OP011P, RCDVZ_PRDDTA_OP010P, RCDVZ_PRDDTA_OP009P, RCDVZ_PRDDTA_OP008P,
RCDVZ_DOCDTA_TBLOCA, RCDVZ_CISPRDDTA_CMOFPSC

"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
-- These are cell level movements, already in period form, and the where qualifiers make sure we are grabbing the correct
-- date times since there are a lot of erroneous/incorrect open periods. This makes sure an open period correctly signifies 
-- someone has no additional movements and is still incarcerated. 
transfers AS (
  SELECT t.*
  FROM (
    SELECT 
        RECORD_KEY,
        CUSTODY_NUMBER,
        ADMISSION_NUMBER,
        TRANSFER_NUMBER,
        MOVE_IN_DATE,
        MOVE_OUT_DATE,
        OUTCOUNT_REASON,
        FACILITY,
        CELL_NUMBER,
        LEAD(MOVE_IN_DATE) OVER (PARTITION BY RECORD_KEY ORDER BY CAST(CUSTODY_NUMBER AS INT64), CAST(ADMISSION_NUMBER AS INT64), CAST(TRANSFER_NUMBER AS INT64), MOVE_IN_DATE) AS OPEN,
        ROW_NUMBER() OVER(PARTITION BY RECORD_KEY, MOVE_IN_DATE ORDER BY MOVE_IN_DATE, MOVE_OUT_DATE DESC, CAST(CUSTODY_NUMBER AS INT64), CAST(ADMISSION_NUMBER AS INT64), CAST(TRANSFER_NUMBER AS INT64)) AS seq
    FROM {RCDVZ_PRDDTA_OP011P}
    ) t
    WHERE seq = 1
    AND (MOVE_OUT_DATE IS NOT NULL OR OPEN IS NULL)
    AND (MOVE_IN_DATE < MOVE_OUT_DATE OR MOVE_OUT_DATE IS NULL) # removes bad data with reverse periods (but need to keep actually open periods)
), 
-- These are slightly higher level movements than the previous CTE, primarily transfers. This table is already in period
-- form and the TRANSFER_REASON helps fill in more period information.
high_level_transfers AS (
  SELECT
    RECORD_KEY, 
    CUSTODY_NUMBER,
    ADMISSION_NUMBER,
    TRANSFER_NUMBER,
    TRANSFER_IN_LOCATION,
    TRANSFER_IN_DATE,
    TRANSFER_REASON,
    TRANSFER_TO_DATE,
    RESPONSIBLE_DIVISION
  FROM {RCDVZ_PRDDTA_OP010P}
), 
-- These are slightly higher level movements than the previous CTE, primarily release information. This table is already in period
-- form and the RELEASE_REASON helps fill in more period information. 
releases AS (
  SELECT
    RECORD_KEY, 
    CUSTODY_NUMBER,
    ADMISSION_NUMBER,
    CURRENT_STATUS,
    RELEASE_DATE,
    RELEASE_REASON
  FROM {RCDVZ_PRDDTA_OP009P}
), 
-- This table contains the highlest level custody information of the previous CTEs, primarily custody information. One person should 
-- have the same custody number through incarceration to supervision until liberty. This table is already in period form.
custody AS (
  SELECT 
    RECORD_KEY, 
    CUSTODY_NUMBER,
    CUSTODY_DATE,
    CUSTODY_TYPE,
    DISCHARGE_DATE,
  FROM {RCDVZ_PRDDTA_OP008P}
), 
-- This CTE pulls additional information about the locations someone may be reporting to. 
locations AS (
  SELECT
    LOCATION_CODE,
    LOCATION_TYPE,
    LOCATION_NAME,
    COUNTY,
  FROM {RCDVZ_DOCDTA_TBLOCA}
), 
-- This CTE pulls supervision level data for a person and normalizes the erroneous values that all fit into Low/Med/High supervision level.
clean_supervision_levels AS (
  SELECT
    RECORD_KEY, 
    ENTRY_DATE, 
    CASE 
      WHEN COMMUNITY_SUPER_LVL IN ('LOW', 'LTD', 'LMT', 'LDT', 'L0W', 'LIM') THEN 'Low'
      WHEN COMMUNITY_SUPER_LVL IN ('MED', 'M') THEN 'Medium'
      WHEN COMMUNITY_SUPER_LVL IN ('HI', 'HIG') THEN 'High'
    ELSE NULL END AS COMMUNITY_SUPER_LVL, 
  FROM {RCDVZ_CISPRDDTA_CMOFRH}
  WHERE ENTRY_DATE IS NOT NULL
),
-- This CTE makes the higher level of supervision take precedent over the others in the event they get conflicting supervision levels assigned on the same date.
highest_suplevel AS (
  SELECT 
    RECORD_KEY,
    ENTRY_DATE,
    COMMUNITY_SUPER_LVL,
    ROW_NUMBER() OVER (PARTITION BY RECORD_KEY, ENTRY_DATE ORDER BY CASE WHEN COMMUNITY_SUPER_LVL = 'High' THEN 1 WHEN COMMUNITY_SUPER_LVL = 'Medium' THEN 2 WHEN COMMUNITY_SUPER_LVL = 'Low' THEN 3 ELSE 4 END) AS weighted_suplevel
  FROM clean_supervision_levels
  WHERE COMMUNITY_SUPER_LVL IS NOT NULL
),
-- Grabbing the higher supervision level per date per person and get the last supervision level, making the previous one X if there were none before. 
prev_supervision_level AS (
  SELECT 
    RECORD_KEY,
    ENTRY_DATE, 
    COMMUNITY_SUPER_LVL,
    IFNULL(LAG(COMMUNITY_SUPER_LVL) OVER(PARTITION BY RECORD_KEY ORDER BY ENTRY_DATE), 'X') AS LAST_LEVEL,
  FROM highest_suplevel
  WHERE weighted_suplevel = 1
),
-- Grabbing only where supervision level changes and is populated so we don't create extra unnecessary periods. 
supervision_changes AS (
  SELECT * 
  FROM prev_supervision_level
  WHERE COMMUNITY_SUPER_LVL !=  LAST_LEVEL
),
-- Joining in other relevant transfer tables to the base transfers from OP011P, custody CTE has released from over
-- arching custody periods so pulling in the release reason when relevant. 
periods AS (
  SELECT 
    transfers.RECORD_KEY,
    ROW_NUMBER() OVER (PARTITION BY transfers.RECORD_KEY ORDER BY CAST(MOVE_IN_DATE AS DATETIME), CAST(MOVE_OUT_DATE AS DATETIME), CAST(transfers.CUSTODY_NUMBER AS INT64), CAST(transfers.ADMISSION_NUMBER AS INT64), CAST(transfers.TRANSFER_NUMBER AS INT64)) AS PERIOD_ID,
    transfers.CUSTODY_NUMBER,
    transfers.ADMISSION_NUMBER,
    transfers.TRANSFER_NUMBER,
    CURRENT_STATUS,
    MOVE_IN_DATE, # admission_date
    MOVE_OUT_DATE, # release_date
    locations.COUNTY,
    locations.LOCATION_NAME,
    locations.LOCATION_TYPE,
    FACILITY,
    transfers.OUTCOUNT_REASON AS ADMISSION_REASON,
    IF(DATE(transfers.MOVE_OUT_DATE) = (DATE(RELEASE_DATE)), RELEASE_REASON, null) AS RELEASE_REASON,
    high_level_transfers.RESPONSIBLE_DIVISION, # custodial_authority 
    high_level_transfers.TRANSFER_TO_DATE,
    TRANSFER_REASON,
  FROM transfers
  LEFT JOIN high_level_transfers
    ON transfers.RECORD_KEY = high_level_transfers.RECORD_KEY
    AND transfers.CUSTODY_NUMBER = high_level_transfers.CUSTODY_NUMBER
    AND transfers.ADMISSION_NUMBER = high_level_transfers.ADMISSION_NUMBER
    AND transfers.TRANSFER_NUMBER = high_level_transfers.TRANSFER_NUMBER
  LEFT JOIN locations 
    ON transfers.FACILITY = locations.LOCATION_CODE
  LEFT JOIN releases
    ON transfers.RECORD_KEY = releases.RECORD_KEY
    AND transfers.CUSTODY_NUMBER = releases.CUSTODY_NUMBER
    AND transfers.ADMISSION_NUMBER = releases.ADMISSION_NUMBER
  WHERE LOCATION_TYPE NOT IN ('L', 'I') # Institution or Jail 
  AND high_level_transfers.RESPONSIBLE_DIVISION NOT IN ('I', 'L') -- Don't want Supervision Period where custodial authority is jail or prison facility
  AND CURRENT_STATUS NOT IN ('IN', 'LC') -- Some CURRENT_STATUS of IN/LC (facility/jail) have LOCATION_TYPE type of community so we want to make sure to exclude those
),
--  Joining in custody level where the date is between MOVE_IN_DATE and MOVE_OUT_DATE. 
adding_supervision_level AS (
  SELECT 
    periods.RECORD_KEY,
    PERIOD_ID,
    CURRENT_STATUS,
    LOCATION_TYPE, 
    MOVE_IN_DATE,
    MOVE_OUT_DATE,
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    sc.COMMUNITY_SUPER_LVL,
    sc.ENTRY_DATE,
    ADMISSION_REASON,
    IF(MOVE_OUT_DATE = TRANSFER_TO_DATE AND RELEASE_REASON IS NULL, TRANSFER_REASON, RELEASE_REASON) AS RELEASE_REASON, ## see changes between this and cust level 
    RESPONSIBLE_DIVISION,
  FROM periods
  LEFT JOIN supervision_changes sc
  ON periods.RECORD_KEY = sc.RECORD_KEY 
  AND sc.ENTRY_DATE BETWEEN MOVE_IN_DATE AND COALESCE(MOVE_OUT_DATE,'9999-01-01')
),
-- Setting up to add additional periods for when multiple supervision level classifications happen within one existing period,
-- and carrying admission reasons for next period through to release reason of previous to populate. 
info_to_split_periods_with_supervision_level_changes AS ( 
  SELECT 
    RECORD_KEY,
    PERIOD_ID,
    LEAD(PERIOD_ID) OVER(PARTITION BY RECORD_KEY ORDER BY PERIOD_ID, ENTRY_DATE) AS NEXT_PERIOD,
    LAG(PERIOD_ID) OVER(PARTITION BY RECORD_KEY ORDER BY PERIOD_ID, ENTRY_DATE) AS LAST_PERIOD,
    CURRENT_STATUS,
    LOCATION_TYPE, 
    MOVE_IN_DATE,
    MOVE_OUT_DATE,
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    COMMUNITY_SUPER_LVL,
    ENTRY_DATE,
    ADMISSION_REASON,
    IFNULL(RELEASE_REASON, LEAD(ADMISSION_REASON) OVER (PARTITION BY RECORD_KEY ORDER BY period_id)) AS RELEASE_REASON,
    RESPONSIBLE_DIVISION
  FROM adding_supervision_level
), 
-- Breaking up duplicate periods with different supervision levels by using entry_date to end one and begin the next, 
-- also adding CUST_CHANGE as admission/release reason for these instances.
split_periods_with_multiple_supervision_levels AS (
  SELECT DISTINCT
    RECORD_KEY,
    PERIOD_ID AS OLD_PERIOD,
    LAST_PERIOD,
    NEXT_PERIOD,
    CURRENT_STATUS,
    LOCATION_TYPE, 
    IF(PERIOD_ID = LAST_PERIOD, LAG(ENTRY_DATE) OVER(PARTITION BY RECORD_KEY ORDER BY PERIOD_ID, MOVE_IN_DATE), MOVE_IN_DATE) AS MOVE_IN_DATE, 
    IF(PERIOD_ID = NEXT_PERIOD, ENTRY_DATE, MOVE_OUT_DATE) AS MOVE_OUT_DATE, 
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    COMMUNITY_SUPER_LVL,
    IF(PERIOD_ID = LAST_PERIOD, 'SUPLEVEL_CHANGE', ADMISSION_REASON) AS ADMISSION_REASON,
    IF(PERIOD_ID = NEXT_PERIOD, 'SUPLEVEL_CHANGE', RELEASE_REASON) AS RELEASE_REASON,
    RESPONSIBLE_DIVISION
  FROM info_to_split_periods_with_supervision_level_changes
), 
-- Reassinging period id after properly splitting periods with supervision level changes. 
new_periods AS (
  SELECT DISTINCT
    RECORD_KEY,
    ROW_NUMBER() OVER (PARTITION BY RECORD_KEY ORDER BY MOVE_IN_DATE, MOVE_OUT_DATE, spl.CURRENT_STATUS, LOCATION_TYPE, COUNTY, LOCATION_NAME, FACILITY, COMMUNITY_SUPER_LVL, spl.ADMISSION_REASON, RELEASE_REASON, spl.RESPONSIBLE_DIVISION) AS PERIOD_ID,
    spl.CURRENT_STATUS,
    LOCATION_TYPE, 
    MOVE_IN_DATE,
    MOVE_OUT_DATE,
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    COMMUNITY_SUPER_LVL,
    spl.ADMISSION_REASON,
    RELEASE_REASON,
    spl.RESPONSIBLE_DIVISION,
  FROM split_periods_with_multiple_supervision_levels spl
), 
-- Getting supervision levels that were assigned within the 30 days before the first supervision period.
supervision_levels_prior_to_supervision_start AS (
  SELECT b.*
  FROM (
    SELECT
      np.RECORD_KEY,
      PERIOD_ID,
      CURRENT_STATUS,
      LOCATION_TYPE, 
      MOVE_IN_DATE,
      MOVE_OUT_DATE,
      COUNTY,
      LOCATION_NAME,
      FACILITY,
      IFNULL(np.COMMUNITY_SUPER_LVL, sc.COMMUNITY_SUPER_LVL) AS COMMUNITY_SUPER_LVL,
      ADMISSION_REASON,
      RELEASE_REASON,
      RESPONSIBLE_DIVISION,
      ROW_NUMBER() OVER (PARTITION BY np.RECORD_KEY, period_id ORDER BY ENTRY_DATE DESC) as seq
    FROM new_periods np
    LEFT JOIN supervision_changes sc
    ON np.RECORD_KEY = sc.RECORD_KEY
    AND period_id = 1
    AND (DATE(sc.ENTRY_DATE)) BETWEEN DATE_SUB(DATE(MOVE_IN_DATE), INTERVAL 30 DAY) AND (DATE(MOVE_IN_DATE))
  ) b
  WHERE seq = 1
),
-- OR overwrites their CASELOAD data so we are using @ALL and the LAST_UPDATE_WHEN field to capture when supervising officer changes
staff_caseloads AS (
  SELECT DISTINCT 
    RECORD_KEY, 
    CASELOAD, 
    LAST_UPDATED_WHEN
    FROM (
      SELECT DISTINCT 
        RECORD_KEY,
        CASELOAD,
        LAST_UPDATED_WHEN,
        IFNULL(LAG(caseload) OVER(PARTITION BY record_key ORDER BY LAST_UPDATED_WHEN), 'X') AS LAST_CASELOAD
      FROM {RCDVZ_PRDDTA_OP013P@ALL}
    )
    # Adding this filter to make sure all caseloads have a corresponding entry in CMCMST, which we use for State_Staff
    WHERE CASELOAD IN (SELECT DISTINCT CASELOAD FROM {RCDVZ_CISPRDDTA_CMCMST@ALL})
    AND CASELOAD != LAST_CASELOAD
), 
-- Joining in CASELOAD where the LAST_UPDATED_WHEN date is between MOVE_IN_DATE and MOVE_OUT_DATE.
adding_caseloads AS (
  SELECT 
    sls.RECORD_KEY,
    PERIOD_ID,
    CURRENT_STATUS,
    LOCATION_TYPE, 
    MOVE_IN_DATE,
    MOVE_OUT_DATE,
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    COMMUNITY_SUPER_LVL,
    ADMISSION_REASON,
    RELEASE_REASON,
    RESPONSIBLE_DIVISION,
    CASELOAD,
    LAST_UPDATED_WHEN
  FROM supervision_levels_prior_to_supervision_start sls
  LEFT JOIN staff_caseloads sc
  ON sls.RECORD_KEY = sc.RECORD_KEY 
  AND sc.LAST_UPDATED_WHEN BETWEEN MOVE_IN_DATE AND COALESCE(MOVE_OUT_DATE,'9999-01-01')
),
-- Setting up to add additional periods for when multiple caseload changes happen within one existing period,
-- and carrying admission reasons for next period through to release reason of previous to populate. 
setup_split_periods_with_caseload_changes AS ( 
  SELECT 
    RECORD_KEY,
    PERIOD_ID,
    LEAD(PERIOD_ID) OVER(PARTITION BY RECORD_KEY ORDER BY PERIOD_ID, LAST_UPDATED_WHEN) AS NEXT_PERIOD,
    LAG(PERIOD_ID) OVER(PARTITION BY RECORD_KEY ORDER BY PERIOD_ID, LAST_UPDATED_WHEN) AS LAST_PERIOD,
    CURRENT_STATUS,
    LOCATION_TYPE, 
    MOVE_IN_DATE,
    MOVE_OUT_DATE,
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    COMMUNITY_SUPER_LVL,
    LAST_UPDATED_WHEN,
    ADMISSION_REASON,
    RELEASE_REASON,
    RESPONSIBLE_DIVISION, 
    CASELOAD
  FROM adding_caseloads
), 
-- Breaking up duplicate periods with different CASELOADS by using LAST_UPDATED_WHEN to end one and begin the next, 
-- also adding CASELOAD_CHANGE as admission/release reason for these instances.
split_periods_with_multiple_caseload_changes AS (
  SELECT DISTINCT
    RECORD_KEY,
    PERIOD_ID AS OLD_PERIOD,
    LAST_PERIOD,
    NEXT_PERIOD,
    CURRENT_STATUS,
    LOCATION_TYPE, 
    IF(PERIOD_ID = LAST_PERIOD, LAG(LAST_UPDATED_WHEN) OVER(PARTITION BY RECORD_KEY ORDER BY PERIOD_ID, MOVE_IN_DATE), MOVE_IN_DATE) AS MOVE_IN_DATE, 
    IF(PERIOD_ID = NEXT_PERIOD, LAST_UPDATED_WHEN, MOVE_OUT_DATE) AS MOVE_OUT_DATE, 
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    COMMUNITY_SUPER_LVL,
    IF(PERIOD_ID = LAST_PERIOD, 'CASELOAD_CHANGE', ADMISSION_REASON) AS ADMISSION_REASON,
    IF(PERIOD_ID = NEXT_PERIOD, 'CASELOAD_CHANGE', RELEASE_REASON) AS RELEASE_REASON,
    RESPONSIBLE_DIVISION, 
    CASELOAD
  FROM setup_split_periods_with_caseload_changes
), 
-- Re-numbering the period id after splitting periods.
fixed_periods AS (
  SELECT DISTINCT
    RECORD_KEY,
    ROW_NUMBER() OVER (PARTITION BY RECORD_KEY ORDER BY MOVE_IN_DATE, MOVE_OUT_DATE, spc.CURRENT_STATUS, LOCATION_TYPE, COUNTY, LOCATION_NAME, FACILITY, COMMUNITY_SUPER_LVL, spc.ADMISSION_REASON, RELEASE_REASON, spc.RESPONSIBLE_DIVISION) AS PERIOD_ID,
    CURRENT_STATUS,
    LOCATION_TYPE, 
    MOVE_IN_DATE,
    MOVE_OUT_DATE,
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    COMMUNITY_SUPER_LVL,
    ADMISSION_REASON,
    RELEASE_REASON,
    RESPONSIBLE_DIVISION,
    CASELOAD
  FROM split_periods_with_multiple_caseload_changes spc
), 
-- Each person only has one row in OP013P latest so this grabs their most up to date caseload. 
current_caseloads AS (
  SELECT DISTINCT CASELOAD, RECORD_KEY 
  FROM {RCDVZ_PRDDTA_OP013P}
  WHERE CASELOAD IN (SELECT DISTINCT CASELOAD FROM RCDVZ_CISPRDDTA_CMCMST__ALL_generated_view)
),
-- Some caseloads are assigned before or after all supervision period dates. In order to ensure all open periods have the most accurate 
-- caseload for products, join back to source of truth OP013P latest when open period caseload is null.
caseloads_for_open_periods AS (
  SELECT DISTINCT
    fp.RECORD_KEY,
    PERIOD_ID, 
    fp.CURRENT_STATUS,
    LOCATION_TYPE, 
    MOVE_IN_DATE,
    MOVE_OUT_DATE,
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    fp.COMMUNITY_SUPER_LVL,
    fp.ADMISSION_REASON,
    fp.RELEASE_REASON,
    fp.RESPONSIBLE_DIVISION, 
    IFNULL(fp.CASELOAD, fill.CASELOAD) AS CASELOAD
  FROM fixed_periods fp
  LEFT JOIN  current_caseloads fill
  ON fp.RECORD_KEY = fill.RECORD_KEY
  AND fp.MOVE_OUT_DATE IS NULL 
),
-- Carrying supervision level and caseload id over periods. 
final AS (
  SELECT DISTINCT
    RECORD_KEY,
    PERIOD_ID, 
    CURRENT_STATUS,
    LOCATION_TYPE, 
    MOVE_IN_DATE,
    MOVE_OUT_DATE,
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    LAST_VALUE(COMMUNITY_SUPER_LVL ignore nulls) OVER (periods_for_person range between UNBOUNDED preceding and current row) AS COMMUNITY_SUPER_LVL,
    ADMISSION_REASON,
    RELEASE_REASON,
    RESPONSIBLE_DIVISION, 
    LAST_VALUE(CASELOAD ignore nulls) OVER (periods_for_person range between UNBOUNDED preceding and current row) AS CASELOAD,
  FROM caseloads_for_open_periods
  WINDOW periods_for_person AS (PARTITION BY RECORD_KEY ORDER BY PERIOD_ID)
)
SELECT * FROM final
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_or",
    ingest_view_name="Supervision_Period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
