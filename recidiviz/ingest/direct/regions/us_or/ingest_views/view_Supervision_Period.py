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
transfers AS (
  # These are cell level movements, already in period form, and the where qualifiers make sure we are grabbing the correct
  # date times since there are a lot of erroneous incorrect open periods. This makes sure the open period correctly signifies 
  # someone has no additional movements and is still incarcerated. 
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
        ROW_NUMBER() OVER(PARTITION BY RECORD_KEY, MOVE_IN_DATE ORDER BY MOVE_IN_DATE, CAST(CUSTODY_NUMBER AS INT64), CAST(ADMISSION_NUMBER AS INT64) DESC, CAST(TRANSFER_NUMBER AS INT64)) AS seq
    FROM {RCDVZ_PRDDTA_OP011P}
    ) t
    WHERE seq = 1
    AND (MOVE_OUT_DATE IS NOT NULL OR OPEN IS NULL)
    AND (MOVE_IN_DATE < MOVE_OUT_DATE OR MOVE_OUT_DATE IS NULL) # removes bad data with reverse periods (but need to keep actually open periods)
), 
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
), releases AS (
  SELECT
    RECORD_KEY, 
    CUSTODY_NUMBER,
    ADMISSION_NUMBER,
    CURRENT_STATUS,
    RELEASE_DATE,
    RELEASE_REASON
  FROM {RCDVZ_PRDDTA_OP009P}
), custody AS (
  SELECT 
    RECORD_KEY, 
    CUSTODY_NUMBER,
    CUSTODY_DATE,
    CUSTODY_TYPE,
    DISCHARGE_DATE,
  FROM {RCDVZ_PRDDTA_OP008P}
), locations AS (
  SELECT
    LOCATION_CODE,
    LOCATION_TYPE,
    LOCATION_NAME,
    COUNTY,
  FROM {RCDVZ_DOCDTA_TBLOCA}
),
supervision_level AS (
   SELECT 
    RECORD_KEY,
    EFFECTIVE_DATE,
    UPPER(IFNULL(PROXY_RISK_LEVEL, PSC_RISK_LEVEL)) AS RISK_LEVEL,
  FROM {RCDVZ_CISPRDDTA_CMOFPSC}
  WHERE EFFECTIVE_DATE IS NOT NULL
), prev_supervision_level AS (
    SELECT 
    RECORD_KEY,
    EFFECTIVE_DATE, 
    RISK_LEVEL,
    IFNULL(LAG(RISK_LEVEL) OVER(PARTITION BY RECORD_KEY ORDER BY EFFECTIVE_DATE), 'X') AS LAST_LEVEL,
  FROM supervision_level
  WHERE RISK_LEVEL IS NOT NULL
),
supervision_changes AS (
  # Grabbing only where supervision level changes and is populated so we don't create extra unnecessary periods. 
  SELECT * 
  FROM prev_supervision_level
  WHERE RISK_LEVEL !=  LAST_LEVEL
),
periods AS (
  # Joining in other relevant transfer tables to the base transfers from OP011P, custody CTE has released from over
  # arching custody periods so pulling in the release reason when relevant. 
  SELECT 
    transfers.RECORD_KEY,
    ROW_NUMBER() OVER (PARTITION BY transfers.RECORD_KEY ORDER BY CAST(transfers.CUSTODY_NUMBER AS INT64), CAST(transfers.ADMISSION_NUMBER AS INT64), CAST(transfers.TRANSFER_NUMBER AS INT64), CAST(MOVE_IN_DATE AS DATETIME)) AS PERIOD_ID,
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
  AND high_level_transfers.RESPONSIBLE_DIVISION NOT IN ('I', 'L') # Only counting as Incarceration Period if custodial authority is jail or prison facility
), adding_supervision_level AS (
  # Joining in custody level where the date is between MOVE_IN_DATE and MOVE_OUT_DATE.
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
    sc.RISK_LEVEL,
    sc.EFFECTIVE_DATE,
    ADMISSION_REASON,
    IF(MOVE_OUT_DATE = TRANSFER_TO_DATE AND RELEASE_REASON IS NULL, TRANSFER_REASON, RELEASE_REASON) AS RELEASE_REASON, ## see changes between this and cust level 
    RESPONSIBLE_DIVISION,
  FROM periods
  LEFT JOIN supervision_changes sc
  ON periods.RECORD_KEY = sc.RECORD_KEY 
  AND sc.EFFECTIVE_DATE BETWEEN MOVE_IN_DATE AND MOVE_OUT_DATE
),
info_to_split_periods_with_supervision_level_changes AS ( 
  # Setting up to add additional periods for when multiple custody level classifications happen within one existing period,
  # and carrying admission reasons for next period through to release reason of previous to populate. 
  SELECT 
    RECORD_KEY,
    PERIOD_ID,
    LEAD(PERIOD_ID) OVER(PARTITION BY RECORD_KEY ORDER BY PERIOD_ID, EFFECTIVE_DATE) AS NEXT_PERIOD,
    LAG(PERIOD_ID) OVER(PARTITION BY RECORD_KEY ORDER BY PERIOD_ID, EFFECTIVE_DATE) AS LAST_PERIOD,
    CURRENT_STATUS,
    LOCATION_TYPE, 
    MOVE_IN_DATE,
    MOVE_OUT_DATE,
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    RISK_LEVEL,
    EFFECTIVE_DATE,
    ADMISSION_REASON,
    IFNULL(RELEASE_REASON, LEAD(ADMISSION_REASON) OVER (PARTITION BY RECORD_KEY ORDER BY period_id)) AS RELEASE_REASON,
    RESPONSIBLE_DIVISION
  FROM adding_supervision_level
), split_periods_with_multiple_supervision_levels AS (
  # Breaking up duplicate periods with different custody levels by using effective date to end one and begin the next, 
  # also adding CUST_CHANGE as admission/release reason for these instances.
  SELECT DISTINCT
    RECORD_KEY,
    PERIOD_ID AS OLD_PERIOD,
    LAST_PERIOD,
    NEXT_PERIOD,
    CURRENT_STATUS,
    LOCATION_TYPE, 
    IF(PERIOD_ID = LAST_PERIOD, LAG(EFFECTIVE_DATE) OVER(PARTITION BY RECORD_KEY ORDER BY PERIOD_ID, MOVE_IN_DATE), MOVE_IN_DATE) AS MOVE_IN_DATE, 
    IF(PERIOD_ID = NEXT_PERIOD, EFFECTIVE_DATE, MOVE_OUT_DATE) AS MOVE_OUT_DATE, 
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    RISK_LEVEL,
    IF(PERIOD_ID = LAST_PERIOD AND ADMISSION_REASON IS NULL, 'SUPLEVEL_CHANGE', ADMISSION_REASON) AS ADMISSION_REASON,
    IF(PERIOD_ID = NEXT_PERIOD AND RELEASE_REASON IS NULL, 'SUPLEVEL_CHANGE', RELEASE_REASON) AS RELEASE_REASON,
    RESPONSIBLE_DIVISION
  FROM info_to_split_periods_with_supervision_level_changes
), new_periods AS (
  SELECT
    RECORD_KEY,
    ROW_NUMBER() OVER (PARTITION BY RECORD_KEY ORDER BY MOVE_IN_DATE) AS PERIOD_ID,
    CURRENT_STATUS,
    LOCATION_TYPE, 
    MOVE_IN_DATE,
    MOVE_OUT_DATE,
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    RISK_LEVEL,
    ADMISSION_REASON,
    RELEASE_REASON,
    RESPONSIBLE_DIVISION
  FROM split_periods_with_multiple_supervision_levels
), 
final AS (
  # Carrying custody level over periods and adding a return from court release reason. 
  SELECT 
    RECORD_KEY,
    PERIOD_ID, 
    CURRENT_STATUS,
    LOCATION_TYPE, 
    MOVE_IN_DATE,
    MOVE_OUT_DATE,
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    LAST_VALUE(RISK_LEVEL ignore nulls) OVER (periods_for_person range between UNBOUNDED preceding and current row) AS RISK_LEVEL,
    ADMISSION_REASON,
    RELEASE_REASON,
    RESPONSIBLE_DIVISION
  FROM new_periods
  WINDOW periods_for_person AS (PARTITION BY RECORD_KEY ORDER BY PERIOD_ID)
)
SELECT * FROM final
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_or",
    ingest_view_name="Supervision_Period",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="RECORD_KEY, PERIOD_ID",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
