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
"""Query containing incarceration period information from the following tables:
RCDVZ_PRDDTA_OP011P, RCDVZ_PRDDTA_OP010P, RCDVZ_PRDDTA_OP009P, RCDVZ_PRDDTA_OP008P,
RCDVZ_DOCDTA_TBLOCA, RCDVZ_DOCDTA_TBCELL, RCDVZ_CISPRDDTA_CLOVER, RCDVZ_CISPRDDT_CLCLHD

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
    AND (MOVE_OUT_DATE IS NOT NULL OR OPEN IS NULL) # cleans out random open periods while keeping intentionally open ones
    AND (MOVE_IN_DATE < MOVE_OUT_DATE OR OPEN IS NULL) # removes bad data with reverse periods (but need to keep actually open periods)
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
), units AS (
  SELECT DISTINCT
    LOCATION_CODE,
    CELL_NUMBER,
    UNIT_NUMBER,
  FROM {RCDVZ_DOCDTA_TBCELL}
), cust_clclhd AS (
  # The subquery within this one is to ensure we only get one, and the most accurate, custody level per date.
  SELECT  c.* 
  FROM (
    SELECT DISTINCT
      RECORD_KEY,
      EFFECTIVE_DATE,
      CASE 
        WHEN INSTITUTION_RISK IN ('1', '2') THEN 'MINIMUM'
        WHEN INSTITUTION_RISK = '3' THEN 'MEDIUM'
        WHEN INSTITUTION_RISK = '4' THEN 'CLOSE'
        WHEN INSTITUTION_RISK = '5' THEN 'MAXIMUM'
        ELSE INSTITUTION_RISK
      END AS INSTITUTION_RISK1,
      ROW_NUMBER() OVER(PARTITION BY RECORD_KEY, EFFECTIVE_DATE ORDER BY EFFECTIVE_DATE DESC, CLASS_ACTION_DATE DESC) AS seq
    FROM {RCDVZ_CISPRDDT_CLCLHD}
    WHERE INSTITUTION_RISK NOT LIKE 'P%'
  ) c
  WHERE seq = 1
), cust_clover AS (
  # The subquery within this one is to ensure we only get one, and the most accurate, custody level override per date.
  # Also, making all custody levels text so it doesn't consider custody level 1 to Minimum a change.
  SELECT cl.* 
  FROM (
    SELECT DISTINCT 
    RECORD_KEY,
    EFFECTIVE_DATE,
    CASE 
      WHEN INSTITUTION_RISK IN ('1', '2', 'P-1', 'P-2', 'P-MINIMUM') THEN 'MINIMUM'
      WHEN INSTITUTION_RISK IN ('3', 'P-3', 'P-MEDIUM')  THEN 'MEDIUM'
      WHEN INSTITUTION_RISK IN ('4', 'P-4', 'P-CLOSE') THEN 'CLOSE'
      WHEN INSTITUTION_RISK IN ('5', 'P-5', 'P-MAXIMUM') THEN 'MAXIMUM'
      ELSE INSTITUTION_RISK
    END AS INSTITUTION_RISK1,
    ROW_NUMBER() OVER(PARTITION BY RECORD_KEY, EFFECTIVE_DATE ORDER BY EFFECTIVE_DATE DESC, SEQUENCE_NO DESC) AS seq,
  FROM {RCDVZ_CISPRDDTA_CLOVER}
  ) cl
  WHERE seq = 1
),
rank_cust AS (
  # Because CLOVER is the override table, we want to make sure we take preference of the custody level there, if exists. 
  SELECT 
    RECORD_KEY,
    clc.EFFECTIVE_DATE, 
    IF(clc.EFFECTIVE_DATE = clo.EFFECTIVE_DATE, clo.INSTITUTION_RISK1, clc.INSTITUTION_RISK1) AS INSTITUTION_RISK
  FROM cust_clclhd clc
  LEFT JOIN cust_clover clo
  USING (RECORD_KEY, EFFECTIVE_DATE) 
),
rank_cust_level AS (
  SELECT 
    RECORD_KEY,
    EFFECTIVE_DATE, 
    INSTITUTION_RISK,
    LAG(INSTITUTION_RISK) OVER(PARTITION BY RECORD_KEY ORDER BY EFFECTIVE_DATE) AS LAST_LEVEL,
  FROM rank_cust
), cust_changes AS (
  # Grabbing only where custody level changes and is populated so we don't create extra unnecessary periods. 
  SELECT * 
  FROM rank_cust_level
  WHERE INSTITUTION_RISK !=  LAST_LEVEL AND INSTITUTION_RISK IS NOT NULL
),
periods AS (
  # Joining in other relevant transfer tables to the base cell movements from OP011P, custody CTE has released from over
  # arching custody periods so pulling in the release reason when relevant. 
  SELECT 
    transfers.RECORD_KEY,
    ROW_NUMBER() OVER (PARTITION BY transfers.RECORD_KEY ORDER BY CAST(transfers.CUSTODY_NUMBER AS INT64), CAST(transfers.ADMISSION_NUMBER AS INT64), CAST(transfers.TRANSFER_NUMBER AS INT64), CAST(MOVE_IN_DATE AS DATETIME)) AS PERIOD_ID,
    transfers.CUSTODY_NUMBER,
    transfers.ADMISSION_NUMBER,
    transfers.TRANSFER_NUMBER,
    releases.CURRENT_STATUS,
    custody.CUSTODY_TYPE, # incarceration_type
    MOVE_IN_DATE, # admission_date
    MOVE_OUT_DATE, # release_date
    locations.COUNTY,
    locations.LOCATION_NAME,
    locations.LOCATION_TYPE,
    FACILITY,
    LAG(FACILITY) OVER (PARTITION BY transfers.RECORD_KEY ORDER BY CAST(transfers.CUSTODY_NUMBER AS INT64), CAST(transfers.ADMISSION_NUMBER AS INT64), CAST(transfers.TRANSFER_NUMBER AS INT64), CAST(MOVE_IN_DATE AS DATETIME)) AS LAST_FACILITY,
    transfers.CELL_NUMBER, # housing_unit
    UNIT_NUMBER,
    LAG(UNIT_NUMBER) OVER (PARTITION BY transfers.RECORD_KEY ORDER BY CAST(transfers.CUSTODY_NUMBER AS INT64), CAST(transfers.ADMISSION_NUMBER AS INT64), CAST(transfers.TRANSFER_NUMBER AS INT64), CAST(MOVE_IN_DATE AS DATETIME)) AS LAST_UNIT, 
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
  LEFT JOIN custody
    ON transfers.RECORD_KEY = custody.RECORD_KEY
    AND transfers.CUSTODY_NUMBER = custody.CUSTODY_NUMBER
  LEFT JOIN locations 
    ON transfers.FACILITY = locations.LOCATION_CODE
  LEFT JOIN units 
    ON transfers.FACILITY = units.LOCATION_CODE
    AND transfers.CELL_NUMBER = units.CELL_NUMBER
  LEFT JOIN releases
    ON transfers.RECORD_KEY = releases.RECORD_KEY
    AND transfers.CUSTODY_NUMBER = releases.CUSTODY_NUMBER
    AND transfers.ADMISSION_NUMBER = releases.ADMISSION_NUMBER
  WHERE LOCATION_TYPE IN ('L', 'I') # Institution or Jail 
  AND high_level_transfers.RESPONSIBLE_DIVISION IN ('I', 'L') # Only counting as Incarceration Period if custodial authority is jail or prison facility
), merging_units1 AS (
  # Because schema has housing unit but not cell number, we want to squash cell transfers happening within same units.
  SELECT
    periods.RECORD_KEY,
    PERIOD_ID,
    LOCATION_TYPE, 
    IF(FACILITY = LAST_FACILITY AND UNIT_NUMBER = LAST_UNIT, null, MOVE_IN_DATE) AS MOVE_IN_DATE,
    MOVE_OUT_DATE,
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    LAST_FACILITY,
    CELL_NUMBER, 
    UNIT_NUMBER, 
    LAST_UNIT,
    ADMISSION_REASON,
    RELEASE_REASON,
    RESPONSIBLE_DIVISION,
    TRANSFER_TO_DATE,
    TRANSFER_REASON,
  FROM periods
), merging_units2 AS (
  # Bringing MOVE_IN_DATE down to last MOVE_IN_DATE with the same unit number.
  SELECT
    RECORD_KEY,
    PERIOD_ID,
    LOCATION_TYPE, 
    LAST_VALUE(MOVE_IN_DATE ignore nulls) OVER (periods_for_units range between UNBOUNDED preceding and current row) AS MOVE_IN_DATE,
    MOVE_OUT_DATE,
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    LAST_FACILITY,
    CELL_NUMBER, 
    UNIT_NUMBER, 
    LAST_UNIT,
    ADMISSION_REASON,
    RELEASE_REASON,
    RESPONSIBLE_DIVISION,
    TRANSFER_TO_DATE,
    TRANSFER_REASON,
  FROM merging_units1
  WINDOW periods_for_units AS (PARTITION BY RECORD_KEY ORDER BY PERIOD_ID)
), merging_units3 AS (
  # Taking the MOVE_IN_DATE with the most recent MOVE_OUT_DATE to collapse periods with cell movements in same unit. 
  SELECT mu.* FROM (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY RECORD_KEY, merging_units2.MOVE_IN_DATE ORDER BY MOVE_OUT_DATE DESC) as seq
    FROM merging_units2
  ) mu
  WHERE seq = 1
), added_releases AS (
  # Joining in custody level where the date is between MOVE_IN_DATE and MOVE_OUT_DATE.
  SELECT 
    merging_units3.RECORD_KEY,
    PERIOD_ID,
    LOCATION_TYPE, 
    MOVE_IN_DATE,
    MOVE_OUT_DATE,
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    LAST_FACILITY,
    CELL_NUMBER, 
    UNIT_NUMBER, 
    LAG(CELL_NUMBER) OVER(PARTITION BY merging_units3.RECORD_KEY ORDER BY PERIOD_ID) AS LAST_CELL,
    INSTITUTION_RISK,
    EFFECTIVE_DATE,
    ADMISSION_REASON,
    IF(MOVE_OUT_DATE = TRANSFER_TO_DATE AND RELEASE_REASON IS NULL, TRANSFER_REASON, RELEASE_REASON) AS RELEASE_REASON,
    RESPONSIBLE_DIVISION,
  FROM merging_units3
  LEFT JOIN cust_changes rcl
  ON (merging_units3.RECORD_KEY = rcl.RECORD_KEY AND rcl.EFFECTIVE_DATE BETWEEN MOVE_IN_DATE AND MOVE_OUT_DATE)
), cell_changes AS ( 
  # Adding UNIT_CHANGE as admission reason when null
  SELECT 
    RECORD_KEY,
    PERIOD_ID,
    LOCATION_TYPE, 
    MOVE_IN_DATE,
    MOVE_OUT_DATE,
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    LAST_FACILITY,
    CELL_NUMBER, 
    UNIT_NUMBER, 
    LAST_CELL, 
    INSTITUTION_RISK,
    EFFECTIVE_DATE,
    IF(ADMISSION_REASON IS NULL AND LAST_FACILITY = FACILITY AND LAST_CELL != CELL_NUMBER, 'UNIT_CHANGE', ADMISSION_REASON) AS ADMISSION_REASON,
    RELEASE_REASON,
    RESPONSIBLE_DIVISION
  FROM added_releases
),
additional_releases AS ( 
  # Setting up to add additional periods for when multiple custody level classifications happen within one existing period,
  # and carrying admission reasons for next period through to release reason of previous to populate. 
  SELECT 
    RECORD_KEY,
    PERIOD_ID,
    LEAD(PERIOD_ID) OVER(PARTITION BY RECORD_KEY ORDER BY PERIOD_ID, EFFECTIVE_DATE) AS NEXT_PERIOD,
    LAG(PERIOD_ID) OVER(PARTITION BY RECORD_KEY ORDER BY PERIOD_ID, EFFECTIVE_DATE) AS LAST_PERIOD,
    LOCATION_TYPE, 
    MOVE_IN_DATE,
    MOVE_OUT_DATE,
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    CELL_NUMBER, 
    UNIT_NUMBER, 
    INSTITUTION_RISK,
    EFFECTIVE_DATE,
    ADMISSION_REASON,
    IFNULL(RELEASE_REASON, LEAD(ADMISSION_REASON) OVER (PARTITION BY RECORD_KEY ORDER BY period_id)) AS RELEASE_REASON,
    RESPONSIBLE_DIVISION
  FROM cell_changes
), add_periods AS (
  # Breaking up duplicate periods with different custody levels by using effective date to end one and begin the next, 
  # also adding CUST_CHANGE as admission/release reason for these instances.
  SELECT DISTINCT
    RECORD_KEY,
    PERIOD_ID AS OLD_PERIOD,
    LAST_PERIOD,
    NEXT_PERIOD,
    LOCATION_TYPE, 
    IF(PERIOD_ID = LAST_PERIOD, LAG(EFFECTIVE_DATE) OVER(PARTITION BY RECORD_KEY ORDER BY PERIOD_ID, MOVE_IN_DATE), MOVE_IN_DATE) AS MOVE_IN_DATE, 
    IF(PERIOD_ID = NEXT_PERIOD, EFFECTIVE_DATE, MOVE_OUT_DATE) AS MOVE_OUT_DATE, 
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    CELL_NUMBER, 
    UNIT_NUMBER, 
    INSTITUTION_RISK,
    IF(PERIOD_ID = LAST_PERIOD AND ADMISSION_REASON IS NULL, 'CUST_CHANGE', ADMISSION_REASON) AS ADMISSION_REASON,
    IF(PERIOD_ID = NEXT_PERIOD AND RELEASE_REASON IS NULL, 'CUST_CHANGE', RELEASE_REASON) AS RELEASE_REASON,
    RESPONSIBLE_DIVISION
  FROM additional_releases
),
switch_admission AS (
  # The transfer reasons from OP009P generally match better as release reasons, but there are instances of INTAKE and 
  # VIOLATION (revocation) when they should be admission reasons instead, so swapping those below. 
  # Also dropping same day periods with no admission reason or cell.
  SELECT 
    RECORD_KEY,
    ROW_NUMBER() OVER (PARTITION BY RECORD_KEY ORDER BY MOVE_IN_DATE) AS PERIOD_ID,
    OLD_PERIOD,
    LAST_PERIOD,
    NEXT_PERIOD,
    LOCATION_TYPE, 
    MOVE_IN_DATE,
    MOVE_OUT_DATE,
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    CELL_NUMBER, 
    UNIT_NUMBER, 
    INSTITUTION_RISK,
    IF(RELEASE_REASON IN ('INTK', 'INEM', 'INTA', 'INTO', 'INBH', 'IAIP', 'INMA', 'VIOL'), RELEASE_REASON, ADMISSION_REASON) AS ADMISSION_REASON,
    IF(RELEASE_REASON IN ('INTK', 'INEM', 'INTA', 'INTO', 'INBH', 'IAIP', 'INMA', 'VIOL'), 'TRANSFER', RELEASE_REASON) AS RELEASE_REASON,
    RESPONSIBLE_DIVISION
  FROM add_periods
  WHERE (DATE(MOVE_IN_DATE)) != (DATE(MOVE_OUT_DATE)) OR UNIT_NUMBER != 'NO CELL' OR ADMISSION_REASON IS NOT NULL
), final AS (
  # Carrying custody level over periods and adding a return from court release reason. 
  SELECT 
    RECORD_KEY,
    PERIOD_ID, 
    LOCATION_TYPE, 
    MOVE_IN_DATE,
    MOVE_OUT_DATE,
    COUNTY,
    LOCATION_NAME,
    FACILITY,
    UNIT_NUMBER, 
    LAST_VALUE(INSTITUTION_RISK ignore nulls) OVER (periods_for_person range between UNBOUNDED preceding and current row) AS INSTITUTION_RISK,
    ADMISSION_REASON,
    IF(ADMISSION_REASON = 'COUR' AND RELEASE_REASON IS NULL AND MOVE_OUT_DATE IS NOT NULL, 'RCOUR', RELEASE_REASON) AS RELEASE_REASON,
    RESPONSIBLE_DIVISION
  FROM switch_admission
  WINDOW periods_for_person AS (PARTITION BY RECORD_KEY ORDER BY PERIOD_ID)
)
SELECT * 
FROM final
"""


VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_or",
    ingest_view_name="Incarceration_Period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
