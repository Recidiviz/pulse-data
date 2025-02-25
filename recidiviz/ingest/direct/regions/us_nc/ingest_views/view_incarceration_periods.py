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
"""Query containing information about incarceration periods in NC."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH sentences AS (
-- This CTE joins historical sentence data with the details we can find in the Inmate Profile
-- raw data (INMT4AA1) about an individual's latest period of incarceration. Most of the
-- fields added here are blank for most of the rows in the resulting dataset, because most
-- rows here do not correspond to a person's latest period of incarceration. This
-- makes it easier to find instance where people were released to parole.
-- This also updates end_dates when people died before their sentence end date.
  SELECT 
    sentences.CIDORNUM as doc_id,
    -- Since this is based on sentence data, every row has both a start date and an end date.
    GIEFFDT as sentence_begin_date,
    CASE 
      WHEN PARBEGDT != '0001-01-01 00:00:00' THEN LEAST(PARBEGDT, CIRELDAT) 
      WHEN CILAMVTY = 'DEATH' THEN LEAST(CILAMVDT, CIRELDAT)
      ELSE CIRELDAT 
    END AS end_date,
    CIPREFIX as sentence_prefix,
    PARBEGDT as parole_begin_date,
    CILAMVTY as movement_type,
    CILAMVDT as movement_date,
    CICURLOC as facility,
    CICCLASS as custody_level,
  FROM {INMT4BB1} sentences
  LEFT JOIN {INMT4AA1} profiles
  ON sentences.CIDORNUM = profiles.CIDORNUM
  AND sentences.CIPREFIX = profiles.CIPREFX2
  -- sentence start date is not in the future
  WHERE GIEFFDT < CAST(@update_timestamp as string)
),
-- This CTE creates a new period in any case where a person was 'RETURNED FROM PAROLE'
-- after being released to supervision. Since their new 'end_date' is the
-- date on which their supervision period began, any movement involving a return from
-- parole after that is assumed to be a revocation.
-- This CTE adjusts the movement reason field for rows in which a person was released
-- to parole before their sentence end date.
new_revocation_periods AS (
  SELECT 
    doc_id,
    sentence_begin_date,
    end_date,
    CASE
      WHEN parole_begin_date != '0001-01-01 00:00:00'
      AND end_date = parole_begin_date 
      THEN 'PAROLE/RETURN TO PAR'
      ELSE movement_type 
    END AS movement_type,
    movement_date,
    facility,
    custody_level,
    parole_begin_date,
    sentence_prefix
  FROM sentences

  UNION ALL 

  SELECT 
    doc_id, 
    movement_date as sentence_begin_date,
    null as end_date,
    movement_type,
    movement_date,
    facility,
    custody_level,
    parole_begin_date,
    sentence_prefix
  FROM sentences
  -- if this person was revoked after being released to parole
  WHERE sentences.movement_date > end_date 
  AND sentences.movement_type IN ('RETURNED FROM PAROLE')
), 
-- This CTE separates movement type into period start or end reasons based on which
-- date the movement occurred on.
sentences_with_reasons_split AS (
  SELECT 
    doc_id,
    sentence_begin_date as start_date,
    end_date,
    CASE 
      WHEN ABS(DATE_DIFF(CAST(movement_date AS DATETIME), CAST(sentence_begin_date AS DATETIME), DAY)) < 5
      THEN movement_type 
      ELSE NULL 
    END AS start_reason,
    CASE
      WHEN ABS(DATE_DIFF(CAST(movement_date AS DATETIME), CAST(end_date AS DATETIME), DAY)) < 5
      THEN movement_type
      ELSE NULL
    END AS end_reason,
    facility,
    custody_level,
    -- measure to remove sentences that are served concurrently and start on the same date
    ROW_NUMBER() OVER (PARTITION BY doc_id, sentence_begin_date ORDER BY end_date DESC) as shared_start_chron_order,
FROM new_revocation_periods
-- Because we match start/end reasons by movement date and call any movement date a "match" 
-- that is within 5 days of the start or end date, periods that are shorter than 5 days 
-- appear with identical start and end reasons unless we filter them out. (see PR #20262)
-- We do this because many movement dates do not exactly match the start/end date of a sentence,
-- despite corresponding to them in reality.
WHERE DATE_DIFF(CAST(end_date AS DATETIME), CAST(sentence_begin_date AS DATETIME), DAY) > 5),

remove_nested_periods AS ( 
  SELECT *,
    LAG(start_date) OVER (PARTITION BY doc_id ORDER BY start_date) as prev_start_date,
    LAG(end_date) OVER (PARTITION BY doc_id ORDER BY start_date) as prev_end_date
  FROM sentences_with_reasons_split
  WHERE shared_start_chron_order = 1
)

SELECT  
  doc_id,
  start_date,
  end_date,
  start_reason,
  end_reason,
  facility,
  custody_level,
  ROW_NUMBER() OVER (PARTITION BY doc_id ORDER BY start_date) AS sentence_order,
FROM remove_nested_periods
WHERE ((prev_start_date IS NULL AND prev_end_date IS NULL)
  OR (prev_start_date < start_date AND prev_end_date < end_date))
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nc",
    ingest_view_name="incarceration_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
