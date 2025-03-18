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
"""Query containing sentence status snapshots for sentences stored in Elite.

This information is available only at the level of bookings in Elite, not individual
sentence components. All sentences that are components of the same booking (or sentence
group) will have the same status information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.persistence.entity.state.entities import (
    STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH
-- This CTE pulls only the necessary raw data fields from the elite_offenderbookingstable
-- and joins in SENTENCE_SEQ from elite_offendersentences in order to attach these statuses
-- to every component sentence of a booking.
-- The ACTIVE_FLAG field is the only status information available on these bookings, and 
-- denotes whether or not an incarceration sentence is currently active. This CTE
-- stores the previous value of that field in each row so we can track when it changes.
all_entries AS (
  SELECT DISTINCT 
    bookings.OFFENDER_BOOK_ID,
    SENTENCE_SEQ,
    ACTIVE_FLAG AS status,
    LAG(ACTIVE_FLAG) OVER (PARTITION BY bookings.OFFENDER_BOOK_ID, SENTENCE_SEQ ORDER BY COALESCE(bookings.MODIFY_DATETIME,bookings.CREATE_DATETIME)) AS prev_status,
    LEAD(ACTIVE_FLAG) OVER (PARTITION BY bookings.OFFENDER_BOOK_ID, SENTENCE_SEQ ORDER BY COALESCE(bookings.MODIFY_DATETIME,bookings.CREATE_DATETIME)) AS next_status,
    COALESCE(bookings.MODIFY_DATETIME,bookings.CREATE_DATETIME) AS STATUS_UPDATE_DATETIME
  FROM
    {{elite_offenderbookingstable@ALL}} bookings
-- Inner join these two tables because if a booking does not lead to a sentence, we do not
-- ingest it.
  JOIN {{elite_offendersentences}} sentences
  ON(REPLACE(REPLACE(sentences.OFFENDER_BOOK_ID,',',''), '.00', '') = bookings.OFFENDER_BOOK_ID)
-- Inner join these two tables because if a sentence does not have any charges, we do not
-- ingest it.
  JOIN {{elite_offenderchargestable}} charges
  ON(REPLACE(REPLACE(sentences.OFFENDER_BOOK_ID,',',''), '.00', '') = REPLACE(REPLACE(charges.OFFENDER_BOOK_ID,',',''), '.00', '')
  AND sentences.CHARGE_SEQ = charges.CHARGE_SEQ)
-- Left join this table because we left join it in the sentence view, and we only use it 
-- to avoid ingesting status snapshots for sentences we do not ingest.
  LEFT JOIN {{elite_orderstable}} orders
  ON(REPLACE(REPLACE(charges.OFFENDER_BOOK_ID,',',''), '.00', '') = REPLACE(REPLACE(orders.OFFENDER_BOOK_ID,',',''), '.00', '')
  AND charges.ORDER_ID = orders.ORDER_ID)
  -- Filter out 6 rows where CONVICTION_DATE values are not within a reasonable range.
  WHERE CAST(orders.CONVICTION_DATE AS DATETIME) BETWEEN '{STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND}' AND @update_timestamp
),
-- Preserve only updates that signify the start or end of a sentence, or a change in 
-- the status of the sentence. This removes all rows that do not contain a substantive
-- change in status from the results.
changed_statuses_only AS (
SELECT DISTINCT
    *
FROM
  all_entries
WHERE
  -- The first status for this booking
  (prev_status IS NULL AND status IS NOT NULL)
  -- Any change in status for the booking
  OR (status != prev_status)
  -- The final status for the booking
  OR (prev_status IS NOT NULL AND status IS NULL)
)

SELECT
  OFFENDER_BOOK_ID,
  SENTENCE_SEQ,
  STATUS_UPDATE_DATETIME,
  CASE  
    -- When someone escapes from incarceration, the value of this field becomes 'N' until
    -- they return. This is the only scenario when an 'N' is followed by a 'Y', so we denote
    -- it specifically and map the status to SUSPENDED.
      WHEN status = 'N' and next_status = 'Y' THEN 'ESCAPE'
      WHEN status = 'N' THEN 'INACTIVE'
      WHEN status = 'Y' THEN 'ACTIVE'
      ELSE status
    END AS status_processed
FROM (
  SELECT 
    OFFENDER_BOOK_ID,
    SENTENCE_SEQ,
    STATUS_UPDATE_DATETIME,
    status,
    -- We need to reindex the previous and subsequent statuses now that we have removed
    -- all static updates from the results.
    LAG(status) OVER (PARTITION BY OFFENDER_BOOK_ID, SENTENCE_SEQ ORDER BY STATUS_UPDATE_DATETIME) AS prev_status,
    LEAD(status) OVER (PARTITION BY OFFENDER_BOOK_ID, SENTENCE_SEQ ORDER BY STATUS_UPDATE_DATETIME) AS next_status
  FROM changed_statuses_only
)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="elite_sentence_status_snapshot",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
