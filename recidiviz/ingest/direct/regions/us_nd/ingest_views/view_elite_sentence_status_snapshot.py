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

from recidiviz.common.constants.reasonable_dates import (
    STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
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
    bookings.BOOKING_BEGIN_DATE,
    bookings.BOOKING_END_DATE,
    sentences.EFFECTIVE_DATE,
    sentences.START_DATE,
    sentences.SENTENCE_EXPIRY_DATE,
    ACTIVE_FLAG AS status,
    LAG(ACTIVE_FLAG) OVER (PARTITION BY bookings.OFFENDER_BOOK_ID, SENTENCE_SEQ ORDER BY COALESCE(bookings.MODIFY_DATETIME,bookings.CREATE_DATETIME)) AS prev_status,
    COALESCE(bookings.MODIFY_DATETIME,bookings.CREATE_DATETIME) AS STATUS_UPDATE_DATETIME,
    bookings.update_datetime,
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
-- Pull out the initial sentence serving start date from the `all_entries` CTE
sentence_serving_starts AS (
  SELECT DISTINCT
    OFFENDER_BOOK_ID,
    SENTENCE_SEQ,
    COALESCE(START_DATE, EFFECTIVE_DATE) AS STATUS_UPDATE_DATETIME,
    "ACTIVE" AS status_processed,
  FROM all_entries
  WHERE
      -- Drop sentences that have been imposed but not yet started, this can happen for a few reasons:
      -- sentence start/effective date is set in the near future
      -- sentence is consecutive and the parent sentence (often a life sentence) has not been completed yet
      CAST(COALESCE(START_DATE, EFFECTIVE_DATE) AS DATETIME) < update_datetime
      -- Drop sentence status starts for sentences that were start/completed on the same day or
      -- are marked as starting after the booking end date, only ingest the COMPLETED status for those sentences
      AND CAST(COALESCE(START_DATE, EFFECTIVE_DATE) AS DATETIME) < LEAST(CAST(IFNULL(SENTENCE_EXPIRY_DATE, "9999-12-31") AS DATETIME), CAST(IFNULL(BOOKING_END_DATE, "9999-12-31") AS DATETIME))
),
-- Pull out the final sentence completion date from the `all_entries` CTE
sentence_serving_ends AS (
  SELECT
    OFFENDER_BOOK_ID,
    SENTENCE_SEQ,
    -- Use the SENTENCE_EXPIRY_DATE as the sentence completion date when it comes before the
    -- BOOKING_END_DATE to account for concurrent sentence sequences with shorter sentence lengths
    CAST(LEAST(CAST(IFNULL(SENTENCE_EXPIRY_DATE, "9999-12-31") AS DATETIME), CAST(IFNULL(BOOKING_END_DATE, "9999-12-31") AS DATETIME)) AS STRING) AS STATUS_UPDATE_DATETIME,
    "INACTIVE" AS status_processed,
  FROM all_entries
  WHERE LEAST(CAST(IFNULL(SENTENCE_EXPIRY_DATE, "9999-12-31") AS DATETIME), CAST(IFNULL(BOOKING_END_DATE, "9999-12-31") AS DATETIME)) < update_datetime
  -- Pick the most recently ingested row per sentence for the sentence completion date
  QUALIFY ROW_NUMBER() OVER (PARTITION BY OFFENDER_BOOK_ID, SENTENCE_SEQ ORDER BY update_datetime DESC) = 1
),
-- Pull out the any escape start statuses from the `all_entries` CTE
escape_starts AS (
  SELECT DISTINCT
    OFFENDER_BOOK_ID,
    SENTENCE_SEQ,
    escape_start.STATUS_UPDATE_DATETIME,
    "ESCAPE" AS status_processed,
  FROM all_entries escape_start
  LEFT JOIN sentence_serving_ends
  USING (OFFENDER_BOOK_ID, SENTENCE_SEQ)
  WHERE status = 'N' AND prev_status = 'Y'
  -- Do not include escapes that occurred after this sentence sequence was already completed, which can occur when a
  -- subset of consecutive sentences have finished/expired, but at least 1 is still active
  AND CAST(escape_start.STATUS_UPDATE_DATETIME AS DATETIME) < CAST(IFNULL(sentence_serving_ends.STATUS_UPDATE_DATETIME, "9999-12-31") AS DATETIME)
),
-- Pull out the any escape return statuses from the `all_entries` CTE
escape_ends AS (
  SELECT DISTINCT
    OFFENDER_BOOK_ID,
    SENTENCE_SEQ,
    escape_return.STATUS_UPDATE_DATETIME,
    "ACTIVE" AS status_processed,
  FROM all_entries escape_return
  LEFT JOIN sentence_serving_ends
  USING (OFFENDER_BOOK_ID, SENTENCE_SEQ)
  WHERE status = 'Y' AND prev_status = 'N'
  -- Do not include escapes that occurred after this sentence sequence was already completed, which can occur when a
  -- subset of consecutive sentences have finished/expired, but at least 1 is still active
  AND CAST(escape_return.STATUS_UPDATE_DATETIME AS DATETIME) < CAST(IFNULL(sentence_serving_ends.STATUS_UPDATE_DATETIME, "9999-12-31") AS DATETIME)
)
SELECT * FROM sentence_serving_starts
UNION ALL
SELECT * FROM sentence_serving_ends
UNION ALL
SELECT * FROM escape_starts
UNION ALL
SELECT * FROM escape_ends
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="elite_sentence_status_snapshot",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
