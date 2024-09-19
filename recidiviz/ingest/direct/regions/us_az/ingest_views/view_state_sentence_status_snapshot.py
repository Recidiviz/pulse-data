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
"""A historical snapshot for when a given sentence had a given status in AZ.

This view treats historical sentences differently than current sentences. This is 
because when Arizona completed a system migration in late 2019, the CREATE_DTM
and UPDT_DTM values on many tables were overwritten to reflect the date of the migration
rather than the date the row was created in the old system.

As a proxy for update datetimes for historical sentence statuses, we use sentence start
and end datetimes that are documented separately. For sentences that began after the 
system migration, we can rely on the CREATE_DTM and UPDT_DTM fields.

ADCRR staff, or the system itself, also seems to update offense information indefinitely
for years after the associated sentence has ended. The sentence status is not what is 
getting updated in these changes, so this view discounts any "updates" that are made
after the end of a sentence."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
-- This CTE pulls the most recently-updated sentence begin and end dates associated with 
-- the sentence. We use this to assess if the sentence has been completed or is currently being served,
-- and to update the imposed date when it has been overwritten by the ACIS migration.
sentence_critical_dates AS (
  SELECT
    OFFENSE_ID,
    CAST(COALESCE(NULLIF(SENTENCE_END_DTM_ML, 'NULL'), NULLIF(SENTENCE_END_DTM, 'NULL')) AS DATETIME) AS sentence_end_dtm,
    CAST(COALESCE(NULLIF(SENTENCE_BEGIN_DTM_ML, 'NULL'), NULLIF(SENTENCE_BEGIN_DTM, 'NULL')) AS DATETIME) AS sentence_begin_dtm
  FROM {AZ_DOC_SC_OFFENSE}
),
-- Get the key identifiers for each person & sentence, including statuses over time and
-- all relevant dates.
sentences_base as (
  SELECT 
    sc_off.OFFENSE_ID, 
    sc_off.COMMITMENT_ID, 
    sc_ep.DOC_ID, 
    doc_ep.PERSON_ID,
    status.description AS status, 
    CAST(COALESCE(NULLIF(sc_off.UPDT_DTM, 'NULL'), NULLIF(sc_off.CREATE_DTM, 'NULL')) AS DATETIME) AS updt_dtm,
    sentence_critical_dates.SENTENCE_BEGIN_DTM,
    sentence_critical_dates.SENTENCE_END_DTM
FROM {AZ_DOC_SC_OFFENSE@ALL} sc_off
-- It's okay to use the _latest view for these raw data tables because I'm only using 
-- them to access fields that do not change over time
JOIN {AZ_DOC_SC_COMMITMENT} sc_comm
USING(COMMITMENT_ID)
JOIN {AZ_DOC_SC_EPISODE} sc_ep
USING(SC_EPISODE_ID)
JOIN {DOC_EPISODE} doc_ep
USING(DOC_ID)
-- LOOKUP values for the descriptions also do not change over time
JOIN {LOOKUPS}  status
ON(sc_off.SENTENCE_STATUS_ID = status.LOOKUP_ID)
JOIN sentence_critical_dates
USING(OFFENSE_ID)
-- TODO(#33341): Figure out how to incorporate vacated sentences.
WHERE UPPER(status.description) NOT LIKE "%VACATE%"
),
-- When a sentence started and ended prior to the ACIS system migration, rows associated
-- with it all have UPDT_DTM and CREATE_DTM values no earlier than 2019-11-30. Those
-- fields are unreliable for pre-migration sentences, so this CTE updates the updt_dtm
-- values for sentence impositions to be the same as the date the sentence began.
-- It does not change anything about sentences that have an end date after 2019-11-30.
update_imposed_dates_for_past_sentences AS (
    SELECT 
        * EXCEPT (updt_dtm),
        CASE WHEN 
            SENTENCE_END_DTM < CAST('2019-11-30' AS DATE)
            AND status = 'Imposed'
            -- There are a handful of entries with an end date but no begin date.
            -- TODO(#33487): Confirm that these rows are still valid data, and find out
            -- why they are missing sentence_begin_dtm values.
            THEN COALESCE(sentence_begin_dtm, sentence_end_dtm)
            ELSE updt_dtm
        END AS updt_dtm
    FROM sentences_base
),
-- Since there are no standard sentence completion statuses included in this data, we 
-- add a row with a completed status for every sentence that has a populated SENTENCE_END_DTM
-- that is in the past. The updt_dtm of the status is the SENTENCE_END_DTM.
add_completed_status AS (
  SELECT 
    *, 
    LAG(status) OVER (PARTITION BY OFFENSE_ID ORDER BY updt_dtm) AS prev_status 
  FROM (
    SELECT DISTINCT
      * EXCEPT (STATUS, updt_dtm),
      -- Add a row with a "Completed" status
      CASE 
        WHEN UPPER(STATUS) = 'IMPOSED' 
          AND sentence_end_dtm < current_datetime() 
          -- We only want to update the most recent status when a case is closed
          -- to avoid prematurely marking it as completed.
          AND LEAD(STATUS) OVER (PARTITION BY OFFENSE_ID ORDER BY updt_dtm) IS NULL
        THEN 'Recidiviz Marked Completed'
        ELSE status
      END AS status,
      -- Use the SENTENCE_END_DTM as the update datetime for the created "Completed" status
        CASE 
        WHEN UPPER(STATUS) = 'IMPOSED' 
          AND sentence_end_dtm < current_datetime() 
          -- We only want to update the most recent status when a case is closed
          -- to avoid prematurely marking it as completed.
          AND LEAD(STATUS) OVER (PARTITION BY OFFENSE_ID ORDER BY updt_dtm) IS NULL
        THEN sentence_end_dtm
        ELSE updt_dtm
      END AS updt_dtm
    FROM update_imposed_dates_for_past_sentences
  )
), 
-- For some reason, there are regularly updates made to offense information for sentences
-- that ended years ago. These updates create new rows suggesting the sentence was just 
-- imposed each time they are created. This CTE protects this view from producing a new "Imposed"
-- row on sentences that have already been marked as complete whenever the UPDT_DTM in
-- raw data has a more recent value.
ignore_trailing_updates AS (
    SELECT * FROM add_completed_status
    WHERE (prev_status != 'Completed' OR prev_status IS NULL)
    AND (updt_dtm <= SENTENCE_END_DTM OR SENTENCE_END_DTM IS NULL)
)
SELECT DISTINCT
  *,
FROM ignore_trailing_updates
--  Only keep rows where the status of a particular OFFENSE_ID changed.
WHERE (status != prev_status
OR (status IS NULL AND prev_status IS NOT NULL)
OR (prev_status IS NULL AND status IS NOT NULL))
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="state_sentence_status_snapshot",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
