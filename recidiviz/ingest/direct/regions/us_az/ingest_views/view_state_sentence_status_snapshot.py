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
  SELECT DISTINCT
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
-- Since the UPDT_DTM and CREATE_DTM values were mostly all overwritten by the system migration 
-- date 2019-11-30, we have to handle sentences that began before the migration differently.
-- We use the SENTENCE_BEGIN_DTM and SENTENCE_END_DTM fields to anchor status updates
-- in time rather than the UPDT_DTM or CREATE_DTM values of rows in the raw data.
-- This CTE does not create "Recidiviz Marked Completed" statuses for sentences that
-- began pre-migration but have not yet ended.
pre_migration_sentences AS (
SELECT DISTINCT
  PERSON_ID, 
  DOC_ID,
  COMMITMENT_ID,
  OFFENSE_ID,
  sentence_begin_dtm AS status_update_datetime,
  'Recidiviz Marked Started' AS status_raw_text
FROM
  sentences_base
WHERE
  -- sentence started pre-migration, so does not have a useful value in AZ_DOC_SC_OFFENSE.UPDT_DTM
  sentence_begin_dtm < CAST('2019-11-30' AS DATE)

UNION ALL

SELECT DISTINCT
  PERSON_ID, 
  DOC_ID,
  COMMITMENT_ID,
  OFFENSE_ID,
  CASE
    WHEN sentence_end_dtm = sentence_begin_dtm 
    --  173 sentence end dtm values appear prior to the sentence begin dtm, sometimes by hundreds of years. This is likely due to a migration problem.
    OR sentence_end_dtm < sentence_begin_dtm
    -- There are sometimes multiple identical updates made to a single sentence, so choose
    -- the most recent update datetime to use as the closure date. 
    THEN MAX(updt_dtm) OVER (PARTITION BY OFFENSE_ID, COMMITMENT_ID, DOC_ID)
    ELSE sentence_end_dtm
  END AS status_update_datetime,
  'Recidiviz Marked Completed' AS status_raw_text
FROM
  sentences_base
WHERE
  sentence_begin_dtm < CAST('2019-11-30' AS DATE)
AND
  -- sentence is completed
  sentence_end_dtm < current_datetime()
)

SELECT * FROM pre_migration_sentences
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="state_sentence_status_snapshot",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
