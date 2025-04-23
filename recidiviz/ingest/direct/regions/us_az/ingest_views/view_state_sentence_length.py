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
"""Query for state sentence lengths.

For each of the date fields in the entity, we coalesce three fields from 
the source raw data. This is to accomodate the way automatic and manual overrides are
documented in ACIS. The system prioritizes Manual Lock fields first, because these
contain dates that were manually entered by an officer to override the automatically calculated
date. Then, Adjust Release Date fields are prioritized. These are automatic adjustments
made by ACIS to release dates that fall on weekends or holidays. The final field is the
original release date calculated by ACIS based on sentence information. 
"""
from recidiviz.ingest.direct.regions.us_az.ingest_views.common_sentencing_views_and_utils import (
    VALID_PEOPLE_AND_SENTENCES,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH 
-- Maps epiodes to people with valid sentences
valid_people AS ({VALID_PEOPLE_AND_SENTENCES}),
-- The result of this CTE is one row for each combination of sentence and set of
-- release dates for all people with valid sentences.
-- Whenever one of the release dates changes, a row will be added
-- with the updated information about the sentence and the date on which that information
-- changed.
base AS (
SELECT DISTINCT
    off.OFFENSE_ID,
    valid_people.PERSON_ID,
    CAST(NULLIF(off.UPDT_DTM,'NULL') AS DATETIME) AS UPDT_DTM,
    -- See view description for explanation of datetime field hierarchy.
    COALESCE(
        NULLIF(COMMUNITY_SUPV_BEGIN_DTM_ML, 'NULL'),
        NULLIF(COMMUNITY_SUPV_BEGIN_DTM_ARD, 'NULL'), 
        NULLIF(COMMUNITY_SUPV_BEGIN_DTM, 'NULL')) AS CommunitySupervisionBeginDate, -- CSBD
    COALESCE(
        NULLIF(EARNED_RLS_CREDIT_DTM_ML, 'NULL'), 
        NULLIF(EARNED_RLS_CREDIT_DTM_ARD, 'NULL'), 
        NULLIF(EARNED_RLS_CREDIT_DTM, 'NULL')) AS EarnedReleaseCreditDate, -- ERCD
    COALESCE(
        NULLIF(SENTENCE_END_DTM_ML, 'NULL'), 
        NULLIF(SENTENCE_END_DTM, 'NULL')) AS SentenceExpirationDate, -- SED
    COALESCE(
        NULLIF(TR_TO_ABSOLUTE_DSCHRG_DTM_ML, 'NULL'), 
        NULLIF(TR_TO_ABSOLUTE_DSCHRG_DTM_ARD, 'NULL'),
        NULLIF(TR_TO_ABSOLUTE_DSCHRG_DTM, 'NULL')) AS TransitionToAbsoluteDischargeDate, -- TR to ADD
    COALESCE(
        NULLIF(ABSOLUTE_DISCHARGE_DTM_ML, 'NULL'), 
        NULLIF(ABSOLUTE_DISCHARGE_DTM_ARD, 'NULL'),
        NULLIF(ABSOLUTE_DISCHARGE_DTM, 'NULL')) AS AbsoluteDischargeDate, -- Absolute Discharge Date (ADD)
FROM 
    {{AZ_DOC_SC_OFFENSE@ALL}} AS off
JOIN valid_people
    USING(OFFENSE_ID)
),
-- Filter malformed date values out of the query before any more processing is done.
filter_to_valid_dates AS (
SELECT 
    OFFENSE_ID,
    PERSON_ID,
    UPDT_DTM,
    IF(SentenceExpirationDate BETWEEN '1900-01-01' AND '2100-01-01', SentenceExpirationDate, NULL) AS SentenceExpirationDate,
    IF(CommunitySupervisionBeginDate BETWEEN '1900-01-01' AND '2100-01-01', CommunitySupervisionBeginDate, NULL) AS CommunitySupervisionBeginDate,
    IF(EarnedReleaseCreditDate BETWEEN '1900-01-01' AND '2100-01-01', EarnedReleaseCreditDate, NULL) AS EarnedReleaseCreditDate,
    IF(TransitionToAbsoluteDischargeDate BETWEEN '1900-01-01' AND '2100-01-01', TransitionToAbsoluteDischargeDate, NULL) AS TransitionToAbsoluteDischargeDate,
    IF(AbsoluteDischargeDate BETWEEN '1900-01-01' AND '2100-01-01', AbsoluteDischargeDate, NULL) AS AbsoluteDischargeDate
FROM base 
),
-- Collect the previous value of each release date for each sentence, so that we can
-- filter these results to only include rows where a release date changed.
previous_dates AS (
  SELECT 
    *,
    LAG(CommunitySupervisionBeginDate) OVER sentence_window AS prev_CSBD,
    LAG(EarnedReleaseCreditDate) OVER sentence_window AS prev_ERCD,
    LAG(SentenceExpirationDate) OVER sentence_window AS prev_SED,
    LAG(TransitionToAbsoluteDischargeDate) OVER sentence_window AS prev_TRtoADD,
    LAG(AbsoluteDischargeDate) OVER sentence_window AS prev_ADD
  FROM filter_to_valid_dates
  -- There is one offense that has two updates at the exact same time. SED is included
  -- here to sort them deterministically.
  WINDOW sentence_window AS (PARTITION BY OFFENSE_ID ORDER BY UPDT_DTM, SentenceExpirationDate)
)

SELECT 
  OFFENSE_ID,
  PERSON_ID,
  UPDT_DTM,
  CommunitySupervisionBeginDate,
  EarnedReleaseCreditDate,
  SentenceExpirationDate,
  TransitionToAbsoluteDischargeDate,
  AbsoluteDischargeDate
FROM previous_dates
WHERE UPDT_DTM IS NOT NULL
AND 
-- any of the dates changed, appeared, or disappeared
(CommunitySupervisionBeginDate IS DISTINCT FROM prev_CSBD
OR EarnedReleaseCreditDate IS DISTINCT FROM prev_ERCD
OR SentenceExpirationDate IS DISTINCT FROM prev_SED
OR TransitionToAbsoluteDischargeDate IS DISTINCT FROM prev_TRtoADD
OR AbsoluteDischargeDate IS DISTINCT FROM prev_ADD)
"""
VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="state_sentence_length",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
