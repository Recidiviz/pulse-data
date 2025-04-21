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
"""
Creates status snapshots for when a given sentence has started and finished in AZ.

This structure of this ingest view is due to a few quirks:

* When Arizona completed a system migration in late 2019, the CREATE_DTM
  and UPDT_DTM values on many tables were overwritten to reflect the date of the migration
  rather than the date the row was created in the old system.

* Some sentences start and end at the same time OR have a sentence end date before
  the sentence start date. This is because sentence end dtm is a calculated field,
  so someone may already have credit for serving their entire sentence.

* ADCRR staff, or the system itself, also seems to update offense information indefinitely
  for years after the associated sentence has ended. The sentence status is not what is 
  getting updated in these changes, so this view discounts any "updates" that are made
  after the end of a sentence.

To account for these quirks and to make this data more simple to maintain, we:

1. Create a status with the SENTENCE_BEGIN_DTM as the critical date.
2. Create a status with the SENTENCE_END_DTM as the critical date if 
   that date has passed.

Product needs are focused on sentence starts and ends, so we do not need to
hydrate intermediate status updates as this time. There seems to not be a
SUSPENDED status to affect downstream projected date calculations.
"""

from recidiviz.common.constants.reasonable_dates import (
    STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
)
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

-- This CTE maps sentences to people and subsets
-- sentences to what we consider valid.
valid_sentences AS ({VALID_PEOPLE_AND_SENTENCES}),
-- This CTE cleans the AZ_DOC_SC_OFFENSE table for use in future CTEs.
AZ_DOC_SC_OFFENSE_cleaned AS (
    SELECT 
        OFFENSE_ID,
        NULLIF(SENTENCE_BEGIN_DTM_ML, 'NULL') AS SENTENCE_BEGIN_DTM_ML,
        NULLIF(SENTENCE_BEGIN_DTM, 'NULL') AS SENTENCE_BEGIN_DTM,
        NULLIF(SENTENCE_END_DTM_ML, 'NULL') AS SENTENCE_END_DTM_ML,
        NULLIF(SENTENCE_END_DTM, 'NULL') AS SENTENCE_END_DTM,
        SENTENCE_STATUS_ID
    FROM {{AZ_DOC_SC_OFFENSE}}
),
-- This CTE creates an initial status for sentences to denote SERVING.
initial_serving_statuses AS (
    SELECT
        PERSON_ID,
        OFFENSE_ID,
        COALESCE(SENTENCE_BEGIN_DTM_ML, SENTENCE_BEGIN_DTM) AS status_update_datetime,
        CASE 
            WHEN DESCRIPTION NOT IN 
            ('Imposed', 'Paroled to Consecutive', 'Paroled')
            THEN 'Recidiviz Marked Started' 
            ELSE DESCRIPTION 
        END AS status_raw_text,
        1 AS sequence_num
    FROM
        valid_sentences
    JOIN
        AZ_DOC_SC_OFFENSE_cleaned
    USING
        (OFFENSE_ID)
    JOIN
        {{LOOKUPS}}
    ON
        SENTENCE_STATUS_ID = LOOKUP_ID
    WHERE
        CAST(COALESCE(SENTENCE_BEGIN_DTM_ML, SENTENCE_BEGIN_DTM) AS DATETIME) BETWEEN '{STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND}' AND @update_timestamp
),
-- This CTE creates statuses for sentences that have terminated.
-- We preserve the completion reason if it was provided, but most
-- sentence statuses provided by AZ are 'Imposed' and overwritten
-- by us for clarity.
completion_statuses AS (
    SELECT
        PERSON_ID,
        OFFENSE_ID,
        -- 173 sentence end dtm values appear prior to the sentence 
        -- begin dtm, sometimes by hundreds of years. 
        -- This is likely due to a migration problem.
        GREATEST(
            COALESCE(SENTENCE_BEGIN_DTM_ML, SENTENCE_BEGIN_DTM), 
            COALESCE(SENTENCE_END_DTM_ML, SENTENCE_END_DTM)
            ) AS status_update_datetime,
        CASE
            WHEN DESCRIPTION IN (
                'Completed', 
                'Early Termination',
                'Set-Aside',
                'Expungement'
            )
            THEN DESCRIPTION ELSE 'Recidiviz Marked Completed' 
        END AS status_raw_text,
        2 AS sequence_num
    FROM
        valid_sentences
    JOIN
        AZ_DOC_SC_OFFENSE_cleaned
    USING
        (OFFENSE_ID)
    JOIN
        {{LOOKUPS}}
    ON
        SENTENCE_STATUS_ID = LOOKUP_ID
    WHERE
        COALESCE(SENTENCE_END_DTM_ML, SENTENCE_END_DTM) IS NOT NULL
    AND
        CAST(COALESCE(SENTENCE_END_DTM_ML, SENTENCE_END_DTM) AS DATETIME) BETWEEN '{STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND}' AND @update_timestamp
    AND 
        CAST(COALESCE(SENTENCE_BEGIN_DTM_ML, SENTENCE_BEGIN_DTM) AS DATETIME) BETWEEN '{STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND}' AND @update_timestamp
)
SELECT * FROM initial_serving_statuses
UNION ALL
SELECT * FROM completion_statuses
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="state_sentence_status_snapshot",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
