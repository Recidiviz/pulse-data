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
"""Query that collects information about sentence lengths in UT.

TODO(#37588): This view is based on the following assumptions, which need to be confirmed with the 
state:
- One case can have multiple associated offenses
- Each offense can have its own associated sentence
- Sentences can involve three components: prison time, probation time, and jail time
- The maximum sentence length can be found by adding the maximum possible duration of all of those components together

It is likely that this logic will need to be revisited once we have clarity on the
sentencing and related data storage practices in Utah. 
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- Preprocess date fields from raw data for easier use in later CTEs.
cleaned_dates AS (
SELECT DISTINCT
  crt.ofndr_num,
  ofnse.ofnse_id,
  NULLIF(CAST(CAST(crt.sent_jail_day AS FLOAT64) AS INT64), 0) as jail_days,
  NULLIF(CAST(CAST(prob.prob_sent_days AS FLOAT64) AS INT64), 0) AS probation_days,
  NULLIF(CAST(CAST(prsn.max_days AS FLOAT64) AS INT64), 0) as max_prison_days,
  CAST(crt.updt_dt as DATETIME) AS jail_update_datetime,
  CAST(prob.updt_dt AS DATETIME) as probation_update_datetime,
  CAST(prsn.updt_dt AS DATETIME) AS prison_update_datetime
FROM 
  {crt_case@ALL} crt
JOIN 
  {rfrd_ofnse} ofnse
USING 
  (intr_case_num)
LEFT JOIN 
  {prob_sent@ALL} prob
USING 
  (intr_case_num)
-- Inner join so that we do not ingest sentence lengths for sentences with no charges. 
JOIN 
  {prsn_sent_len@ALL} prsn
USING 
  (ofnse_id)
-- Exclude sentences with no imposed date. These are all in the past, and make up 5% of closed cases.
WHERE sent_dt IS NOT NULL
),
-- collect date information for each sentence
dates AS (
SELECT 
  ofndr_num,
  ofnse_id,
  jail_days,
  probation_days,
  max_prison_days,
  (IFNULL(jail_days, 0) + IFNULL(probation_days, 0) + IFNULL(max_prison_days, 0)) AS total_days,
  jail_update_datetime,
  probation_update_datetime,
  prison_update_datetime
FROM cleaned_dates
-- There are two rows with a probation_days value of -1. Exclude them since it is unclear what that means
-- and they are both >20 years old and closed.
WHERE (probation_days IS NULL OR probation_days >= 0)
),
-- This maintains only those rows where one of the sentence length components changed. 
  -- Only preserve rows where one of the sentence length variables changed. Changes
  -- preserved by this logic include:
    -- The first value
    -- Any value that is different from the previous value of the same field
    -- The final value
changed_dates_only AS (
  SELECT DISTINCT
    * EXCEPT(prev_jail_days, prev_probation_days, prev_max_prison_days, jail_update_datetime, probation_update_datetime, prison_update_datetime),
    CASE
      WHEN ((prev_jail_days IS NULL AND jail_days IS NOT NULL) OR (jail_days != prev_jail_days) OR (prev_jail_days IS NOT NULL AND jail_days IS NULL)) THEN jail_update_datetime
      WHEN ((prev_probation_days IS NULL AND probation_days IS NOT NULL) OR (probation_days != prev_probation_days) OR (prev_probation_days IS NOT NULL AND probation_days IS NULL)) THEN probation_update_datetime
      WHEN((prev_max_prison_days IS NULL AND max_prison_days IS NOT NULL) OR (max_prison_days != prev_max_prison_days) OR (prev_max_prison_days IS NOT NULL AND max_prison_days IS NULL)) THEN prison_update_datetime
    END AS relevant_update_datetime
  FROM (
    SELECT DISTINCT
      *, 
      LAG(jail_days) OVER (PARTITION BY ofnse_id ORDER BY jail_update_datetime) AS prev_jail_days,
      LAG(probation_days) OVER (PARTITION BY ofnse_id ORDER BY probation_update_datetime) AS prev_probation_days,
      LAG(max_prison_days) OVER (PARTITION BY ofnse_id ORDER BY prison_update_datetime) AS prev_max_prison_days,
    FROM dates
  )
  WHERE 
  -- Rows where the number of jail days changes.
    ((prev_jail_days IS NULL AND jail_days IS NOT NULL)
      OR (jail_days != prev_jail_days)
      OR (prev_jail_days IS NOT NULL AND jail_days IS NULL))
    OR 
  -- Rows where the number of probation days changes.
    ((prev_probation_days IS NULL AND probation_days IS NOT NULL)
      OR (probation_days != prev_probation_days)
      OR (prev_probation_days IS NOT NULL AND probation_days IS NULL))
    OR 
  -- Rows where the number of prison days changes.
    ((prev_max_prison_days IS NULL AND max_prison_days IS NOT NULL)
      OR (max_prison_days != prev_max_prison_days)
      OR (prev_max_prison_days IS NOT NULL AND max_prison_days IS NULL))
)

SELECT 
  *,
  -- Create a sequence number for sentence lengths. If two rows have the same update_datetime 
  -- and distinct duration values, assume the longer one is the most recent.
  ROW_NUMBER() OVER (PARTITION BY OFNSE_ID ORDER BY relevant_update_datetime, total_days) AS sequence_num
FROM changed_dates_only
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ut",
    ingest_view_name="sentence_length",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
