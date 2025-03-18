# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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

Sentences in Utah can be suspended upon imposition, meaning that a person is sentenced
to a term of probation that, if they serve successfully, will take the place of their prison sentence.
If they are revoked from that term of probation, they must serve the entirety of their original
prison sentence. These types of sentences are ingested as probation sentences. 

For the period during which a person is serving their probation, the release dates associated
with their sentences are the dates they may be released from probation. If and when the person
is revoked and required to serve their prison sentence, these dates will change to reflect
the person's projected release dates from prison."""

from recidiviz.ingest.direct.regions.us_ut.ingest_views.common_sentencing_views_and_utils import (
    VALID_PEOPLE_AND_SENTENCES,
)
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
-- Collect identifiers for sentences to ingest.
base_sentences AS ({VALID_PEOPLE_AND_SENTENCES}),
-- Collect identfiiers for probation sentences that have bene revoked. This will allow 
-- us to discern which dates (probation or incarceration) are correct to use as a person's
-- projected release dates.
probation_revocations AS (
SELECT
  DISTINCT intr_case_num,
  cd.lgl_stat_chg_desc,
  cast(case_stat.stat_beg_dt as datetime) as stat_beg_dt
FROM
  {{case_stat}} case_stat
LEFT JOIN
  {{lgl_stat_chg_cd}} cd
USING
  (lgl_stat_chg_cd)
WHERE
  lgl_stat_chg_desc ='PROBATION REVOKED'
  OR lgl_stat_chg_desc LIKE '%PROB VIOLATION%' 
),
-- Collect all dates associated with a case. 
case_dates AS (
SELECT
  intr_case_num,
  CAST(dt.updt_dt AS DATETIME) AS updt_dt,
  CAST(sched_expire_dt AS DATETIME) AS sched_expire_dt,
  CAST(early_trmn_dt AS DATETIME) AS early_trmn_dt,
  CAST(sched_trmn_dt AS DATETIME) AS sched_trmn_dt
FROM
  {{est_dt_hist}} dt

UNION DISTINCT

SELECT
  DISTINCT intr_case_num,
  CAST(dt.updt_dt AS DATETIME) AS updt_dt,
  CAST(sched_expire_dt AS DATETIME) AS sched_expire_dt,
  CAST(early_trmn_dt AS DATETIME) AS early_trmn_dt,
  CAST(sched_trmn_dt AS DATETIME) AS sched_trmn_dt
FROM
  {{est_dt}} dt 
),
-- Connect cases with their corresponding dates and revocation statuses.
-- Only attach probation revocation indicators to probation sentences.
cases_with_dates AS (
SELECT
  sent.*,
  case_dates.updt_dt,
  case_dates.sched_expire_dt,
  case_dates.early_trmn_dt,
  case_dates.sched_trmn_dt,
  rev.lgl_stat_chg_desc,
  rev.stat_beg_dt
FROM
  base_sentences sent
JOIN
  case_dates
USING
  (intr_case_num)
LEFT JOIN
  probation_revocations rev
ON
  (sent.intr_case_num = rev.intr_case_num
    AND sent.sentence_type = 'PROBATION')
),
-- Determine which dates should be associated with a given sentence based on the presence
-- or absence of a probation revocation. Create initial result table.
cases_with_prioritized_dates AS (
SELECT DISTINCT
  ofndr_num,
  intr_case_num,
  max_end_dt,
  early_trmn_dt,
  updt_dt,  
  ROW_NUMBER() OVER (
    PARTITION BY ofndr_num, intr_case_num, sentence_type 
    -- If two updates were made on the same date, sort them so that the latest date 
    -- appears as the most recent. This is an assumption based on the observation that
    -- dates are rarely moved to be sooner, but commonly moved to farther in the future.
    ORDER BY updt_dt, max_end_dt DESC
    ) AS sequence_num
FROM (
  SELECT 
    ofndr_num,
    intr_case_num,
    sentence_type,
    CASE  
    -- This is a probation sentence that has been revoked, after the date of the revocation. We should use the incarceration dates.
      WHEN stat_beg_dt IS NOT NULL AND updt_dt > stat_beg_dt THEN sched_expire_dt
    -- This is a probation sentence that has been revoked, before the date of the revocation. We should use the probation dates.
      WHEN stat_beg_dt IS NOT NULL AND updt_dt < stat_beg_dt THEN sched_trmn_dt
      WHEN sentence_type = 'PROBATION' AND stat_beg_dt IS NULL THEN sched_trmn_dt
      WHEN sentence_type = 'INCARCERATION' THEN sched_expire_dt
    END AS max_end_dt,
    early_trmn_dt,
    updt_dt
  FROM
    cases_with_dates
  )
)

SELECT * FROM cases_with_prioritized_dates
  -- There are 5 rows with no hydrated date fields. 
  -- Since they provide us no information and are always followed by meaningful date 
  -- entries, we exclude them.
  WHERE
    -- Filter to only include rows where
    ((max_end_dt BETWEEN '{STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND}' AND '2500-01-01' OR max_end_dt IS NULL)
    AND (early_trmn_dt BETWEEN '{STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND}' AND '2500-01-01' OR early_trmn_dt IS NULL))
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ut",
    ingest_view_name="sentence_length",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
