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
"""Query that collects information about sentences statuses over time in UT.

Sentences in Utah can be suspended upon imposition, meaning that a person is sentenced
to a term of probation that, if they serve successfully, will take the place of their prison sentence.
If they are revoked from that term of probation, they must serve the entirety of their original
prison sentence. These types of sentences are ingested as probation sentences, and will 
appear as probation sentences even if the term of probation is revoked and the person 
is required to face incarceration."""

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

-- Collect all sentence dates to understand when a sentence began being served.
sentence_starts AS (
SELECT DISTINCT
  ofndr_num,
  intr_case_num,
  CAST(sent_dt AS DATETIME) as updt_dt,
  'SERVING' AS status,
FROM
  base_sentences
JOIN
  {{crt_case}} crt
USING
  (ofndr_num,
    intr_case_num)
WHERE
  sent_dt IS NOT NULL 
),

-- Collect all sentence termination dates to understand when a sentence was completed.
sentence_ends AS (
SELECT DISTINCT
  ofndr_num,
  intr_case_num,
  CAST(crt_trmn_dt AS DATETIME) as updt_dt,
  'COMPLETE' AS status,
FROM
  base_sentences
JOIN
  {{crt_case}} crt
USING
  (ofndr_num, intr_case_num)
WHERE crt_trmn_dt IS NOT NULL
),

-- Combine sentencing dates and court termination dates to create a ledger of status updates.
combined AS (
  SELECT * FROM sentence_starts
  UNION ALL 
  SELECT * FROM sentence_ends
)

SELECT
  ofndr_num,
  intr_case_num,
  status,
  updt_dt,
  -- There are 6 sentences with multiple statuses entered into the UT database with the
  -- exact same timestamp. Order these manually so that the SERVING status comes before
  -- the COMPLETE status.
  ROW_NUMBER() OVER (PARTITION BY ofndr_num, intr_case_num ORDER BY updt_dt, IF(status='COMPLETE',1,0)) AS sequence
  FROM (
    SELECT *,
    LAG(status) OVER (PARTITION BY ofndr_num, intr_case_num ORDER BY updt_dt, IF(status='COMPLETE',1,0)) AS prev_status
    FROM combined
  )
WHERE 
  status IS DISTINCT FROM prev_status
AND
-- This is the case in 247 out of 661,445 resulting rows.
-- It does not seem like the norm, and appears to be a data entry error in a majority
-- of cases. Exclude these cases for the time being and revisit once we have
-- more context from UT. TODO(#38008)
  (NOT (status = 'SERVING' AND prev_status = 'COMPLETE') OR prev_status IS NULL)
AND 
  updt_dt BETWEEN '{STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND}' AND @update_timestamp
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ut",
    ingest_view_name="sentence_status",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
