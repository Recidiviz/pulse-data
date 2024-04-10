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
"""Query containing charge information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH commitments AS (
  SELECT DISTINCT 
    episode.PERSON_ID,
    commitment.COUNTY_ID,
    NULLIF(commitment.JUDGE_ID,'NULL') AS JUDGE_ID,
    episode.COURT_FIRST_NAME, 
    episode.COURT_MIDDLE_NAME, 
    episode.COURT_LAST_NAME, 
    NULLIF(offense.ARS_ID,'NULL') AS ARS_ID,
    CAST(NULLIF(offense.OFFENSE_DTM, 'NULL') AS DATETIME) AS OFFENSE_DTM,
    NULLIF(offense.SUBSECTION_CODE,'NULL') AS SUBSECTION_CODE,
    offense.OFFENSE_NUMBER,
    offense.OFFENSE_ID,
    (offense.OFFENSE_ID = sc_episode.FINAL_OFFENSE_ID) AS CONTROLLING_OFFENSE_FLAG
  FROM {AZ_DOC_SC_COMMITMENT} commitment
  JOIN {AZ_DOC_SC_OFFENSE} offense
  USING(COMMITMENT_ID)
  LEFT JOIN {AZ_DOC_SC_EPISODE} sc_episode 
  USING(SC_EPISODE_ID)
  LEFT JOIN {DOC_EPISODE} episode
  USING(DOC_ID)
),
charges AS (
  SELECT 
    commitments.*,
    COUNTY_LU.DESCRIPTION AS COUNTY,
    ars.NCIC_CODE,
    ars.ARS_CODE,
    ars.DESCRIPTION,
    ars.MURDER_DEGREE,
    ars.VIOLENCE_FLAG,
    ars.SEX_OFFENSE_FLAG,
    CASE 
      WHEN ars.TRNSTN_PGM_RLS_INELIG_FLAG IS NULL 
      THEN CAST(NULL AS STRING)
      WHEN ars.TRNSTN_PGM_RLS_INELIG_FLAG='N' THEN 'INELIGIBLE FOR TPR'
      WHEN ars.TRNSTN_PGM_RLS_INELIG_FLAG='Y' THEN 'ELIGIBLE FOR TPR'
    END AS TRNSTN_PGM_RLS_INELIG_FLAG
  FROM commitments
  LEFT JOIN {AZ_DOC_SC_ARS_CODE} ars
  USING(ARS_ID)
  LEFT JOIN {LOOKUPS} COUNTY_LU
  ON(COUNTY_ID = COUNTY_LU.LOOKUP_ID)
)

SELECT * FROM charges
-- There are a small number of SC_EPISODE_IDs that have populated DOC_ID values in AZ_DOC_SC_EPISODE,
-- but no corresponding row for that DOC_ID in DOC_EPISODE. This leads to a small number
-- of NULL PERSON_ID values in these results that need to be excluded.
-- TODO(#28796): Find out in what circumstances this would be the case.
WHERE PERSON_ID IS NOT NULL
-- There are a small number of rows in AZ_DOC_SC_OFFENSE that do not have a populated ARS_ID field.
AND ARS_ID IS NOT NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="state_charge",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
