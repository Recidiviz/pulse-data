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
"""Query containing sentence information from the Docstars system."""

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
-- This CTE joins basic information about each charge and sentence. This will be used
-- to populate static fields on the state_sentence, state_sentence_group, and state_sentence
-- entities.
charge_and_sentence_base AS (
SELECT DISTINCT 
  ot.SID,
  ot.CASE_NUMBER,
  ot.RecID,
  -- If a case was sentenced out of state, we do not know the cardinality of court 
  -- cases to charges, and we cannot guarantee that it aligns with that of ND, which this
  -- view is build to accommodate. This is a component of some entity external IDs, but 
  -- is never used as a unique identifier, so these sentences will still be uniquely
  -- identifiable. They will also still end up in the appropriate inferred sentence groups
  -- by virtue of sharing charges and offense dates.
  IF((ot.COUNTY IS NOT NULL AND CAST(ot.COUNTY AS INT64) > 100), NULL, ot.COURT_NUMBER) AS COURT_NUMBER,
  -- Tom told us to use the CST NCIC code if there is one, and otherwise to use the CODE value.
  -- Only one of these is ever hydrated.
  COALESCE(ot.COMMON_STATUTE_NCIC_CODE,ot.CODE) AS OFFENSE_CODE,
  XREF.DESCRIPTION AS CHARGE_DESCRIPTION,
  ot.LEVEL,
  ot.COUNTY AS CHARGE_COUNTY,
  CASE 
    WHEN CAST(ot.COUNTY AS INT64) < 100 THEN 'STATE'
    WHEN ot.COUNTY = 'FD' THEN 'FEDERAL'
    WHEN  CAST(ot.COUNTY AS INT64) IN (154, 155) THEN 'INTERNATIONAL/ERROR'
    WHEN ot.COUNTY IS NULL THEN 'NO INFO'
    ELSE 'OTHER_STATE'
  END AS SENTENCING_AUTHORITY,
  ot.COUNT,
  ot.REQUIRES_REGISTRATION,  
  ot.MASTER_OFFENSE_IND,
  -- NULL out OFFENSEDATE values that are not within a reasonable range.
  IF(CAST(ot.OFFENSEDATE AS DATETIME) BETWEEN '{STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND}' AND @update_timestamp, 
    CAST(ot.OFFENSEDATE AS DATETIME), 
    CAST(NULL AS DATETIME)) AS OFFENSEDATE,
  oc.JUDGE,
  oc.TB_CTY AS SENTENCING_COUNTY,
  oc.PAROLE_FR,
  oc.DESCRIPTION AS CASE_DESCRIPTION,
FROM
  {{docstars_offensestable}} ot
-- Only ingest entities that have both sentence and charge information. This is all but ~400 rows in docstars_offensestable.
JOIN {{docstars_offendercasestable}} oc
USING(CASE_NUMBER)
LEFT JOIN
  {{recidiviz_docstars_cst_ncic_code}} xref
ON
  (COALESCE(ot.Common_Statute_NCIC_Code, ot.CODE) = xref.CODE)  
  -- We do not have any charges ingested with a CONVICTED status for sentences that are 
  -- still Pre-Trial, so we do not want to include them here.
  -- For information about pre-trial cases, see the docstars_psi raw data table.
WHERE oc.DESCRIPTION != 'Pre-Trial'
-- Filter out parole sentences that have not yet started
AND CAST(oc.PAROLE_FR AS DATETIME) BETWEEN '{STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND}' AND @update_timestamp
),
-- This CTE collects all updates made over time to the dates associated with each 
-- sentence. This information will be used to hydrate the state_sentence_length entity.
sentence_dates AS (
SELECT DISTINCT 
  CASE_NUMBER,
  PAROLE_TO, 
  TERM_DATE,
  SENT_YY,
  SENT_MM,
  RecDate
FROM {{docstars_offendercasestable@ALL}} oc
)

SELECT DISTINCT *
FROM charge_and_sentence_base
JOIN sentence_dates
USING(CASE_NUMBER)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="docstars_sentence",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
