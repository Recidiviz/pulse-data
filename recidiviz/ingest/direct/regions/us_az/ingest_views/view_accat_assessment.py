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
""" Query for ACCAT assessment data. This is an assessment performed only on supervision
in Arizona.
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- Get the most recent SMI designation for each person.
smi_status AS (
SELECT 
  PERSON_ID,
  SMI
FROM 
  {AZ_INT_MENTAL_HEALTH_ACTION} mh
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY PERSON_ID 
  ORDER BY CAST(CREATE_DTM AS TIMESTAMP) DESC, CAST(UPDT_DTM AS TIMESTAMP) DESC
  ) = 1
)
SELECT DISTINCT
  accat.ACCAT_ID,
  accat.DATE_ASSESSMENT,
  ep.PERSON_ID,
  accat.TOTAL AS TOTAL_SCORE,
  level.DESCRIPTION AS FINAL_LEVEL,
  smi_status.SMI,
  /*
  The external ID '2' is used to denote records "created" during the system migration. 
  These were initially created by a real user, but their ID was overwritten during the migration
  and cannot be recovered. As of 10/18/2024 these rows account for 41% of all assessments. 
  We do not expect to see that number increase over time. 
  */
  NULLIF(accat.CREATE_USERID, '2') AS CONDUCTING_STAFF_EXTERNAL_ID
FROM
  {AZ_CS_OMS_ACCAT} accat
JOIN
  {DPP_EPISODE} ep
USING
  (DPP_ID)
JOIN
  {LOOKUPS} level
ON
  (LEVEL_ID = LOOKUP_ID)
LEFT JOIN 
  smi_status 
USING 
  (PERSON_ID)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="accat_assessment",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
