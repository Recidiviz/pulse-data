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
"""Query containing person information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
-- getting only most recent inmateNumber's to avoid address/phone/email overlaps
-- every person gets a new inmateNumber when if are re-incarcerated post release
current_ids as (
  SELECT 
    inmateNumber  
  FROM {InmatePreviousId} 
  QUALIFY ROW_NUMBER() OVER (PARTITION BY internalID ORDER BY receivedDate DESC) = 1
),
--collecting relevant address/phone/email information
address_info AS (
  SELECT 
    ci.inmateNumber, 
    DATE(startDate) AS startDate,
    DATE(endDate) AS endDate,
    residencePhoneNumber,
    alternatePhoneNumber,
    emailAddress
  FROM current_ids ci
  LEFT JOIN {PIMSResidence} r
    ON ci.inmateNumber = r.inmateNumber
  QUALIFY ROW_NUMBER() OVER(PARTITION BY ci.inmateNumber ORDER BY startDate DESC, endDate DESC nulls first) = 1
)
SELECT 
    i.inmateNumber,
    lastName,
    firstName,
    DATE(NULLIF(dob, 'NULL')) AS dob,
    GENDER_CD,
    RACE_CD,
    residencePhoneNumber,
    emailAddress
FROM 
    {InmatePreviousId} i
LEFT JOIN 
    {CTS_Inmate} c
ON 
    i.inmateNumber = c.ID_NBR
LEFT JOIN address_info ai
ON 
    i.inmateNumber = ai.inmateNumber
QUALIFY ROW_NUMBER() OVER(PARTITION BY internalId ORDER BY i.receivedDate DESC) = 1
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ne",
    ingest_view_name="state_person",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
