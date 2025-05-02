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
"""Address information for NE adults on parole."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
-- getting only most recent inmateNumber's to avoid address period overlaps
-- every person gets a new inmateNumber when if are re-incarcerated post release
current_ids as (
  SELECT 
    inmateNumber  
  FROM {InmatePreviousId} 
  QUALIFY ROW_NUMBER() OVER (PARTITION BY internalID ORDER BY receivedDate DESC) = 1
),
--collecting relevant address information
address_info AS (
  SELECT 
    ci.inmateNumber, 
    residenceContactId,
    addressLine1, 
    addressLine2, 
    UPPER(city) AS city, 
    codevalue AS state, 
    zipCode, 
    DATE(startDate) AS startDate,
    IF(DATE(endDate) < DATE(startDate), DATE(startDate), DATE(endDate)) AS endDate,
    residencePhoneNumber,
    alternatePhoneNumber,
    emailAddress
  FROM current_ids ci
  LEFT JOIN {PIMSResidence} r
    ON ci.inmateNumber = r.inmateNumber
  LEFT JOIN {CodeValue}
    ON codeid = stateCode
  WHERE (DATE(startDate) != DATE(endDate) OR endDate IS NULL)
), 
-- a lot of the periods are overlapping, so we need to clean them up by having the previous 
-- period end when the next one starts
clean_periods AS (
  SELECT 
    inmateNumber, 
    addressLine1, 
    addressLine2, 
    city, 
    state, 
    zipCode, 
    startDate,
    IF(endDate > LEAD(startDate) OVER (PARTITION BY inmateNumber ORDER BY startDate, residenceContactId)
      OR endDate IS NULL AND LEAD(startDate) OVER (PARTITION BY inmateNumber ORDER BY startDate, residenceContactId) IS NOT NULL, 
      LEAD(startDate) OVER (PARTITION BY inmateNumber ORDER BY startDate, residenceContactId), 
    endDate) AS endDate, 
    residencePhoneNumber,
    alternatePhoneNumber,
    emailAddress
  FROM address_info
)
SELECT * FROM clean_periods
WHERE startDate <= @update_timestamp 
  AND (endDate <= @update_timestamp OR endDate IS NULL)
  AND (startDate != endDate OR endDate IS NULL)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ne",
    ingest_view_name="address_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
