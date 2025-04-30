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
"""Query containing supervision contacts information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- some officers have their ID change over time, this is to get there most
-- recent ID number 
latest_officer_id AS (
SELECT 
  argusId,
  paroleOfficerId,
  FROM {PIMSParoleOfficer}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY lower(argusId) ORDER BY startDate DESC) = 1
)
SELECT DISTINCT
  contactDashboardId,
  inmateNumber,
  DATE(dateOfContact) AS dateOfContact,
  paroleOfficerId,
  c1.codeValue AS contactType,
  c2.codeValue AS contactLocation,
  --contactNotes, # leaving in case we want to see text of contact in the future
  status,
FROM {PIMSContactDashboard}
LEFT JOIN {CodeValue} c1
ON c1.codeid = contactType
LEFT JOIN {CodeValue} c2
ON c2.codeid = contactLocation
LEFT JOIN latest_officer_id
ON lower(contactMadeBy) = lower(argusId)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ne",
    ingest_view_name="supervision_contacts",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
