# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Query containing program assignment information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT 
  OFFENDERID,
  WORKPGMASSIGNMENTCODE,
  JOBPGMASSIGNMENTLOC,
  JOBPGMASSIGNMENTSTATUS,
  CAST(
    CONCAT(
      SPLIT(PROGRAMASSIGNEDDATE, ' ')[OFFSET(0)],' ',PROGRAMASSIGNEDTIME
    ) AS DATETIME
  ) AS PADATETIME,
  WRKASSIGNSTATUSDATE,
  WORKASSIGNMENTAUTHBY
FROM {JOBPROGRAMASGMT}
-- WORKPGMASSIGNMENTCODE '00.00' means 'Never Assigned', which shouldn't be ingested
-- as an assignment.
WHERE WORKPGMASSIGNMENTCODE != '00.00' 
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ar",
    ingest_view_name="program_assignment",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
