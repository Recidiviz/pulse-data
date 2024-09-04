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
"""Query containing ARORA assessment information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT 
  OFFENDERID,
    CAST(
      CONCAT(
        SPLIT(RISKCLASSDATE, ' ')[OFFSET(0)],' ',RISKCLASSTIME
      ) AS DATETIME
    ) AS RCDATETIME,
    RISKASSESSMENTTYPE,
    RISK2SCORE,
    CASE 
      WHEN STAFFIDPERFASSESS IN (
        SELECT DISTINCT PARTYID
        FROM {PERSONPROFILE}
      ) THEN STAFFIDPERFASSESS
      ELSE NULL
    END AS STAFFIDPERFASSESS
FROM {RISKNEEDCLASS}
WHERE RISKASMTSTATUS NOT IN (
  'A', -- Needs Confirmation
  '1', -- Pending
  '5' -- Denied
)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ar",
    ingest_view_name="arora_assessment",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
