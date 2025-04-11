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
"""Query containing supervision violations and responses information that does not result in hearing.
These are considered less serious violations they can handle without offical hearings.
If they are more serious or refuse these violation responses they will be ingested
seperately in view_hearing_supervision_violations_and_responses
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- joining all tables with violation and response information, not including violation requiring a hearing
-- those are ingested seperately in view_hearing_supervision_violations_and_responses
vio_and_resp AS (
  SELECT 
    a.id,
    ViolationID,
    inmateNumber, 
    DATE(ViolationDt) AS ViolationDt,
    Synopsis,
    VioSeverity,
    CAST(DATE(ResponseDt) AS STRING) AS ResponseDt,
    ResponseLevel,
    behavior,
    significancelevel,
    technical,
    NewCrime,
    Misdemeanor,
    Felony,
    CASE 
        WHEN technical = '1' then "technical"
        WHEN Misdemeanor = '1' then "misdemeanor"
        WHEN Felony = '1' then "felony"
        ELSE "unknown"
    END AS violationType,
    response,
    sanctionAcceptanceCode

  FROM {PIMSSanctions} a 
  LEFT JOIN {PIMSSanctionsBehaviorEvent} b 
    ON b.EventID = a.id
  LEFT JOIN {PIMSSanctionsNoncompliantBehaviors} c
    ON b.ViolationID = c.ID
  LEFT JOIN {PIMSSanctionsResponseEvent} d
    ON d.EventID = a.id
  LEFT JOIN {PIMSSanctionsNoncompliantResponses} e
    ON d.ResponseID = e.ID
)
SELECT 
  ViolationID,
  inmateNumber, 
  id, 
  ViolationDt,
  behavior,
  VioSeverity,
  violationType,
   TO_JSON_STRING(
      ARRAY_AGG(STRUCT< inmateNumber string,
                        ViolationID string,
                        id string,
                        ResponseDt string,
                        ResponseLevel string,
                        response string,
                        sanctionAcceptanceCode string>
                (inmateNumber,
                ViolationId,
                id,
                ResponseDt,
                ResponseLevel,
                response,
                sanctionAcceptanceCode) ORDER BY response)
    ) AS response_data
FROM vio_and_resp
GROUP BY inmateNumber, violationID, id, ViolationDt,violationType,VioSeverity,behavior
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ne",
    ingest_view_name="non_hearing_supervision_violations_and_responses",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
