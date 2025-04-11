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
"""Query containing supervision hearing violations and responses information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- taking all relevent hearing and allegations
violation_hearings AS (
SELECT 
  pkReviewHearingDetail,
  inmateId,
  pkAllegationID, -- to make external id unique
  DATE(hearingDate) AS hearingDate,
  continueParole, 
  revokeReparole,
  residenceVerification, 
  revokeBoard, -- if board revokes parole
  absconding,
  associates,
  communicationDevice,
  directives,
  employmentEducation,
  financial,
  intoxicants,
  narcotics,
  specialCond,
  laws,
  charges,
  allegationType,
  narcoticType,
  c1.codeValue AS pleaCode, 
  continued,
  continuedDate,
  c2.codeValue as causeCode,
FROM {PIMSReviewHearing}
LEFT JOIN  {PIMSReviewHearingAllegation}
ON pkReviewHearingDetail = fkReviewHearingDetail 
LEFT JOIN {CodeValue} c1
 ON pleaCode = c1.codeId
LEFT JOIN {CodeValue} c2
 ON causeCode = c2.codeId
) 
SELECT 
  pkReviewHearingDetail,
  inmateId AS inmateNumber,-- to correspond with other views
  pkAllegationID, -- to make external id unique
  hearingDate,
  CASE 
    WHEN revokeBoard = '1' THEN "revokeBoard"
    WHEN revokeReparole = '1' THEN "revokeReparole"
    WHEN residenceVerification = '1' THEN "residenceVerification"
    WHEN continueParole = '1' THEN "continueParole"
    ELSE "unknown"
  END AS outcome_action,
  IF(charges = 'Narcotics' AND narcoticType IS NOT NULL, charges || ' - ' || narcoticType, charges) AS charges, -- violation
  IF(charges = 'Abscond', 'Abscond', allegationType) AS allegationType, -- violation type
FROM violation_hearings
-- if continued it means no decision was made and they decided at later date
-- if causeCode is yes that means they were found "guilty"
WHERE continued != '1' AND UPPER(causeCode) = 'YES'
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ne",
    ingest_view_name="hearing_supervision_violations_and_responses",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
