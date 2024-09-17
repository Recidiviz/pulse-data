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
"""Query for program assignments only relating to the Mandatory Literacy program in AZ,
as well as information about exemptions from that program.

Creates placeholder entities for people who have never been placed in the Mandatory
Literacy program because they have an exemption."""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- Collect information about all program assignments and exemptions from the mandatory
-- literacy course.
all_programs AS (
SELECT DISTINCT 
   cycle_name,
   doc_ep.PERSON_ID,
   DOC_ID,
   COMPLETION_PR_FLAG,
   enroll_status.DESCRIPTION AS ENROLL_STATUS,
   ASSIGNMENT_DATE,
   COMPLETION_DATE,
   CHANGE_EFF_DATE,
   PARTICIPATION_START_DATE,
   PARTICIPATION_END_DATE,
   exemption_status.DESCRIPTION AS EXEMPTION,
   NULLIF(education.COMMENTS, 'NULL') AS EXEMPTION_COMMENTS,
   NULLIF(EXEMPTION_REMOVED_DATE, 'NULL') AS EXEMPTION_REMOVED_DATE
FROM {DOC_PR_ASSIGNMENT} pr
LEFT JOIN {DOC_PR_CYCLE} cycle
    USING(CYCLE_ID)
-- Join this entire table so that we can include rows for people who were never assigned
-- to the program because they have an exemption.
FULL OUTER JOIN {DOC_EDUCATION_ACHIEVEMENT} education
    USING(DOC_ID)
LEFT JOIN {LOOKUPS} exemption_status 
  ON (education.EXEMPTION_ID = exemption_status.LOOKUP_ID)
LEFT JOIN {LOOKUPS} enroll_status
    ON(pr.ENROLL_STATUS_ID = enroll_status.LOOKUP_ID)
JOIN {DOC_EPISODE} doc_ep
    USING(DOC_ID)
)

SELECT *
FROM all_programs
WHERE 
    ((UPPER(CYCLE_NAME) LIKE "%MAN%" OR UPPER(CYCLE_NAME) LIKE "%FUNC%")
    AND (UPPER(CYCLE_NAME) LIKE "%LIT%"))
    -- Include people who have not been assigned to the course because of an exemption
    OR (CYCLE_NAME IS NULL AND EXEMPTION IS NOT NULL)
"""
VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="program_assignment",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
