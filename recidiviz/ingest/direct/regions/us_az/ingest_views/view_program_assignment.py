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
   PGMDET.CODE,
   PGMDET.DESCRIPTION AS program_name,
   doc_ep.PERSON_ID,
   DOC_ID,
   COMPLETION_PR_FLAG,
   ENRLSTAT.DESCRIPTION AS ENROLL_STATUS,
   ASSIGNMENT_DATE,
   COMPLETION_DATE,
   CHANGE_EFF_DATE,
   PARTICIPATION_START_DATE,
   PARTICIPATION_END_DATE,
   EXMPTSTAT.DESCRIPTION AS EXEMPTION,
   EDUC.COMMENTS AS EXEMPTION_COMMENTS,
   CAST(EXEMPTION_REMOVED_DATE AS DATETIME) AS EXEMPTION_REMOVED_DATE,
   CAST(COALESCE(ASSIGN.UPDT_DTM, ASSIGN.CREATE_DTM) AS DATETIME) as ASSIGN_UPDT_DTM,
FROM {DOC_PR_ASSIGNMENT} ASSIGN
LEFT JOIN
    {DOC_PR_CYCLE} CYCLE
USING
    (CYCLE_ID)
LEFT JOIN
    {DOC_PROGRAM_COMPLEX_UNIT} PGMCOMPLX
USING
    (PROGRAM_UNIT_ID)
LEFT JOIN
    {DOC_PR_PROGRAM} PRPGM
USING
    (PROGRAM_ID)
LEFT JOIN
    {DOC_PROGRAM_DETAIL} PGMDET 
USING
    (PR_DETAIL_ID)
-- Join this entire table so that we can include rows for people who were never assigned
-- to the program because they have an exemption.
FULL OUTER JOIN
    {DOC_EDUCATION_ACHIEVEMENT} EDUC
USING
    (DOC_ID)
LEFT JOIN
    {LOOKUPS} EXMPTSTAT
ON
    (EDUC.EXEMPTION_ID = EXMPTSTAT.LOOKUP_ID)
LEFT JOIN
    {LOOKUPS} ENRLSTAT
ON
    (ENROLL_STATUS_ID = ENRLSTAT.LOOKUP_ID)
JOIN
    {DOC_EPISODE} DOC_EP
USING
    (DOC_ID)
)

SELECT DISTINCT * EXCEPT (ASSIGN_UPDT_DTM)
FROM all_programs
WHERE 
-- All codes identified by the ADCRR research team that correspond to the Functional Literacy course.
    CODE IN ('E0.01', 'E001','V001','E01A','E009','70.20','E998','X026','E025')
-- Only keep the most recent update 
QUALIFY 
    ROW_NUMBER() OVER (
        PARTITION BY PERSON_ID, DOC_ID, CODE, ASSIGNMENT_DATE 
        ORDER BY ASSIGN_UPDT_DTM DESC, EXEMPTION_REMOVED_DATE DESC
    ) = 1
"""
VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="program_assignment",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
