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
"""Query containing program assignment information for people incarcerated in ND.

NOTE: Some of these "program assignments" are not programs at all, but case manager 
assignments, or work assignments. All expected programs are present (T4C, CBISA, etc), 
but there is a wide array of program descriptions and the word "program" itself seems 
to be used as a bit of a catch-all here. There is no easy way to filter out the irrelevant 
assignments, so they are all ingested here."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
    REPLACE(REPLACE(OFFENDER_BOOK_ID, '.00',''), ',', '') AS OFFENDER_BOOK_ID,
    REPLACE(REPLACE(OFF_PRGREF_ID, '.00',''), ',', '') AS OFF_PRGREF_ID,
    OFFENDER_PROGRAM_STATUS,
    ps.DESCRIPTION AS PROGRAM_DESCRIPTION,
    ca.DESCRIPTION AS ACTIVITY_DESCRIPTION,
    REPLACE(REPLACE(REFERRAL_STAFF_ID, '.00',''), ',', '') AS REFERRAL_STAFF_ID,
    REFERRAL_DATE,
    OFFENDER_START_DATE,
    OFFENDER_END_DATE,
    REJECT_DATE,
    opp.AGY_LOC_ID
FROM
  {elite_OffenderProgramProfiles} opp
LEFT JOIN
  {elite_ProgramServices} ps
USING
  (PROGRAM_ID)
LEFT JOIN
  {recidiviz_elite_CourseActivities} ca
USING
  (CRS_ACTY_ID)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="elite_program_assignment",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
