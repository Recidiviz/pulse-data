# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Query containing person <> staff relationship information.

Returns one row per period of time a person is assigned to a case manager or
supervision officer, filtering out:
* Zero-day assignments
* Assignments that end before 2005 (filtered to avoid huge number of bad dates /
    overlapping periods)
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
    OffenderID,
    DATE(a.StartDate) AS StartDate,
    -- This field is an exclusive end date - it usually is the same day the next 
    -- assignment starts.
    DATE(a.EndDate) AS EndDate,
    StaffID,
    AssignmentType
FROM {AssignedStaff} a
WHERE 
    -- Filter out zero-day assignments
    (EndDate IS NULL OR StartDate != EndDate)
    -- Filter down to assignments within the last 20 years. Older data has a lot of 
    -- overlapping / conflicting assignments
    AND (EndDate IS NULL or DATE(EndDate) > "2005-01-01")
    -- Filter to the case types that are relevant to us right now
    AND AssignmentType in (
        'COU',  # COUNSELOR - INSTITUTION
        'PRO', # PROBATION OFFICER
        'PAO', # PAROLE OFFICER
        'CCC'  # COMMUNITY CORRECTION CASEWORK
    )
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tn",
    ingest_view_name="AssignedStaff",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
