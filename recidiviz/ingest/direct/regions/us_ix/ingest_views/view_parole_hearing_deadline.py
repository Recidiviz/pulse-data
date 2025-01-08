# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Query that generates the State Task Deadline entities for parole hearing dates"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """WITH

-- NOTES ABOUT THIS VIEW:
-- This view gathers all intitial and subsequent parole hearing dates.

-- Collect all future and initial hearing dates
collect_dates AS (
    SELECT
    NextHearingDate AS hearing_date, 
    OffenderId, 
    TermId,
    UpdateDate,
    "Subsequent" as type
    FROM {scl_Term} 
    WHERE NextHearingDate IS NOT NULL

    UNION ALL 

    SELECT 
        InitialParoleHearingDate as hearing_date, 
        OffenderId, 
        TermId,
        UpdateDate,
        "Initial" as type
    FROM {scl_Term} 
    WHERE InitialParoleHearingDate IS NOT NULL 
)

SELECT
    hearing_date,
    OffenderId,
    TermId,
    UpdateDate,
    type
FROM collect_dates
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="parole_hearing_deadline",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
