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
"""Query that generates the State Task Deadline entities for parole hearing dates"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH

-- NOTES ABOUT THIS VIEW:
-- This view gathers all initial and subsequent parole hearing dates.

-- Collect all future and initial hearing dates
collect_dates AS (
    SELECT
    CAST(NextHearingDate AS DATETIME) AS hearing_date, 
    OffenderId, 
    TermId,
    CAST(UpdateDate AS DATETIME) AS UpdateDate,
    CAST(InsertDate AS DATETIME) AS InsertDate,
    "Subsequent" as type,
    --identify the first date per person per term to account for dates set before we started tracking updates
    ROW_NUMBER() OVER(PARTITION BY OffenderId, TermId ORDER BY CAST(UpdateDate AS DATETIME)) AS rownum,
    LAG(CAST(NextHearingDate AS DATETIME)) OVER (PARTITION BY OffenderId, TermId ORDER BY CAST(UpdateDate AS DATETIME)) AS prev_hearing_date
    FROM {scl_Term@ALL}
    WHERE NextHearingDate IS NOT NULL

    UNION ALL 

    SELECT 
        CAST(InitialParoleHearingDate AS DATETIME) as hearing_date, 
        OffenderId, 
        TermId,
        CAST(UpdateDate AS DATETIME) AS UpdateDate,
        CAST(InsertDate AS DATETIME) AS InsertDate,
        "Initial" as type,
        --identify the first date per person per term to account for dates set before we started tracking updates
        ROW_NUMBER() OVER(PARTITION BY OffenderId, TermId ORDER BY CAST(UpdateDate AS DATETIME)) AS rownum,
        LAG(CAST(InitialParoleHearingDate AS DATETIME)) OVER (PARTITION BY OffenderId, TermId ORDER BY CAST(UpdateDate AS DATETIME)) AS prev_hearing_date
    FROM {scl_Term@ALL} 
    WHERE InitialParoleHearingDate IS NOT NULL 
)

SELECT
    hearing_date,
    OffenderId,
    TermId,
    UpdateDate,
    type,
    rownum
FROM collect_dates
WHERE rownum != 1
--only include rows where hearing_date has actually changed
    AND (prev_hearing_date IS NULL OR prev_hearing_date != hearing_date)

UNION ALL 

/* For the first initial and subsequent hearing date per term per person, use UpdateDate if the hearing date falls after
the UpdateDate, otherwise use InsertDate to account for dates set before we received updates */
SELECT
    hearing_date,
    OffenderId,
    TermId,
    IF(hearing_date < UpdateDate, InsertDate, UpdateDate) AS UpdateDate,
    type,
    rownum
FROM collect_dates
WHERE rownum = 1
--only include rows where hearing_date has actually changed
    AND (prev_hearing_date IS NULL OR prev_hearing_date != hearing_date)

"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="parole_hearing_deadline",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
