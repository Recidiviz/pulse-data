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
"""Query for state sentences for US_IX."""
from recidiviz.ingest.direct.regions.us_ix.ingest_views.template_sentence import (
    new_sentence_view_template,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

sentening_ctes = new_sentence_view_template()

VIEW_QUERY_TEMPLATE = f"""
WITH 
    {sentening_ctes},
/*
    This CTE gets the most reliable start and end dates that we know of,
    imposition (SentenceDate) and full term release (FtrdApprovedDate). 
    Note that the release date is a SentenceGroup level
    date! This was the field recommended by Idaho as they have no confidence
    in SentenceStatusId. CorrectionsCompact dates seemed to also be unreliable.
*/
known_sentence_dates AS (
    SELECT
        OffenderId,
        SentenceId,
        DATETIME(DATE(SentenceDate)) AS start_dt,
        DATETIME(DATE(FtrdApprovedDate)) AS end_dt
    FROM final_sentences
    JOIN {{scl_Term}} USING (OffenderId, TermId)
    -- 1 person has SentenceDate in the future (2099). Likely a data error
    WHERE DATE(SentenceDate) < CURRENT_DATE
)

SELECT
    OffenderId,
    SentenceId,
    start_dt AS status_update_datetime,
    'SERVING' AS status
FROM known_sentence_dates

UNION ALL

SELECT
    OffenderId,
    SentenceId,
    -- ~1300 sentences have an end date before sentence imposition
    -- The date difference can sometimes span years, and so seems 
    -- unlikely that it is arrising from pre-trial detention.
    GREATEST(start_dt, end_dt) AS status_update_datetime,
    'COMPLETED' AS status
FROM known_sentence_dates
WHERE end_dt <= CURRENT_DATE
"""


VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="state_sentence_status_snapshot",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
