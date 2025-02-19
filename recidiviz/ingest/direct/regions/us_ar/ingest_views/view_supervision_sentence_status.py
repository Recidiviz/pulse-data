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
"""Query containing supervision sentence status information."""

from recidiviz.ingest.direct.regions.us_ar.ingest_views.us_ar_view_query_fragments import (
    SUPERVISION_SENTENCE_LENGTHS_FRAGMENT,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH
{SUPERVISION_SENTENCE_LENGTHS_FRAGMENT}
-- Status changes for supervision sentences can only be observed when an entry is added to
-- SUPVTIMELINE. Therefore, we can start with the same fragment used for supervision sentence
-- lengths to get the IDs for each supervision sentence being ingested, and join onto SUPVTIMELINE
-- to get the critical dates and the status on those dates. 

-- Some sentences have statuses that should map to COMPLETED/VACATED, but are followed by
-- other statuses. To avoid ingest errors, these cases are handled in one of two ways:
-- 1. If a COMPLETED/VACATED status is followed by other non-COMPLETED/VACATED statuses,
-- then the COMPLETED/VACATED is assumed to be in error and set to 'MARKED_COMPLETE_TOO_EARLY',
-- so that the status will be mapped to INTERNAL_UNKNOWN instead of COMPLETE/VACATED.
-- 2. If a COMPLETED/VACATED is followed by other COMPLETED/VACATED statuses, then only the
-- first COMPLETED/VACATED status is kept and the others are dropped using the QUALIFY at the
-- end of this query. Because we set statuses to MARKED_COMPLETE_TOO_EARLY before this step,
-- this won't filter out any non-COMPLETED/VACATED statuses.

-- TODO(#38496): Investigate these erroneous-looking COMPLETED/VACATED statuses and determine if
-- this approach makes sense.
SELECT *
FROM (
    SELECT
        OFFENDERID,
        COMMITMENTPREFIX,
        SENTENCECOMPONENT,
        sentence_type,
        SUPVPERIODBEGINDATE,
        CASE 
            WHEN SUPVTIMESTATUSFLAG IN ('9','V') AND NOT only_followed_by_completions 
            THEN 'MARKED_COMPLETE_TOO_EARLY' 
            ELSE SUPVTIMESTATUSFLAG 
        END AS SUPVTIMESTATUSFLAG,
        seq_num
    FROM (
        SELECT 
            *,
            LOGICAL_AND(SUPVTIMESTATUSFLAG IN ('9','V')) 
                OVER (
                    PARTITION BY OFFENDERID,COMMITMENTPREFIX,SENTENCECOMPONENT,sentence_type
                    ORDER BY seq_num 
                    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                ) AS only_followed_by_completions
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY 
                        OFFENDERID,
                        COMMITMENTPREFIX,
                        SENTENCECOMPONENT,
                        sentence_type
                    ORDER BY SUPVPERIODBEGINDATE
                ) AS seq_num
            FROM (
                SELECT 
                    sentence_ids.*,
                    timeline.SUPVPERIODBEGINDATE,
                    timeline.SUPVTIMESTATUSFLAG
                FROM (
                    SELECT DISTINCT
                        OFFENDERID,
                        COMMITMENTPREFIX,
                        SENTENCECOMPONENT,
                        sentence_type
                    FROM sentence_lengths_pp
                ) sentence_ids
                LEFT JOIN st_cleaned timeline
                USING(OFFENDERID,COMMITMENTPREFIX,SENTENCECOMPONENT)
            )
        )
    )
)
QUALIFY COALESCE(LAG(SUPVTIMESTATUSFLAG) OVER (
    PARTITION BY OFFENDERID,COMMITMENTPREFIX,SENTENCECOMPONENT,sentence_type
    ORDER BY seq_num
    ),
    'NA'
) NOT IN ('9','V')
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ar",
    ingest_view_name="supervision_sentence_status",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
