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
"""
Ingest view for state sentence group lengths.

For each of the date fields in the entity, we coalesce two fields from 
the source raw data. This is to accomodate the way automatic and manual overrides are
documented in ACIS. The system prioritizes Manual Lock fields first, because these
contain dates that were manually entered by an officer to override the automatically calculated
date. The other field is the original release date calculated by ACIS based on sentence information. 

We define a "sentence group" as a single episode. Many offenses can be associated with a 
single commitment, and many commitments can be affiliated with a single episode. 

FUTURE WORK
-----------
The RELEASE_DTM and RELEASE_DTM_ML fields encapsulate projected_parole_release.
They are described in the data as the "Earliest release date from all active 
offenses in all active commitments in this episode". However, we often get cases
where the projected parole release is AFTER the projected sentence expiration.
We'll need to reconcile this data to include it in the schema.
"""
from recidiviz.ingest.direct.regions.us_az.ingest_views.common_sentencing_views_and_utils import (
    VALID_PEOPLE_AND_SENTENCES,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH 

-- Maps epiodes to people with valid sentences
valid_people AS ({VALID_PEOPLE_AND_SENTENCES}),

-- There are confilicting values for some episodes
-- with the same update datetime. This CTE parses them
-- and we take the minimum value in the output
parsed_dates AS (
    SELECT DISTINCT
        PERSON_ID,
        SC_EPISODE_ID, -- StateSentenceGroup.external_id
        UPDT_DTM,      -- StateSentenceGroupLength.group_update_datetime
        -- Earliest sentence expiration date (SED) from all active offenses 
        -- in all active commitments in this episode
        -- This is for projected_full_term_release_date_min_external
        -- '_ML' columns are 'manual lock', meaning they were manually
        -- updated by staff for a particular reason.
        COALESCE(
            NULLIF(EXPIRATION_DTM_ML, 'NULL'), NULLIF(EXPIRATION_DTM, 'NULL')
        ) AS EXPIRATION_DTM
    FROM 
        {{AZ_DOC_SC_EPISODE@ALL}} AS episode 
    JOIN 
        valid_people
    USING
        (SC_EPISODE_ID)
    WHERE 
        COALESCE(
            NULLIF(EXPIRATION_DTM_ML, 'NULL'), NULLIF(EXPIRATION_DTM, 'NULL')
        ) IS NOT NULL
)
SELECT
    PERSON_ID,
    SC_EPISODE_ID,
    UPDT_DTM,
    MIN(EXPIRATION_DTM) AS EXPIRATION_DTM
FROM
    parsed_dates
WHERE
    EXPIRATION_DTM IS NOT NULL
GROUP BY 
    PERSON_ID, SC_EPISODE_ID, UPDT_DTM
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="state_sentence_group",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
