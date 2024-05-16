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
"""Query for state sentence groups.

For each of the date fields in the entity, we coalesce three fields from 
the source raw data. This is to accomodate the way automatic and manual overrides are
documented in ACIS. The system prioritizes Manual Lock fields first, because these
contain dates that were manually entered by an officer to override the automatically calculated
date. Then, Adjust Release Date fields are prioritized. These are automatic adjustments
made by ACIS to release dates that fall on weekends or holidays. The final field is the
original release date calculated by ACIS based on sentence information. 

We define a "sentence group" in this case as a single commitment. Many offenses can 
be associated with a single commitment, each having their own sentencing requirements.
There is one controlling offense that determines the final release dates for a given 
sentence group."""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH base AS (
SELECT DISTINCT
    -- off.COMMITMENT_ID AS external_id,
    -- doc.DOC_ID AS doc_id,
    doc.PERSON_ID AS person_id,
    off.UPDT_DTM AS group_update_datetime,
    -- See view description for explanation of datetime field hierarchy.
    COALESCE(
        NULLIF(COMMUNITY_SUPV_BEGIN_DTM_ML, 'NULL'),
        NULLIF(COMMUNITY_SUPV_BEGIN_DTM_ARD, 'NULL'), 
        NULLIF(COMMUNITY_SUPV_BEGIN_DTM, 'NULL')) AS CommunitySupervisionBeginDate, -- CSBD
    COALESCE(
        NULLIF(COMMUNITY_SUPV_END_DTM_ML, 'NULL'), 
        NULLIF(COMMUNITY_SUPV_END_DTM_ARD, 'NULL'), 
        NULLIF(COMMUNITY_SUPV_END_DTM, 'NULL')) AS CommunitySupervisionEndDate, -- CSED
    COALESCE(
        NULLIF(EARNED_RLS_CREDIT_DTM_ML, 'NULL'), 
        NULLIF(EARNED_RLS_CREDIT_DTM_ARD, 'NULL'), 
        NULLIF(EARNED_RLS_CREDIT_DTM, 'NULL')) AS EarnedReleaseCreditDate, -- ERCD
    COALESCE(
        NULLIF(EXPIRATION_DTM_ML, 'NULL'), 
        NULLIF(EXPIRATION_DTM, 'NULL')) AS SentenceExpirationDate, -- SED
FROM {AZ_DOC_SC_OFFENSE@ALL} off
LEFT JOIN {AZ_DOC_SC_EPISODE} sc_episode ON off.OFFENSE_ID = sc_episode.FINAL_OFFENSE_ID
LEFT JOIN {DOC_EPISODE} doc ON sc_episode.DOC_ID = doc.DOC_ID
WHERE doc.PERSON_ID IS NOT NULL
AND off.OFFENSE_ID = sc_episode.FINAL_OFFENSE_ID
)

SELECT *
FROM base
-- This is only the case for 68 offenses that do not have any listed release dates.
WHERE group_update_datetime IS NOT NULL
"""
VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="state_sentence_group",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
