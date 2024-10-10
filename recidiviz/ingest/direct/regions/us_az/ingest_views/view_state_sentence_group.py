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

For each of the date fields in the entity, we coalesce three fields from 
the source raw data. This is to accomodate the way automatic and manual overrides are
documented in ACIS. The system prioritizes Manual Lock fields first, because these
contain dates that were manually entered by an officer to override the automatically calculated
date. Then, Adjust Release Date fields are prioritized. These are automatic adjustments
made by ACIS to release dates that fall on weekends or holidays. The final field is the
original release date calculated by ACIS based on sentence information. 

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
-- Cleans AZ_DOC_SC_OFFENSE raw data to null out fields that contain the string 'NULL'
-- and fields that contain dates outside of a reasonable date range.
cleaned_AZ_DOC_SC_OFFENSE_ALL AS (
    SELECT 
        OFFENSE_ID, 
        UPDT_DTM,
        IF(COMMUNITY_SUPV_BEGIN_DTM_ML BETWEEN '1900-01-01' AND '2100-01-01', COMMUNITY_SUPV_BEGIN_DTM_ML, NULL) AS COMMUNITY_SUPV_BEGIN_DTM_ML,
        IF(COMMUNITY_SUPV_BEGIN_DTM_ARD BETWEEN '1900-01-01' AND '2100-01-01', COMMUNITY_SUPV_BEGIN_DTM_ARD, NULL) AS COMMUNITY_SUPV_BEGIN_DTM_ARD,
        IF(COMMUNITY_SUPV_BEGIN_DTM BETWEEN '1900-01-01' AND '2100-01-01', COMMUNITY_SUPV_BEGIN_DTM, NULL) AS COMMUNITY_SUPV_BEGIN_DTM,
        IF(EARNED_RLS_CREDIT_DTM_ML BETWEEN '1900-01-01' AND '2100-01-01', EARNED_RLS_CREDIT_DTM_ML, NULL) AS EARNED_RLS_CREDIT_DTM_ML,
        IF(EARNED_RLS_CREDIT_DTM_ARD BETWEEN '1900-01-01' AND '2100-01-01', EARNED_RLS_CREDIT_DTM_ARD, NULL) AS EARNED_RLS_CREDIT_DTM_ARD,
        IF(EARNED_RLS_CREDIT_DTM BETWEEN '1900-01-01' AND '2100-01-01', EARNED_RLS_CREDIT_DTM, NULL) AS EARNED_RLS_CREDIT_DTM,
        IF(TR_TO_ABSOLUTE_DSCHRG_DTM_ML BETWEEN '1900-01-01' AND '2100-01-01', TR_TO_ABSOLUTE_DSCHRG_DTM_ML, NULL) AS TR_TO_ABSOLUTE_DSCHRG_DTM_ML,
        IF(TR_TO_ABSOLUTE_DSCHRG_DTM_ARD BETWEEN '1900-01-01' AND '2100-01-01', TR_TO_ABSOLUTE_DSCHRG_DTM_ARD, NULL) AS TR_TO_ABSOLUTE_DSCHRG_DTM_ARD,
        IF(TR_TO_ABSOLUTE_DSCHRG_DTM BETWEEN '1900-01-01' AND '2100-01-01', TR_TO_ABSOLUTE_DSCHRG_DTM, NULL) AS TR_TO_ABSOLUTE_DSCHRG_DTM,
        IF(ABSOLUTE_DISCHARGE_DTM_ML BETWEEN '1900-01-01' AND '2100-01-01', ABSOLUTE_DISCHARGE_DTM_ML, NULL) AS ABSOLUTE_DISCHARGE_DTM_ML,
        IF(ABSOLUTE_DISCHARGE_DTM_ARD BETWEEN '1900-01-01' AND '2100-01-01', ABSOLUTE_DISCHARGE_DTM_ARD, NULL) AS ABSOLUTE_DISCHARGE_DTM_ARD,
        IF(ABSOLUTE_DISCHARGE_DTM BETWEEN '1900-01-01' AND '2100-01-01', ABSOLUTE_DISCHARGE_DTM, NULL) AS ABSOLUTE_DISCHARGE_DTM
    FROM (
        SELECT
            OFFENSE_ID, 
            NULLIF(UPDT_DTM, 'NULL') AS UPDT_DTM,
            NULLIF(COMMUNITY_SUPV_BEGIN_DTM_ML, 'NULL') AS COMMUNITY_SUPV_BEGIN_DTM_ML,
            NULLIF(COMMUNITY_SUPV_BEGIN_DTM_ARD, 'NULL') AS COMMUNITY_SUPV_BEGIN_DTM_ARD, 
            NULLIF(COMMUNITY_SUPV_BEGIN_DTM, 'NULL') AS COMMUNITY_SUPV_BEGIN_DTM,
            NULLIF(EARNED_RLS_CREDIT_DTM_ML, 'NULL') AS EARNED_RLS_CREDIT_DTM_ML,
            NULLIF(EARNED_RLS_CREDIT_DTM_ARD, 'NULL') AS EARNED_RLS_CREDIT_DTM_ARD, 
            NULLIF(EARNED_RLS_CREDIT_DTM, 'NULL') AS EARNED_RLS_CREDIT_DTM,
            NULLIF(TR_TO_ABSOLUTE_DSCHRG_DTM_ML, 'NULL') AS TR_TO_ABSOLUTE_DSCHRG_DTM_ML, 
            NULLIF(TR_TO_ABSOLUTE_DSCHRG_DTM_ARD, 'NULL') AS TR_TO_ABSOLUTE_DSCHRG_DTM_ARD,
            NULLIF(TR_TO_ABSOLUTE_DSCHRG_DTM, 'NULL') AS TR_TO_ABSOLUTE_DSCHRG_DTM,
            NULLIF(ABSOLUTE_DISCHARGE_DTM_ML, 'NULL') AS ABSOLUTE_DISCHARGE_DTM_ML, 
            NULLIF(ABSOLUTE_DISCHARGE_DTM_ARD, 'NULL') AS ABSOLUTE_DISCHARGE_DTM_ARD,
            NULLIF(ABSOLUTE_DISCHARGE_DTM, 'NULL') AS ABSOLUTE_DISCHARGE_DTM
        FROM {{AZ_DOC_SC_OFFENSE@ALL}}
    )
    ),
-- Cleans AZ_DOC_SC_EPISODE raw data to null out fields that contain the string 'NULL'
-- and fields that contain dates outside of a reasonable date range.
cleaned_AZ_DOC_SC_EPISODE_ALL AS (
    SELECT 
        SC_EPISODE_ID,
        FINAL_OFFENSE_ID, 
        UPDT_DTM,
        IF(EXPIRATION_DTM_ML BETWEEN '1900-01-01' AND '2100-01-01', EXPIRATION_DTM_ML, NULL) AS EXPIRATION_DTM_ML,
        IF(EXPIRATION_DTM BETWEEN '1900-01-01' AND '2100-01-01', EXPIRATION_DTM, NULL) AS EXPIRATION_DTM,
    FROM (
        SELECT
            SC_EPISODE_ID,
            FINAL_OFFENSE_ID, 
            NULLIF(UPDT_DTM, 'NULL') AS UPDT_DTM,
            NULLIF(EXPIRATION_DTM_ML, 'NULL') AS EXPIRATION_DTM_ML,
            NULLIF(EXPIRATION_DTM, 'NULL') AS EXPIRATION_DTM, 
        FROM {{AZ_DOC_SC_EPISODE@ALL}}
        )
),
-- There are confilicting values for some episodes
-- with the same update datetime. This CTE parses them
-- and we take the minimum value in the output
parsed_dates AS (
    SELECT DISTINCT
        PERSON_ID,
        SC_EPISODE_ID,  -- StateSentenceGroup.external_id
        COALESCE(ruling_offense.UPDT_DTM, episode.UPDT_DTM) AS UPDT_DTM,    -- StateSentenceGroupLength.group_update_datetime
    COALESCE(
        COMMUNITY_SUPV_BEGIN_DTM_ML,
        COMMUNITY_SUPV_BEGIN_DTM_ARD,
        COMMUNITY_SUPV_BEGIN_DTM) AS CommunitySupervisionBeginDate, -- CSBD
    COALESCE(
        EARNED_RLS_CREDIT_DTM_ML, 
        EARNED_RLS_CREDIT_DTM_ARD, 
        EARNED_RLS_CREDIT_DTM) AS EarnedReleaseCreditDate, -- ERCD
    COALESCE(
        TR_TO_ABSOLUTE_DSCHRG_DTM_ML, 
        TR_TO_ABSOLUTE_DSCHRG_DTM_ARD,
        TR_TO_ABSOLUTE_DSCHRG_DTM) AS TransitionToAbsoluteDischargeDate, -- TR to ADD
    COALESCE(
        ABSOLUTE_DISCHARGE_DTM_ML, 
        ABSOLUTE_DISCHARGE_DTM_ARD,
        ABSOLUTE_DISCHARGE_DTM) AS AbsoluteDischargeDate, -- Absolute Discharge Date (ADD)
    COALESCE(
        EXPIRATION_DTM_ML, 
        EXPIRATION_DTM) AS SentenceExpirationDate -- SED
    FROM 
        cleaned_AZ_DOC_SC_EPISODE_ALL AS episode 
    JOIN 
        valid_people
    USING
        (SC_EPISODE_ID)
    -- The dates associated with the Final Ruling Offense are the ones that dictate
    -- the time a person actually spends in prison for a given group of sentences
    -- imposed together.
    JOIN 
        cleaned_AZ_DOC_SC_OFFENSE_ALL AS ruling_offense
    ON
        (episode.FINAL_OFFENSE_ID = ruling_offense.OFFENSE_ID)
)
SELECT
    PERSON_ID,
    SC_EPISODE_ID,
    UPDT_DTM,
    MIN(SentenceExpirationDate) AS SentenceExpirationDate,
    MIN(CommunitySupervisionBeginDate) AS CommunitySupervisionBeginDate,
    MIN(EarnedReleaseCreditDate) AS EarnedReleaseCreditDate,
    MIN(TransitionToAbsoluteDischargeDate) AS TransitionToAbsoluteDischargeDate,
    MIN(AbsoluteDischargeDate) AS AbsoluteDischargeDate
FROM
    parsed_dates
WHERE 
    UPDT_DTM IS NOT NULL
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
