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
"""Query produces a view for StateSentenceGroupLength information.

####### HYDRATED FIELDS #######

parole_eligibility_date_external
--------------------------------------
  - Raw data field: CG_MD
  - Critical date: The create datetime from MODOC

projected_parole_release_date_external
--------------------------------------
  - Raw data field: CG_MM
  - Critical date: The create datetime from MODOC

####### RAW DATA NOTES #######

Raw data files include:
  - LBAKRDTA_TAK044 has minimum eligibility data
Things we discovered/considered:
  - We could not use CG_RC (P&P report date)... it wasn't reliable
  - We use the create datetime to have correct ledger updates
  - CG_MD has quite a few invalid values, likely typos from entry
  - CG_MM has quite a few invalid values, likely typos from entry
  - CG_MD and CG_MM can change and become null if new sentences
    are imposed within the cycle (ex: revocation and additional charge)
"""
from recidiviz.ingest.direct.regions.us_mo.ingest_views.templates_sentences import (
    BS_BT_BU_IMPOSITION_FILTER,
    VALID_STATUS_CODES,
    VALID_SUPERVISION_SENTENCE_INITIAL_INFO,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Produces a row with an update datetime,
# the parole eligibility and release date for that given update datetime.
# The parole dates CAN be null, even if there were dates at a previous point in time.
# Recall this is a GROUP level set of dates and can change if a new sentence has been imposed.
VIEW_QUERY_TEMPLATE = f"""
WITH 
-- This CTE gets cycle IDs so we can make sure we don't hydrate
-- groups with no valid sentences
sentence_groups_from_valid_sentences AS (
    SELECT DISTINCT
        BS_DOC,
        BS_CYC
    FROM
        {{LBAKRDTA_TAK022}}
    LEFT JOIN
        {{LBAKRDTA_TAK023}}
    ON
        BS_DOC = BT_DOC AND
        BS_CYC = BT_CYC AND
        BS_SEO = BT_SEO
    LEFT JOIN
        ({VALID_SUPERVISION_SENTENCE_INITIAL_INFO})
    ON
        BS_DOC = BU_DOC AND
        BS_CYC = BU_CYC AND
        BS_SEO = BU_SEO
    JOIN (
        {VALID_STATUS_CODES}
        -- The Status Sequence (SSO) is not necessarily chronological, so
        -- we order by the status change date instead
        QUALIFY (
            ROW_NUMBER() OVER(
                PARTITION BY BS_DOC, BS_CYC, BS_SEO 
                ORDER BY CAST(BW_SY AS INT64)
            ) = 1)

    ) AS earliest_status_code
    USING (BS_DOC, BS_CYC, BS_SEO)
    WHERE
        {BS_BT_BU_IMPOSITION_FILTER}
),

-- First we handle the strange datetimes for our fields.
-- The date columns are julian. The time columns are padded because
-- they sometimes have only 5 digits.
-- We prefer the row update as the critical date, but fall back to the row create date.
parsed_eligibility_dates AS (
    SELECT DISTINCT 
        CG_DOC, 
        CG_CYC, 
        -- We use created fields to have a unique datetime and pad the time field to parse them correctly.
        SAFE.PARSE_DATETIME("%y%j-%H%M%S", concat(RIGHT(CG_DCR, 5), '-', LPAD(CG_TCR, 6, '0'))) AS group_update_datetime,
        -- These fields have varied invalid values, like '0' and February 30th
        SAFE.PARSE_DATE("%Y%m%d", CG_MD) AS CG_MD, 
        SAFE.PARSE_DATE("%Y%m%d", CG_MM) AS CG_MM 
    FROM
        {{LBAKRDTA_TAK044}}
)
SELECT DISTINCT
    CG_DOC, -- unique for each person
    CG_CYC, -- unique for each cycle (sentence group)
    group_update_datetime,
    CG_MD, -- parole eligibility
    CG_MM  -- parole release
FROM
    parsed_eligibility_dates
JOIN
    sentence_groups_from_valid_sentences
ON
    CG_DOC = BS_DOC 
AND
    CG_CYC = BS_CYC
WHERE
    -- There are 4 cycles that had a null date
    -- but the cycles were a long time ago
    group_update_datetime IS NOT NULL 
AND
    -- Only hydrate a row if we have an actual 
    -- projected date to hydrate!
    (CG_MD IS NOT NULL OR CG_MM IS NOT NULL)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mo",
    ingest_view_name="sentence_group_length",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
