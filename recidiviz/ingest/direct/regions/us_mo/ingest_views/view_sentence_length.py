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
"""Query produces a view for StateSentenceLength entities.

This currently includes:
  - Time of update (Preferring latest update over date created)
  - projected_completion_date_max_external: BS_PD
  - projected_completion_date_min_external: BT_PC

Raw data files include:
  - LBAKRDTA_TAK022, has BS_PD
  - LBAKRDTA_TAK023, has BT_PC
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

VIEW_QUERY_TEMPLATE = f"""

WITH 

-- This CTE is the sentence_external_ids for sentences with valid
-- status and imposition info. Otherwise we may hydrate sentence length
-- entities with no parent sentence.
valid_sentences AS (

    SELECT DISTINCT
        -- We concat because the a later view has to UNION ALL
        -- and every table has a different prefix :/
        CONCAT(BS_DOC, '-', BS_CYC, '-', BS_SEO) AS sentence_external_id
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
    ) AS earliest_status_code
    USING (BS_DOC, BS_CYC, BS_SEO)
    WHERE
        {BS_BT_BU_IMPOSITION_FILTER}
),

-- This CTE gets the projected_completion_date_max with two potential
-- critical dates. This covers the vast majority of sentences
bs_pd_with_dates AS (
    SELECT DISTINCT 
        BS_DOC, 
        BS_CYC, 
        BS_SEO, 
        /*
            This filters out: 
              - Ivestigation sentence data (66666666)
              - interstate compact and indeterminate dates (88888888)
              - life sentences magic date (99999999)        
              - unknown/error dates (0 and 20000000)
        */
        SAFE.PARSE_DATE('%Y%m%d', BS_PD) AS BS_PD,
        SAFE.PARSE_DATE("%y%j", RIGHT(BS_DCR, 5)) AS created,
        SAFE.PARSE_DATE("%y%j", RIGHT(BS_DLU, 5)) AS updated
    FROM 
        {{LBAKRDTA_TAK022@ALL}}
),
-- Some sentences do not have create or update dates. However,
-- some other sentences in their cycle do. We collect the earliest
-- creation and latest update for a cycle to impute missing critical dates
bs_pd_group_dates AS (
    SELECT 
        BS_DOC, 
        BS_CYC, 
        min(created) AS earliest_group_create_date, 
        max(updated) AS latest_group_update_date
    FROM bs_pd_with_dates
    GROUP BY 1, 2
),
-- This CTE assigns a critical date to the projected completion date max
-- Critical date preference: 
-- sentence updated -> sentence created -> latest group update -> earliest group create
bs_pd_with_critical_date AS (
    SELECT 
        BS_DOC, 
        BS_CYC, 
        BS_SEO,
        -- Critical date preference: 
        -- updated -> created -> latest group update -> earliest group create
        CASE 
            WHEN updated IS NOT NULL 
                THEN updated
            WHEN updated IS NULL AND created IS NOT NULL 
                THEN created 
            WHEN updated IS NULL AND created IS NULL AND latest_group_update_date IS NOT NULL 
                THEN latest_group_update_date
            ELSE earliest_group_create_date 
        END AS critical_date,
        BS_PD
    FROM 
        bs_pd_with_dates
    JOIN 
        bs_pd_group_dates
    USING
        (BS_DOC, BS_CYC)
    WHERE 
        BS_PD IS NOT NULL
),
-- This CTE is what we will use to hydrate projected_completion_date_max_external
-- There are sentences where there was no critical date,
-- but they all have completion dates in the past. We filter them out here.
projected_completion_date_max_view AS (
    SELECT * 
    FROM bs_pd_with_critical_date 
    WHERE critical_date IS NOT NULL
),
-- This CTE gets the projected_completion_date_min with two potential
-- critical dates. This covers the vast majority of sentences
bt_pc_with_dates AS (
    SELECT DISTINCT 
        BT_DOC, 
        BT_CYC, 
        BT_SEO, 
        /*
            This filters out: 
              - interstate compact and indeterminate dates (88888888)
              - life sentences magic date (99999999)        
              - unknown/error dates (0 and 7777777)
        */
        SAFE.PARSE_DATE('%Y%m%d', BT_PC) AS BT_PC,
        SAFE.PARSE_DATE("%y%j", RIGHT(BT_DCR, 5)) AS created,
        SAFE.PARSE_DATE("%y%j", RIGHT(BT_DLU, 5)) AS updated
    FROM 
        {{LBAKRDTA_TAK023@ALL}}
),
-- Some sentences do not have create or update dates. However,
-- some other sentences in their cycle do. We collect the earliest
-- creation and latest update for a cycle to impute missing critical dates
bt_pc_group_dates AS (
    SELECT 
        BT_DOC, 
        BT_CYC, 
        min(created) AS earliest_group_create_date, 
        max(updated) AS latest_group_update_date
    FROM bt_pc_with_dates
    GROUP BY 1, 2
),
-- This CTE assigns a critical date to the projected completion date min
-- Critical date preference: 
-- sentence updated -> sentence created -> latest group update -> earliest group create
bt_pc_with_critical_date AS (
    SELECT 
        BT_DOC, 
        BT_CYC, 
        BT_SEO,
        -- Critical date preference: 
        -- updated -> created -> latest group update -> earliest group create
        CASE 
            WHEN updated IS NOT NULL 
                THEN updated
            WHEN updated IS NULL AND created IS NOT NULL 
                THEN created 
            WHEN updated IS NULL AND created IS NULL AND latest_group_update_date IS NOT NULL 
                THEN latest_group_update_date
            ELSE earliest_group_create_date 
        END AS critical_date,
        BT_PC
    FROM 
        bt_pc_with_dates
    JOIN 
        bt_pc_group_dates
    USING
        (BT_DOC, BT_CYC)
    WHERE 
        BT_PC IS NOT NULL
),
-- This CTE is what we will use to hydrate projected_completion_date_min_external
-- There are sentences where there was no critical date,
-- but they all have completion dates in the past. We filter them out here.
projected_completion_date_min_view AS (
    SELECT * 
    FROM bt_pc_with_critical_date
    WHERE critical_date IS NOT NULL
),
-- This CTE stacks all of our data for completion dates, so that we can
-- fill forward through all critical dates in the final output of the view.
stacked_data AS (

SELECT 
    BS_DOC AS person_external_id,
    CONCAT(BS_DOC, '-', BS_CYC, '-', BS_SEO) AS sentence_external_id,
    critical_date,
    BS_PD,
    NULL AS BT_PC 
FROM 
    projected_completion_date_max_view

UNION ALL

SELECT 
    BT_DOC AS person_external_id,
    CONCAT(BT_DOC, '-', BT_CYC, '-', BT_SEO) AS sentence_external_id,
    critical_date,
    NULL AS BS_PD,
    BT_PC 
FROM 
    projected_completion_date_min_view
)
SELECT DISTINCT
    person_external_id,
    sentence_external_id,
    critical_date,
    LAST_VALUE(BS_PD IGNORE NULLS) OVER (
        PARTITION BY person_external_id, sentence_external_id 
        ORDER BY critical_date
    ) AS BS_PD,
    LAST_VALUE(BT_PC IGNORE NULLS) OVER (
        PARTITION BY person_external_id, sentence_external_id 
        ORDER BY critical_date
    ) AS BT_PC
FROM
    stacked_data
JOIN
    valid_sentences
USING (sentence_external_id)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mo",
    ingest_view_name="sentence_length",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
