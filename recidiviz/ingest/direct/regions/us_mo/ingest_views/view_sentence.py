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
"""Query produces a view for sentence information known at imposition.
This includes:
  - Charge information
  - Type of sentence (state prison, parole, etc.) *imposed*
  - Static information that won't change over the lifetime of a sentence.

Raw data files include:
  - LBAKRDTA_TAK022 has the base information for incarceration sentences
  - LBAKRDTA_TAK023 has detailed infor for incarceration sentences
  - LBAKRDTA_TAK024 has detailed infor for supervision sentences
  - LBAKRDTA_TAK025 and LBAKRDTA_TAK026 for status code information
"""
from recidiviz.ingest.direct.regions.us_mo.ingest_views.templates_sentences import (
    BS_BT_BU_IMPOSITION_FILTER,
    IS_PRETRIAL_OR_BOND_STATUS,
    VALID_STATUS_CODES,
    VALID_SUPERVISION_SENTENCE_INITIAL_INFO,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

BASE_SENTENCE_INFO = """
SELECT
    BS_DOC, -- unique for each person
    BS_CYC, -- unique for each sentence group
    BS_SEO, -- unique for each sentence
    BS_CNS, -- sentence county_code,
    BS_NCI, -- charge ncic_code,
    BS_ASO, -- charge statute,
    BS_CLT, -- charge classification_type,
    BS_CNT, -- charge county_code,
    BS_CLA, -- charge classification_subtype,
    BS_DO,  -- charge offense_date,
    BS_COD, -- charge description,
    CASE    -- charge judicial_district_code
       WHEN BS_CRC = '999' THEN NULL ELSE BS_CRC
    END AS BS_CRC,
    BS_CCI,
    BS_CRQ
FROM
    {LBAKRDTA_TAK022} AS base_sentence
"""

# We get data for anyone who was incarcerated,
# however the existence of this data does not neccessarily
# mean they were *sentenced* to incarceration.
# For example, they could have had probation revoked
INCARCERATION_SENTENCE_DETAIL_INFO = """
SELECT
    BT_DOC, -- unique for each person
    BT_CYC, -- unique for each sentence group
    BT_SEO, -- unique for each sentence
    BT_SD,  -- sentence imposed_date
    BT_CRR, -- sentence is_life
    BT_SDI -- sentence is_capital_punishment
FROM
    {LBAKRDTA_TAK023}
"""

VIEW_QUERY_TEMPLATE = f"""
WITH

    -- This CTE provides charge level information
    base_sentence_info AS ({BASE_SENTENCE_INFO}),

    -- These are all statuses beginning from the
    -- the first non-pre-trial and non-bond status
    valid_status_codes AS ({VALID_STATUS_CODES}),

    -- This gives the status code and description of the first status. 
    -- We use both to infer sentence type and sentencing authority
    earliest_status_code AS (
        SELECT * FROM valid_status_codes
        WHERE NOT ({IS_PRETRIAL_OR_BOND_STATUS})
        -- The Status Sequence (SSO) is not necessarily chronological, so
        -- we order by the status change date instead
        QUALIFY (
            ROW_NUMBER() OVER(
                PARTITION BY BS_DOC, BS_CYC, BS_SEO 
                ORDER BY CAST(BW_SY AS INT64)
            ) = 1)
    ),

    -- These are not neccessarily incarceration sentences,
    -- but has imposition information if the eariest status code
    -- denotes incarceration.
    incarceration_detail AS ({INCARCERATION_SENTENCE_DETAIL_INFO}),

    -- These are not neccessarily supervision sentences, but will have
    -- the necessary imposition date for probation sentences.
    supervision_detail AS ({VALID_SUPERVISION_SENTENCE_INITIAL_INFO}),

    -- We need all valid sentences as a relation so that we know
    -- any consecutive sentence IDs are for a valid sentence.
    valid_sentences AS (

    SELECT
        base_sentence_info.*,
        incarceration_detail.BT_SD,  -- sentence imposed_date, incarceration
        incarceration_detail.BT_CRR, -- sentence is_life
        incarceration_detail.BT_SDI, -- sentence is_capital_punishment
        supervision_detail.BU_SF,    -- sentence imposed_date, supervision
        earliest_status_code.FH_SDE AS initial_status_desc,
        earliest_status_code.BW_SCD AS initial_status_code
    FROM 
        base_sentence_info
    LEFT JOIN 
        incarceration_detail
    ON
        BS_DOC = BT_DOC AND 
        BS_CYC = BT_CYC AND
        BS_SEO = BT_SEO
    LEFT JOIN 
        supervision_detail 
    ON
        BS_DOC = BU_DOC AND 
        BS_CYC = BU_CYC AND
        BS_SEO = BU_SEO
    JOIN 
        earliest_status_code
    USING
        (BS_DOC, BS_CYC, BS_SEO)
    WHERE
        {BS_BT_BU_IMPOSITION_FILTER}
    ),

    -- Maps sentence external IDs to the external IDs of
    -- the sentence they are consecutive to.
    valid_consecutive_sentences AS (
        SELECT DISTINCT
            BS_DOC,
            BS_CYC,
            BS_SEO,    
            CASE
                WHEN BS_CCI = 'CS'
                THEN concat(BS_DOC, '-', BS_CYC, '-', BS_CRQ)
                ELSE null
            END AS parent_sentence_external_id_array
        FROM
            valid_sentences
        WHERE
            concat(BS_DOC, '-', BS_CYC, '-', BS_CRQ)
        IN (
            SELECT concat(BS_DOC, '-', BS_CYC, '-', BS_SEO)
            FROM valid_sentences
        )
    )
-- We left join because not every sentence is consecutive
SELECT
    valid_sentences.BS_DOC,  -- unique for each person
    valid_sentences.BS_CYC,  -- unique for each sentence group
    valid_sentences.BS_SEO,  -- unique for each sentence
    valid_sentences.BS_CNS,  -- sentence county_code
    valid_sentences.BS_NCI,  -- charge ncic_code,
    valid_sentences.BS_ASO,  -- charge statute,
    valid_sentences.BS_CLT,  -- charge classification_type,
    valid_sentences.BS_CNT,  -- charge county_code,
    valid_sentences.BS_CLA,  -- charge classification_subtype,
    valid_sentences.BS_DO,   -- charge offense_date,
    valid_sentences.BS_COD,  -- charge description,
    valid_sentences.BS_CRC,  -- charge judicial_district_code
    valid_sentences.BT_SD,  -- sentence imposed_date, incarceration
    valid_sentences.BT_CRR, -- sentence is_life
    valid_sentences.BT_SDI, -- sentence is_capital_punishment
    valid_sentences.BU_SF,    -- sentence imposed_date, supervision
    valid_consecutive_sentences.parent_sentence_external_id_array,
    valid_sentences.initial_status_desc,
    valid_sentences.initial_status_code
FROM valid_sentences
LEFT JOIN valid_consecutive_sentences
USING (BS_DOC, BS_CYC, BS_SEO)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mo",
    ingest_view_name="sentence",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
