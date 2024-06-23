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
    EARLIEST_STATUS_CODE,
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
    CASE
        WHEN BS_CCI = 'CS'
        THEN concat(BS_DOC, '-', BS_CYC, '-', BS_CRQ)
        ELSE null
    END AS parent_sentence_external_id_array
FROM
    {LBAKRDTA_TAK022} AS base_sentence
"""

# We get data for anyone who was incarcerated,
# so we add an FSO placeholder to join to status codes.
# We infer an incarceration sentence if the initial
# status code for this person/cycle has an FSO of 0
INCARCERATION_SENTENCE_DETAIL_INFO = """
SELECT
    BT_DOC, -- unique for each person
    BT_CYC, -- unique for each sentence group
    BT_SEO, -- unique for each sentence
    BT_SD,  -- sentence imposed_date
    BT_CRR, -- sentence is_life
    BT_SDI, -- sentence is_capital_punishment
    '0' AS FSO_PLACEHOLDER -- needed to join to earliest status table
FROM
    {LBAKRDTA_TAK023}
"""

# We get multiple rows per sentence and then join to
# a relation of earliest statuses
SUPERVISION_SENTENCE_DETAIL_INFO = """
SELECT
    BU_DOC, -- unique for each person
    BU_CYC, -- unique for each sentence group
    BU_SEO, -- unique for each sentence
    BU_FSO, -- mutliple per sentence, needed to join
    BU_SF  -- sentence imposed_date
FROM
    {LBAKRDTA_TAK024}
"""

VIEW_QUERY_TEMPLATE = f"""
WITH

    -- This CTE provides charge level information
    base_sentence_info AS ({BASE_SENTENCE_INFO}),

    -- This gives the FSO, code, and description of the first status. 
    -- '0' is incarceration, > 0 is supervision. We use all three pieces
    -- of data to infer sentence_type
    earliest_status_code AS ({EARLIEST_STATUS_CODE}),

    -- These are actual incarceration sentences.
    -- We inner join to earliest_status_code to filter out TAK023 data
    -- that arises to capture incarceration from revocation.
    incarceration_sentences AS (
        SELECT *
        FROM ({INCARCERATION_SENTENCE_DETAIL_INFO}) AS inc
        JOIN 
            earliest_status_code
        ON
            earliest_status_code.BS_DOC = inc.BT_DOC AND 
            earliest_status_code.BS_CYC = inc.BT_CYC AND 
            earliest_status_code.BS_SEO = inc.BT_SEO AND
            earliest_status_code.BV_FSO = inc.FSO_PLACEHOLDER
    ),

    -- These are actual supervision sentences.
    -- We inner join to earliest_status_code to filter out TAK024 data
    -- that arises from field sequence data over time and/or
    -- pre-trial investigation data.
    supervision_sentences AS (
        SELECT *
        FROM ({SUPERVISION_SENTENCE_DETAIL_INFO}) AS sup
        JOIN 
            earliest_status_code
        ON
            earliest_status_code.BS_DOC = sup.BU_DOC AND 
            earliest_status_code.BS_CYC = sup.BU_CYC AND 
            earliest_status_code.BS_SEO = sup.BU_SEO AND
            earliest_status_code.BV_FSO = sup.BU_FSO
    )

SELECT
    base_sentence_info.BS_DOC,                 -- unique for each person
    base_sentence_info.BS_CYC,                 -- unique for each sentence group
    base_sentence_info.BS_SEO,                 -- unique for each sentence
    base_sentence_info.BS_CNS,                 -- sentence county_code
    base_sentence_info.BS_NCI,                 -- charge ncic_code,
    base_sentence_info.BS_ASO,                 -- charge statute,
    base_sentence_info.BS_CLT,                 -- charge classification_type,
    base_sentence_info.BS_CNT,                 -- charge county_code,
    base_sentence_info.BS_CLA,                 -- charge classification_subtype,
    base_sentence_info.BS_DO,                  -- charge offense_date,
    base_sentence_info.BS_COD,                 -- charge description,
    base_sentence_info.BS_CRC,                 -- charge judicial_district_code
    base_sentence_info.parent_sentence_external_id_array,
    incarceration_sentences.BT_SD,  -- sentence imposed_date, incarceration
    incarceration_sentences.BT_CRR, -- sentence is_life
    incarceration_sentences.BT_SDI, -- sentence is_capital_punishment
    supervision_sentences.BU_SF,    -- sentence imposed_date, supervision
    earliest_status_code.FH_SDE AS initial_status_desc,
    earliest_status_code.BW_SCD AS initial_status_code,
    earliest_status_code.BV_FSO AS initial_FSO,
FROM base_sentence_info
LEFT JOIN incarceration_sentences
USING(BS_DOC, BS_CYC, BS_SEO)
LEFT JOIN supervision_sentences 
USING(BS_DOC, BS_CYC, BS_SEO)
JOIN earliest_status_code
USING(BS_DOC, BS_CYC, BS_SEO)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mo",
    ingest_view_name="sentence",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
