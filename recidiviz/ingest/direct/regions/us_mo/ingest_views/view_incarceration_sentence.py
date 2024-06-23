# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
  - Type of sentence (state prison, parole, etc.)
  - Static information that won't change over the lifetime of a sentence.

Raw data files include:
  - LBAKRDTA_TAK022 has the base information for incarceration sentences
  - LBAKRDTA_TAK023 has detailed infor for incarceration sentences
"""
# TODO(#30803) Remove this view when new implementation is complete
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

BASE_SENTENCE_INFO = """
SELECT
    base_sentence.BS_DOC, -- unique for each person
    base_sentence.BS_CYC, -- unique for each sentence group
    base_sentence.BS_SEO, -- unique for each sentence
    base_sentence.BS_CCI, -- sentence con_ind
    base_sentence.BS_CRQ, -- sentence con_xref
    base_sentence.BS_CNS, -- sentence county_code,
    base_sentence.BS_NCI, -- charge ncic_code,
    base_sentence.BS_ASO, -- charge statute,
    base_sentence.BS_CLT, -- charge classification_type,
    base_sentence.BS_CNT, -- charge county_code,
    base_sentence.BS_CLA, -- charge classification_subtype,
    base_sentence.BS_DO,  -- charge offense_date,
    base_sentence.BS_COD, -- charge description,
    base_sentence.BS_CRC  -- charge judicial_district_code
FROM
    {LBAKRDTA_TAK022} AS base_sentence
"""

INCARCERATION_SENTENCE_DETAIL_INFO = """
SELECT
    sentence_detail.BT_DOC, -- unique for each person
    sentence_detail.BT_CYC, -- unique for each sentence group
    sentence_detail.BT_SEO, -- unique for each sentence
    sentence_detail.BT_SD,  -- sentence imposed_date
    sentence_detail.BT_CRR, -- sentence is_life
    sentence_detail.BT_SDI  -- sentence is_capital_punishment
FROM
    {LBAKRDTA_TAK023} AS sentence_detail
WHERE
    sentence_detail.BT_SD not in (   
        '0', 
        '19000000',
        '20000000',
        '66666666',
        '77777777',
        '88888888',
        '99999999'
    )
"""

VIEW_QUERY_TEMPLATE = f"""
WITH consecutive_parent_sentences AS (
    SELECT
        BS_DOC, -- unique for each person
        BS_CYC, -- unique for each sentence group
        BS_SEO, -- unique for each sentence
        concat(BS_DOC, '-', BS_CYC, '-', BS_CRQ, '-', 'INCARCERATION') AS parent_sentence_external_id_array,
    FROM 
        {{LBAKRDTA_TAK022}} AS _consecutive
    WHERE
        BS_CCI = 'CS'
),
concurrent_parent_sentences AS (
    SELECT
        _concurrent.BS_DOC,
        _concurrent.BS_CYC,
        _concurrent.BS_SEO,
        -- we want the consecutive sentence that this sentence is concurrent with
        consecutive_parent_sentences.parent_sentence_external_id_array
    FROM 
        {{LBAKRDTA_TAK022}} AS _concurrent
    JOIN
        consecutive_parent_sentences   
    ON
        _concurrent.BS_DOC = consecutive_parent_sentences.BS_DOC AND
        _concurrent.BS_CYC = consecutive_parent_sentences.BS_CYC AND
        _concurrent.BS_CRQ = consecutive_parent_sentences.BS_SEO
    WHERE
        _concurrent.BS_CCI = 'CC'
),
parent_sentence_arrays AS (
    SELECT * FROM consecutive_parent_sentences 
    UNION ALL
    SELECT * FROM concurrent_parent_sentences 
),
base_sentence_info AS ({BASE_SENTENCE_INFO}),
incarceration_sentence_detail_info AS ({INCARCERATION_SENTENCE_DETAIL_INFO})
SELECT
    base_sentence_info.BS_DOC,                 -- unique for each person
    base_sentence_info.BS_CYC,                 -- unique for each sentence group
    base_sentence_info.BS_SEO,                 -- unique for each sentence
    base_sentence_info.BS_CNS,                 -- sentence county_code,
    base_sentence_info.BS_NCI,                 -- charge ncic_code,
    base_sentence_info.BS_ASO,                 -- charge statute,
    base_sentence_info.BS_CLT,                 -- charge classification_type,
    base_sentence_info.BS_CNT,                 -- charge county_code,
    base_sentence_info.BS_CLA,                 -- charge classification_subtype,
    base_sentence_info.BS_DO,                  -- charge offense_date,
    base_sentence_info.BS_COD,                 -- charge description,
    base_sentence_info.BS_CRC,                 -- charge judicial_district_code
    incarceration_sentence_detail_info.BT_SD,  -- sentence imposed_date
    incarceration_sentence_detail_info.BT_CRR, -- sentence is_life
    incarceration_sentence_detail_info.BT_SDI, -- sentence is_capital_punishment
    parent_sentence_arrays.parent_sentence_external_id_array
FROM base_sentence_info
JOIN 
    incarceration_sentence_detail_info 
ON 
    base_sentence_info.BS_DOC = incarceration_sentence_detail_info.BT_DOC AND 
    base_sentence_info.BS_CYC = incarceration_sentence_detail_info.BT_CYC AND 
    base_sentence_info.BS_SEO = incarceration_sentence_detail_info.BT_SEO
LEFT JOIN
    parent_sentence_arrays
ON 
    base_sentence_info.BS_DOC = parent_sentence_arrays.BS_DOC AND 
    base_sentence_info.BS_CYC = parent_sentence_arrays.BS_CYC AND 
    base_sentence_info.BS_SEO = parent_sentence_arrays.BS_SEO
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mo",
    ingest_view_name="incarceration_sentence",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
