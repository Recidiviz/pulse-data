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

# TODO(#26621): Include data for what was StateSupervisionSentence
VIEW_QUERY_TEMPLATE = f"""
WITH
    base_sentence_info AS ({BASE_SENTENCE_INFO}),
    incarceration_sentence_detail_info AS ({INCARCERATION_SENTENCE_DETAIL_INFO})
SELECT
    base_sentence_info.BS_DOC,                 -- unique for each person
    base_sentence_info.BS_CYC,                 -- unique for each sentence group
    base_sentence_info.BS_SEO,                 -- unique for each sentence
    base_sentence_info.BS_CCI,                 -- sentence con_ind
    base_sentence_info.BS_CRQ,                 -- sentence con_xref
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
    incarceration_sentence_detail_info.BT_SDI -- sentence is_capital_punishment
FROM base_sentence_info
LEFT JOIN incarceration_sentence_detail_info 
       ON base_sentence_info.BS_DOC = incarceration_sentence_detail_info.BT_DOC
      AND base_sentence_info.BS_CYC = incarceration_sentence_detail_info.BT_CYC
      AND base_sentence_info.BS_SEO = incarceration_sentence_detail_info.BT_SEO
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mo",
    ingest_view_name="sentence",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="BS_DOC, BS_CYC, BS_SEO",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
