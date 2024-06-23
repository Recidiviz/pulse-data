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
"""Query produces a view for supervision sentence information known at imposition.
This includes:
  - Charge information
  - Type of sentence (probation, parole, etc.), decided by status on imposition.
  - Static information that won't change over the lifetime of a sentence.

Raw data files include:
  - LBAKRDTA_TAK022 has the base information for the charges
  - LBAKRDTA_TAK024 has detailed information for supervision sentences
  - LBAKRDTA_TAK025 links TAK024 to TAK026 via _FSO
  - LBAKRDTA_TAK026 has status codes.

We do not ingest sentences that only have pre-trial status codes!
This includes pre-trial investigation and pre-trial bonds.
"""
# TODO(#30803) Remove this view when new implementation is complete
from recidiviz.ingest.direct.regions.us_mo.ingest_views.templates_sentences import (
    FROM_BU_BS_BV_BW_WHERE_NOT_PRETRIAL,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CHARGE_INFO = """
SELECT
    BS_DOC, -- unique for each person
    BS_CYC, -- unique for each sentence group
    BS_SEO, -- unique for each charge
    BS_CNS, -- sentence county_code,
    BS_NCI, -- charge ncic_code,
    BS_ASO, -- charge statute,
    BS_CLT, -- charge classification_type,
    BS_CNT, -- charge county_code,
    BS_CLA, -- charge classification_subtype,
    BS_DO,  -- charge offense_date,
    BS_COD, -- charge description,
    BS_CRC  -- charge judicial_district_code
FROM
    {LBAKRDTA_TAK022} 
"""

SUPERVISION_SENTENCE = f"""
SELECT
    BU.BU_DOC, -- unique for each person
    BU.BU_CYC, -- unique for each sentence group
    BU.BU_SEO, -- unique for each charge
    BU.BU_SF,  -- imposition date
    BW.BW_SCD AS imposition_status
{FROM_BU_BS_BV_BW_WHERE_NOT_PRETRIAL}
QUALIFY
    -- Keep the first status (at imposition) to create StateSentenceType and ensure the correct imposition date
    (ROW_NUMBER() OVER(PARTITION BY BU_DOC, BU_CYC, BU_SEO ORDER BY CAST(BV_SSO AS INT64)) = 1)
"""

VIEW_QUERY_TEMPLATE = f"""
WITH
    charge_info AS ({CHARGE_INFO}),
    supervision_sentence_info AS ({SUPERVISION_SENTENCE})
SELECT DISTINCT
    charge_info.BS_DOC,                 -- unique for each person
    charge_info.BS_CYC,                 -- unique for each sentence group
    charge_info.BS_SEO,                 -- unique for each charge
    supervision_sentence_info.BU_SF,    -- sentence imposed_date
    supervision_sentence_info.imposition_status,
    FH.FH_SDE,                          -- status code description
    charge_info.BS_CNS,                 -- sentence county_code
    charge_info.BS_NCI,                 -- charge ncic_code
    charge_info.BS_ASO,                 -- charge statute
    charge_info.BS_CLT,                 -- charge classification_type
    charge_info.BS_CNT,                 -- charge county_code
    charge_info.BS_CLA,                 -- charge classification_subtype
    charge_info.BS_DO,                  -- charge offense_date
    charge_info.BS_COD,                 -- charge description
    charge_info.BS_CRC                  -- charge judicial_district_code
FROM 
    charge_info
JOIN 
    supervision_sentence_info
ON 
    charge_info.BS_DOC = supervision_sentence_info.BU_DOC AND 
    charge_info.BS_CYC = supervision_sentence_info.BU_CYC AND 
    charge_info.BS_SEO = supervision_sentence_info.BU_SEO
JOIN
    {{LBAKRCOD_TAK146}} AS FH
ON
    supervision_sentence_info.imposition_status = FH.FH_SCD
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mo",
    ingest_view_name="supervision_sentence",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
