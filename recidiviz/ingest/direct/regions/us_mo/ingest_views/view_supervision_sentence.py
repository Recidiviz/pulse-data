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
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

BOND_STATUSES = (
    "05I5400",  # New Bond Investigation
    "15I3000",  # New PreTrial Bond Supervision
    "25I3000",  # PreTrial Bond Supv-Addl Charge
    "25I5400",  # Bond Investigation-Addl Charge
    "35I3000",  # PreTrial Bond Supv-Revisit
    "35I3500",  # Bond Supv-Pb Suspended-Revisit
    "35I5400",  # Bond Investigation-Revisit
    "35N5450",  # Bond Invest-Vio Field Supv
    "45O5999",  # Inv/Bnd Sup Complete- To DAI
    "45O59ZZ",  # Inv/Bnd Sup to DAI MUST VERIFY
    "60I4010",  # Inmate Bond Return
    "60L4600",  # Bond Return-Papers Only
    "60N4000",  # Bond Release-Assigned to Inst
    "60N4020",  # Bond Revoked to Institution
    "60O4010",  # Inmate Bond Release
    "95O3000",  # Bond Supervision Completion
    "95O3005",  # Bond Supervision-Bond Forfeit
    "95O30ZZ",  # Bond Supv Comp  MUST VERIFY
    "95O3100",  # Bond Supv Term-Technical
    "95O3105",  # Bond Supv Term-New Fel Conv
    "95O3110",  # Bond Supv Term-New Misd Conv
    "95O3115",  # Bond Supv Term-Fel Law Viol
    "95O3120",  # Bond Supv Term-Misd Law Viol
    "95O3500",  # Bond Supv-Pb Susp-Completion
    "95O3505",  # Bond Supv-Pb Susp-Bond Forfeit
    "95O35ZZ",  # Bond Supv-Pb Susp-MUST VERIFY
    "95O3600",  # Bond Supv-Pb Susp-Trm-Tech
    "95O3605",  # Bond Supv-PB Susp-Trm-Fel Conv
    "95O3610",  # Bond Supv-PB Susp-Trm-Mis Conv
    "95O3615",  # Bond Supv-PB Susp-Trm-Fel Vio
    "95O3620",  # Bond Supv-PB Susp-Trm-Misd Vio
    "95O5400",  # Bond Investigation Closed
    "95O5405",  # Bond Invest-No Charge
    "95O54ZZ",  # Bond Invest Closed-MUST VERIFY
    "95O7120",  # DATA ERROR-Bond Forfeiture
    "99O3000",  # PreTrial Bond Supv Discharge
    "99O3100",  # PreTrial Bond-Close Interest
    "99O3130",  # Bond Supv-No Further Action
    "99O5405",  # Bond Invest-No Charge
    "99O7120",  # DATA ERROR-Bond Forfeiture
)


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
FROM
    {{LBAKRDTA_TAK024}} AS BU
JOIN
    {{LBAKRDTA_TAK025}} AS BV
ON
    BU.BU_DOC = BV.BV_DOC AND
    BU.BU_CYC = BV.BV_CYC AND
    BU.BU_SEO = BV.BV_SEO AND
    BU.BU_FSO = BV.BV_FSO
JOIN
    {{LBAKRDTA_TAK026}} AS BW
ON
    BV.BV_DOC = BW.BW_DOC AND
    BV.BV_CYC = BW.BW_CYC AND
    BV.BV_SSO = BW.BW_SSO
WHERE
    -- sentence must have valid imposed_date
    BU.BU_SF NOT IN (   
        '0', 
        '19000000',
        '20000000',
        '66666666',
        '77777777',
        '88888888',
        '99999999'
    )
-- sentences with only pre-trial investigation statuses get dropped
AND NOT (
  -- Investigation start code prefixes
  BW_SCD LIKE '05I5%' OR
  BW_SCD LIKE '10I5%' OR
  BW_SCD LIKE '25I5%' OR
  BW_SCD LIKE '30I5%' OR
  BW_SCD LIKE '35I5%' OR
  -- Investigation end code prefixes
  BW_SCD LIKE '95O5%' OR
  BW_SCD LIKE '99O5%'
) 
-- sentences with only pre-trial bond statuses get dropped
AND (
    BW_SCD NOT IN {BOND_STATUSES}
)
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
    order_by_cols="BS_DOC, BS_CYC, BS_SEO",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
