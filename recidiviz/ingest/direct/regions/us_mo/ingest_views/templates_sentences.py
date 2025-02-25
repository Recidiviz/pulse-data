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
"""This module has reusable strings for various sentencing queries."""

from recidiviz.ingest.direct.regions.us_mo.us_mo_custom_parsers import MAGIC_DATES

INVALID_IMPOSED_DATE = (
    "0",
    "19000000",
    "20000000",
    "66666666",
    "77777777",
    "99999999",
)


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


# These IS Comp statuses are not included,
# because we use INTERSTATE_COMPACT_STATUSES
# to attempt to derive if the sentence originated
# in another state. So we exclude these investigation
# statuses (we don't keep any investagation status)
# and the following statuses related to MODOC starting
# an interstate compact
#     - "05I5210", # IS Comp-Reporting Instr Given
#     - "05I5220", # IS Comp-Invest-Unsup/Priv Prob
#     - "05I5230", # IS Comp-Rept Ins-Unsup/Priv PB
#     - "40N1070", # Parole To IS Compact Sent-Inst
#     - "40N3070", # CR To IS Compact Sent-Inst
#     - "40N8070", # Adm Par To IS Compact Sen-Inst
#     - "95O5210",  # IS Comp-Report Instruct Closed
#     - "95O5220",  # IS Comp-Inv-Unsup/Priv PB-Clos
#     - "95O5230",  # IS Comp-RepIns-Uns/Prv PB-Clos
INTERSTATE_COMPACT_STATUSES = (
    "20I4000",  # IS Compact-Inst-Addl Charge
    "25I5200",  # IS Compact-Invest-Addl Charge
    "25I5210",  # IS Comp-Rep Instr Giv-Addl Chg
    "25I5220",  # IS Comp-Inv-Unsup/Priv PB-AC
    "25I5230",  # IS Comp-Rep Ins-Uns/Priv PB-AC
    "30I4000",  # IS Compact-Institution-Revisit
    "35I4000",  # IS Compact-Prob-Revisit
    "35I4010",  # IS Comp-Unsup/Priv PB-Revisit
    "35I4100",  # IS Compact-Parole-Revisit
    "35I5200",  # IS Compact-Invest-Revisit
    "35I5210",  # IS Comp-Rep Instr Giv-Revisit
    "35I5220",  # IS Comp-Inv-Unsup/Priv PB-Rev
    "35I5230",  # IS Comp-Rep Ins-Uns/Prv PB-Rev
    "40I4270",  # IS Compact-Parole-CRC Work Rel
    "40I7700",  # IS Compact-Erroneous Commit
    "40O4270",  # IS Compact-Parole-Rel From CRC
    "40O7400",  # IS Compact Parole to Missouri
    "40O7700",  # IS Compact-Err Commit-Release
    "45O4270",  # IS Compact-Parole-CRC Work Rel
    "45O42ZZ",  # IS Compact-Parole- MUST VERIFY
    "45O7700",  # IS Compact-Erroneous Commit
    "55N4000",  # IS Compact-Prob-Extension
    "55N4010",  # IS Comp-Unsup/Prv PB-Extension
    "55N4100",  # IS Compact-Parole-Extension
    "95O4000",  # IS Compact-Prob Completion
    "95O4010",  # IS Compact-Prob Return/Tran
    "95O4020",  # IS Compact-Probation Revoked
    "95O4030",  # IS Comp-Unsup/Priv Prob Comp
    "95O4040",  # IS Comp-Unsup/Priv PB Ret/Tran
    "95O4050",  # IS Comp-Unsup/Priv Prob Rev
    "95O4100",  # IS Compact-Parole Completion
    "95O4110",  # IS Compact-Parole Ret/Tran
    "95O4120",  # IS Compact-Parole Revoked
    "99O4000",  # IS Compact-Prob Discharge
    "99O4010",  # IS Compact-Prob Return/Tran
    "99O4020",  # IS Compact-Probation Revoked
    "99O4030",  # IS Comp-Unsup/Priv Prob-Disc
    "99O4040",  # IS Comp-Unsup/Priv PB-Ret/Tran
    "99O4050",  # IS Comp-Unsup/Priv Prob-Rev
    "99O4100",  # IS Compact-Parole Discharge
    "99O4110",  # IS Compact-Parole Ret/Tran
    "99O4120",  # IS Compact-Parole Revoked
)


BS_BT_BU_IMPOSITION_FILTER = f"""
    (BU_SF NOT IN {INVALID_IMPOSED_DATE} AND BU_SF IS NOT NULL)
OR
    (BT_SD NOT IN {INVALID_IMPOSED_DATE} AND BT_SD IS NOT NULL)
OR
    -- OTST is how MODOC designates 'other state'
    BS_CNS = 'OTST'
"""

# We take the earliest (by FSO) non-investigation,
# non-bond, valid supervision data.
# An observation from this view does not necessarily mean
# an individual was *sentenced* to supervision.
VALID_SUPERVISION_SENTENCE_INITIAL_INFO = f"""
SELECT
    BU_DOC, -- unique for each person
    BU_CYC, -- unique for each sentence group
    BU_SEO, -- unique for each sentence
    BU_SF  -- sentence imposed_date
FROM
    {{LBAKRDTA_TAK024}}
WHERE 
    BU_PBT NOT IN ('INV', 'BND')
AND 
    BU_SF NOT IN {INVALID_IMPOSED_DATE}
QUALIFY (
    ROW_NUMBER() OVER(
        PARTITION BY BU_DOC, BU_CYC, BU_SEO 
        ORDER BY CAST(BU_FSO AS INT64)
    ) = 1
)
"""


# Interstate Compact sentence probably has imposed_date = '88888888',
# and we allow that to be null downstream as the sentencing_authority
# will be OTHER_STATE.
# All other sentences must have a valid imposed date
VALID_IMPOSITON = f"""
(
    BU.BU_SF NOT IN {MAGIC_DATES} 
OR 
    BS_CNS = 'OTST' 
OR 
    BW_SCD IN {INTERSTATE_COMPACT_STATUSES}
)
"""

IS_PRETRIAL_OR_BOND_STATUS = f"""
    -- Investigation start code prefixes
    BW_SCD LIKE '10I5%' OR
    BW_SCD LIKE '30I5%' OR
    BW_SCD LIKE '%5I5%' OR
    -- Investigation end code prefixes
    BW_SCD LIKE '%5O5%' OR
    BW_SCD LIKE '99O5%' OR
    BW_SCD IN {BOND_STATUSES}
"""

VALID_STATUS_CODES = f"""
SELECT
    BS_DOC,
    BS_CYC,
    BS_SEO,
    BV_FSO,
    BW_SSO,
    BW_SY,
    BW_SCD,
    FH_SDE
FROM
    {{LBAKRDTA_TAK022}} AS BS
JOIN
    {{LBAKRDTA_TAK025}} AS BV
ON
    BS.BS_DOC = BV.BV_DOC AND
    BS.BS_CYC = BV.BV_CYC AND
    BS.BS_SEO = BV.BV_SEO
JOIN
    {{LBAKRDTA_TAK026}} AS BW
ON
    BV.BV_DOC = BW.BW_DOC AND
    BV.BV_CYC = BW.BW_CYC AND
    BV.BV_SSO = BW.BW_SSO
LEFT OUTER JOIN
    {{LBAKRCOD_TAK146}} AS FH
ON
    FH.FH_SCD = BW.BW_SCD
WHERE
    -- We get weekly data with expected statuses for the (future) week,
    -- so this ensures we only get statuses that have happened
    CAST(BW_SY AS INT64) <= CAST(FORMAT_DATE('%Y%m%d', CURRENT_TIMESTAMP()) AS INT64)
-- Get every status after the first non-pretrial non-bond status code
QUALIFY 
    BW_SY >= MIN(
        CASE WHEN {IS_PRETRIAL_OR_BOND_STATUS}
        THEN NULL ELSE BW_SY END
    ) OVER (PARTITION BY BS_DOC, BS_CYC, BS_SEO)
AND
    BW_SSO >= MIN(
        CASE WHEN {IS_PRETRIAL_OR_BOND_STATUS}
        THEN NULL ELSE BW_SSO END
    ) OVER (PARTITION BY BS_DOC, BS_CYC, BS_SEO)
"""
