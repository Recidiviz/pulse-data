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

# Filters out pretrial and future status codes from {{LBAKRDTA_TAK026}}
STATUS_CODE_FILTERS = f"""
-- sentences with only pre-trial investigation statuses get dropped
NOT (
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
AND (
    -- We get weekly data with expected statuses for the (future) week,
    -- so this ensures we only get statuses that have happened
    CAST(BW_SY AS INT64) <= CAST(FORMAT_DATE('%Y%m%d', CURRENT_TIMESTAMP()) AS INT64)
)
"""

# Used to query supervision sentences that are not
# pretrial bond or investigation (decided by status).
# Available tables are aliased as BU, BV, and BW -
# referring to their respective column prefix (e.g. BU_DOC)
FROM_BU_BV_BW_WHERE_NOT_PRETRIAL = f"""
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
AND {STATUS_CODE_FILTERS}
"""
