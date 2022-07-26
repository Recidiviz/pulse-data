# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Custom enum parsers functions for US_MO. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_mo_custom_enum_parsers.<function name>
"""
import re
from typing import Callable, Dict, List, Optional, Set

from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.str_field_utils import sorted_list_from_str
from recidiviz.ingest.direct.legacy_ingest_mappings.direct_ingest_controller_utils import (
    invert_enum_to_str_mappings,
)

TAK026_STATUS_CYCLE_TERMINATION_REGEX = re.compile(r"99O\d{4}")
TAK026_STATUS_SUPERVISION_SENTENCE_COMPLETION_REGEX = re.compile(r"95O\d{4}")
TAK026_STATUS_SUPERVISION_PERIOD_TERMINATION_REGEX = re.compile(r"\d5O\d{4}")
TAK026_STATUS_SUPERVISION_PERIOD_START_REGEX = re.compile(r"\d5I\d{4}")


INVESTIGATION_START_STATUSES: Set[str] = {
    # All 05I5* Investigation start statuses
    "05I5000",  # New Pre-Sentence Investigation
    "05I5100",  # New Community Corr Court Ref
    "05I5200",  # New Interstate Compact-Invest
    "05I5210",  # IS Comp-Reporting Instr Given
    "05I5220",  # IS Comp-Invest-Unsup/Priv Prob
    "05I5230",  # IS Comp-Rept Ins-Unsup/Priv PB
    "05I5300",  # New Exec Clemency-Invest
    "05I5400",  # New Bond Investigation
    "05I5500",  # New Diversion Investigation
    "05I5600",  # New Sentencing Assessment
    # All 25I5* Investigation Additional Charge statuses
    "25I5000",  # PSI-Addl Charge
    "25I5100",  # Comm Corr Crt Ref-Addl Charge
    "25I5200",  # IS Compact-Invest-Addl Charge
    "25I5210",  # IS Comp-Rep Instr Giv-Addl Chg
    "25I5220",  # IS Comp-Inv-Unsup/Priv PB-AC
    "25I5230",  # IS Comp-Rep Ins-Uns/Priv PB-AC
    "25I5300",  # Exec Clemency-Invest-Addl Chg
    "25I5400",  # Bond Investigation-Addl Charge
    "25I5500",  # Diversion Invest-Addl Charge
    # All 35I5* Investigation Additional Charge statuses
    "35I5000",  # PSI-Revisit
    "35I5100",  # Comm Corr Crt Ref-Revisit
    "35I5200",  # IS Compact-Invest-Revisit
    "35I5210",  # IS Comp-Rep Instr Giv-Revisit
    "35I5220",  # IS Comp-Inv-Unsup/Priv PB-Rev
    "35I5230",  # IS Comp-Rep Ins-Uns/Prv PB-Rev
    "35I5400",  # Bond Investigation-Revisit
    "35I5500",  # Diversion Invest-Revisit
    "35I5600",  # Sentencing Assessment-Revisit
}

SUPERVISION_PERIOD_TERMINATION_REASON_TO_STR_MAPPINGS: Dict[
    StateSupervisionPeriodTerminationReason, List[str]
] = {
    StateSupervisionPeriodTerminationReason.ABSCONSION: [
        "65O1010",  # Offender declared absconder - from TAK026 BW$SCD
        "65O1020",  # Offender declared absconder - from TAK026 BW$SCD
        "65O1030",  # Offender declared absconder - from TAK026 BW$SCD
        "65L9100",  # Offender declared absconder - from TAK026 BW$SCD
    ],
    StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION: [
        "65N9500",  # Offender re-engaged - from TAK026 BW$SCD
    ],
    StateSupervisionPeriodTerminationReason.DEATH: [
        "99O9020",  # Suicide-Institution
        "99O9025",  # Suicide-Inst-Off Premises
        "99O9030",  # Accidental Death-Institution
        "99O9035",  # Accid Death-Inst-Off Premises
        "99O9040",  # Offense Related Death-Instit
        "99O9045",  # Offense Rel Death-Inst-Off Prm
        "99O9050",  # Natural Death-Institution
        "99O9055",  # Natural Death-Inst-Off Premise
        "99O9060",  # Death-Unknown Causes-Instit
        "99O9065",  # Death-Unk Cause-Inst-Off Prem
        "99O9070",  # Death Under Supervision-Field
        "99O9080",  # Death While Off DAI Premises
        "99O9520",  # Suicide-Field
        "99O9530",  # Accidental Death-Field
        "99O9540",  # Offense Related Death-Field
        "99O9550",  # Natural Death-Field
        "99O9560",  # Death-Unknown Causes-Field
        "99O9999",  # Execution
    ],
    StateSupervisionPeriodTerminationReason.DISCHARGE: [
        "99O0000",  # Converted Inactive-Institution
        "99O0010",  # Converted Off Records-Field
        "99O0020",  # Converted Revoked-Field
        "99O0989",  # Erroneous Commitment-Field
        "99O0999",  # Erroneous Commit-Institution
        "99O1000",  # Court Probation Discharge
        "99O1001",  # Court Probation ECC Discharge
        "99O1010",  # Court Prob Disc-CONFIDENTIAL
        "99O1011",  # Ct Prob ECC Disc-CONFIDENTIAL
        "99O1015",  # Court Prob-No Further Action
        "99O1016",  # No Further Action - Closed
        "99O1020",  # Institutional Commutation
        "99O1025",  # Field Commutation
        "99O1026",  # CONFIDENTIAL CLOSED-P&P
        "99O1027",  # Rev/Disch-DAI RETIRED
        "99O1030",  # Institutional Pardon
        "99O1035",  # Field Pardon
        "99O1040",  # Resentenced-Field Completion
        "99O1050",  # Reverse/Remand Discharge-DAI
        "99O1051",  # Reverse/Remand Discharge-P&P
        "99O1055",  # Court Ordered Disc-Institution
        "99O1060",  # Director's Discharge
        "99O1065",  # Director's Discharge-Field
        "99O1070",  # Director's Disc To Custody/Det
        "99O1080",  # CONFIDENTIAL CLOSED-DAI
        "99O1200",  # Court Parole Discharge
        "99O1210",  # Court Parole Revoked-Local
        "99O2000",  # Diversion Disc-CONFIDENTIAL
        "99O2005",  # Div-Term Services-CONFIDENTIAL
        "99O2010",  # Parole Discharge
        "99O2011",  # Parole Discharge-Institution
        "99O2012",  # Parole ECC Discharge
        "99O2013",  # Parole ECC Disc-Institution
        "99O2015",  # Parole Discharge-Admin
        "99O2020",  # Conditional Release Discharge
        "99O2021",  # CR Discharge-Institution
        "99O2022",  # Cond Release ECC Discharge
        "99O2023",  # Cond Rel ECC Disc-Institution
        "99O2025",  # CR Discharge-Admin
        "99O2030",  # Disc-Escape-Inst(Comment-ICOM)
        "99O2035",  # Disc-Absc-Field (Comment-POTR)
        "99O2040",  # Administrative Parole Disc
        "99O2041",  # Admin Parole Disc-Institution
        "99O2042",  # Administrative Parole ECC Disc
        "99O2043",  # Admin Parole ECC Disc-Inst
        "99O2045",  # Admin Parole Disc-Admin
        "99O2050",  # Inmate Field Discharge
        "99O2100",  # Prob Rev-Technical-Jail
        "99O2105",  # Prob Rev-New Felony Conv-Jail
        "99O2110",  # Prob Rev-New Misd Conv-Jail
        "99O2115",  # Prob Rev-Felony Law Viol-Jail
        "99O2120",  # Prob Rev-Codes Not Applicable
        "99O2215",  # Parole Disc-Retroactive
        "99O2225",  # CR Discharge-Retroactive
        "99O2245",  # Admin Parole Disc-Retroactive
        "99O3000",  # PreTrial Bond Supv Discharge
        "99O3100",  # PreTrial Bond-Close Interest
        "99O3130",  # Bond Supv-No Further Action
        "99O4000",  # IS Compact-Prob Discharge
        "99O4010",  # IS Compact-Prob Return/Tran
        "99O4020",  # IS Compact-Probation Revoked
        "99O4030",  # IS Comp-Unsup/Priv Prob-Disc
        "99O4040",  # IS Comp-Unsup/Priv PB-Ret/Tran
        "99O4050",  # IS Comp-Unsup/Priv Prob-Rev
        "99O4100",  # IS Compact-Parole Discharge
        "99O4110",  # IS Compact-Parole Ret/Tran
        "99O4120",  # IS Compact-Parole Revoked
        "99O4200",  # Discharge-Interstate Compact
        "99O4210",  # Interstate Compact Return
        "99O5005",  # PSI Other Disposition
        "99O5010",  # PSI Probation Denied-Other
        "99O5015",  # PSI Plea Withdrawn
        "99O5020",  # PSI Probation Denied-Jail
        "99O5030",  # PSI Cancelled by Court
        "99O5099",  # Investigation Close Interest
        "99O5300",  # Executive Clemency Denied
        "99O5305",  # Executive Clemency Granted
        "99O5310",  # Executive Clemency Inv Comp.
        "99O5405",  # Bond Invest-No Charge
        "99O5500",  # Diversion Denied
        "99O5605",  # SAR Other Disposition
        "99O5610",  # SAR Probation Denied-Other
        "99O5615",  # SAR Plea Withdrawn
        "99O5620",  # SAR Probation Denied-Jail
        "99O5630",  # SAR Cancelled by Court
        "99O6000",  # Discharge-Cell Leasing
        "99O7000",  # Relieved of Supv-Court Disc
    ],
    StateSupervisionPeriodTerminationReason.EXTERNAL_UNKNOWN: [],
    StateSupervisionPeriodTerminationReason.REVOCATION: [
        # TODO(#10498): Consider reclassifying some of these as a status that indicates
        # that it's a return but not a revocation
        "45O0ZZZ",  # Board Holdover     'MUST VERIFY'
        "45O0010",  # Emergency Board RF Housing
        "45O0050",  # Board Holdover
        "45O1ZZZ",  # Parole Return      'MUST VERIFY'
        "45O1010",  # Parole Ret-Tech Viol
        "45O1020",  # Parole Ret-New Felony-Viol
        "45O1021",  # Parole Ret-No Violation
        "45O1050",  # Parole Viol-Felony Law Viol
        "45O1055",  # Parole Viol-Misd Law Viol
        "45O1060",  # Parole Ret-Treatment Center
        "45O1070",  # Parole Return-Work Release
        "45O2ZZZ",  # Probation Revoked  'MUST VERIFY'
        "45O2000",  # Prob Rev-Technical
        "45O2005",  # Prob Rev-New Felony Conv
        "45O2010",  # Prob Rev-New Misd Conv
        "45O2015",  # Prob Rev-Felony Law Viol
        "45O2020",  # Prob Rev-Misd Law Viol
        "45O3ZZZ",  # CR Return          'MUST VERIFY'
        "45O3010",  # CR Ret-Tech Viol
        "45O3020",  # CR Ret-New Felony-Viol
        "45O3021",  # CR Ret-No Violation
        "45O3050",  # CR Viol-Felony Law Viol
        "45O3055",  # CR Viol-Misd Law Viol
        "45O3060",  # CR Ret-Treatment Center
        "45O3070",  # CR Return-Work Release
        "45O40ZZ",  # Resid Fac Return   'MUST VERIFY'
        "45O4010",  # Emergency Inmate RF Housing
        "45O4030",  # RF Return-Administrative
        "45O4035",  # RF Return-Treatment Center
        "45O4040",  # RF Return-Technical
        "45O4045",  # RF Return-New Felony Conv
        "45O4050",  # RF Return-New Misd Conv
        "45O41ZZ",  # EMP Return         'MUST VERIFY'
        "45O4130",  # EMP Return-Administrative
        "45O4135",  # EMP Return-Treatment Center
        "45O4140",  # EMP Return-Technical
        "45O4145",  # EMP Return-New Felony Conv
        "45O4150",  # EMP Return-New Misd Conv
        "45O42ZZ",  # IS Compact-Parole- 'MUST VERIFY'
        "45O4270",  # IS Compact-Parole-CRC Work Rel
        "45O490Z",  # CR Deferred Return 'MUST VERIFY'
        "45O4900",  # CR Deferred Return
        "45O4999",  # Inmate Return From EMP/RF
        "45O50ZZ",  # PSI-Prob Denied    'MUST VERIFY'
        "45O5000",  # PSI Probation Denied-DAI
        "45O51ZZ",  # Pre-Sentence Comm  'MUST VERIFY'
        "45O5100",  # Pre-Sent Assess Commit to DAI
        "45O56ZZ",  # SAR-Prob Denied    'MUST VERIFY'
        "45O5600",  # SAR Probation Denied-DAI
        "45O57ZZ",  # Resentenced-No Rev 'MUST VERIFY'
        "45O5700",  # Resentenced-No Revocation
        "45O59ZZ",  # Inv/Bnd Sup to DAI 'MUST VERIFY'
        "45O5999",  # Inv/Bnd Sup Complete- To DAI
        "45O700Z",  # To DAI-Other Sent  'MUST VERIFY'
        "45O7000",  # Field to DAI-Other Sentence
        "45O7001",  # Field Supv to DAI-Same Offense
        "45O701Z",  # Prob to DAI-Shock  'MUST VERIFY'
        "45O7010",  # Prob-DAI-Shock Incarceration
        "45O702Z",  # Prob-Ct Order Det--'MUST VERIFY'
        "45O7020",  # Prob-Ct Order Detention Sanctn
        "45O703Z",  # Prob-MH 120 Day--'MUST VERIFY'
        "45O7030",  # Prob-Mental Health 120 Day
        "45O706Z",  # Prob-Post Conv-Trt 'MUST VERIFY'
        "45O7060",  # Prob-Post Conv-Trt Pgm
        "45O7065",  # Prob-Post Conv-RDP
        "45O77ZZ",  # IS Cmpct-Err Commt 'MUST VERIFY'
        "45O7700",  # IS Compact-Erroneous Commit
        "45O7999",  # Err Release-P&P Return to DAI
        "45O8ZZZ",  # Admin Return       'MUST VERIFY'
        "45O8010",  # Adm Ret-Tech Viol
        "45O8020",  # Adm Ret-New Felony-Viol
        "45O8021",  # Adm Return-No Violation
        "45O8050",  # Adm Viol-Felony Law Viol
        "45O8055",  # Adm Viol-Misd Law Viol
        "45O8060",  # Adm Ret-Treatment Center
        "45O8070",  # Admin Par Return-Work Release
        "45O9998",  # Converted-Revoke DOC-No Vio
        "45O9999",  # Revocation-Code Not Applicable
        # All Parole Revocation (40I1*) statuses from TAK026
        "40I1010",  # Parole Ret-Tech Viol
        "40I1020",  # Parole Ret-New Felony-Viol
        "40I1021",  # Parole Ret-No Violation
        "40I1025",  # Medical Parole Ret - Rescinded
        "40I1040",  # Parole Ret-OTST Decision Pend
        "40I1050",  # Parole Viol-Felony Law Viol
        "40I1055",  # Parole Viol-Misd Law Viol
        "40I1060",  # Parole Ret-Treatment Center
        "40I1070",  # Parole Return-Work Release
        # All Conditional Release Return (40I3*) statuses from TAK026
        "40I3010",  # CR Ret-Tech Viol
        "40I3020",  # CR Ret-New Felony-Viol
        "40I3021",  # CR Ret-No Violation
        "40I3040",  # CR Ret-OTST Decision Pend
        "40I3050",  # CR Viol-Felony Law Viol
        "40I3055",  # CR Viol-Misd Law Viol
        "40I3060",  # CR Ret-Treatment Center
        "40I3070",  # CR Return-Work Release
    ],
    StateSupervisionPeriodTerminationReason.SUSPENSION: [
        "65O2015",  # Court Probation Suspension
        "65O3015",  # Court Parole Suspension
    ],
    StateSupervisionPeriodTerminationReason.TRANSFER_TO_OTHER_JURISDICTION: [
        "75O3000",  # MO Field-Interstate Transfer
        "75O3010",  # MO Board-Interstate Transfer
    ],
}


SUPERVISION_PERIOD_ADMISSION_REASON_TO_STR_MAPPINGS: Dict[
    StateSupervisionPeriodAdmissionReason, List[str]
] = {
    StateSupervisionPeriodAdmissionReason.ABSCONSION: [
        "65O1010",  # Offender declared absconder - from TAK026 BW$SCD
        "65O1020",  # Offender declared absconder - from TAK026 BW$SCD
        "65O1030",  # Offender declared absconder - from TAK026 BW$SCD
        "99O2035",  # Offender declared absconder - from TAK026 BW$SCD
        "65L9100",  # Offender declared absconder - from TAK026 BW$SCD
    ],
    StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION: [
        # All 40O* statuses correspond to being released from an
        # institution to supervision.
        "40O0010",  # Emer Board RF Housing-Release
        "40O0050",  # Board Holdover Release
        "40O0055",  # Board Hold Rel To Custody/Det
        "40O1010",  # Parole Release
        "40O1015",  # Parolee Released From CRC
        "40O1020",  # Parole To Custody/Detainer
        "40O1025",  # Medical Parole Release
        "40O1030",  # Parole Re-Release
        "40O1040",  # Parole Return Rescinded
        "40O1060",  # Parolee Re-Rel From Inst Pgm
        "40O1065",  # Parolee Rel From Inst Pgm-New
        "40O1080",  # Parolee Released from Inst
        "40O2000",  # Prob Rev-Rel to Field-Spc Cir
        "40O3010",  # Conditional Release
        "40O3015",  # Conditional Released From CRC
        "40O3020",  # CR To Custody/Detainer
        "40O3030",  # Conditional Re-Release
        "40O3040",  # CR Return Rescinded
        "40O3060",  # CR Re-Release From Inst Pgm
        "40O3065",  # CR Released From Inst Pgm-New
        "40O3080",  # Cond Releasee Rel from Inst
        "40O4010",  # Emer Inmate RF Housing-Release
        "40O4099",  # Inmate Release to RF
        "40O4199",  # Inmate Release to EMP
        "40O4270",  # IS Compact-Parole-Rel From CRC
        "40O4900",  # CR Deferred-Release to Field
        "40O5000",  # Release to Field-Invest Pend
        "40O5100",  # Rel to Field-PSI Assess Comm
        "40O6000",  # Converted-CRC DAI to CRC Field
        "40O6010",  # Release for SVP Commit Hearing
        "40O6020",  # Release for Lifetime Supv
        "40O7000",  # Rel to Field-DAI Other Sent
        "40O7001",  # Rel to Field-Same Offense
        "40O7010",  # Rel to Prob-Shck Incarceration
        "40O7020",  # Rel to Prob-Ct Order Det Sanc
        "40O7030",  # Rel to Prob-MH 120 Day
        "40O7060",  # Rel to Prob-Post Conv-Trt Pgm
        "40O7065",  # Rel to Prob-Post-Conv-RDP
        "40O7400",  # IS Compact Parole to Missouri
        "40O7700",  # IS Compact-Err Commit-Release
        "40O8010",  # Admin Parole Release
        "40O8015",  # Adm Parolee Released from CRC
        "40O8020",  # Adm Parole To Custody/Detainer
        "40O8060",  # Adm Par Re-Rel From Inst Pgm
        "40O8065",  # Adm Par Rel From Inst Pgm-New
        "40O8080",  # Adm Parolee Rel from Inst
        "40O9010",  # Release to Probation
        "40O9020",  # Release to Prob-Custody/Detain
        "40O9030",  # Statutory Probation Release
        "40O9040",  # Stat Prob Rel-Custody/Detainer
        "40O9060",  # Release to Prob-Treatment Ctr
        "40O9070",  # Petition Probation Release
        "40O9080",  # Petition Prob Rel-Cust/Detain
        "40O9100",  # Petition Parole Release
        "40O9110",  # Petition Parole Rel-Cus/Detain
    ],
    StateSupervisionPeriodAdmissionReason.COURT_SENTENCE: [
        "15I1000",  # New Court Probation
        "15I1200",  # New Court Parole
        "15I2000",  # New Diversion Supervision
        "15I3000",  # New PreTrial Bond Supervision
        # These statuses are relatively rare (100ish instances cumulatively per year) and happen when someone has been
        # charged with an MO crime, but has been committed to a federal facility or a prison in a different state. The
        # person is only actually committed to a MO facility when we see a 40I6000 status. When this shows up on the
        # admission date of a supervision period, it's likely because the person has been assigned to a PO for
        # accounting purposes.
        # TODO(#2905): Write a query to figure out the spans of time someone is under CC Fed/State incarceration and
        #  generate incarceration periods for that time so we don't count these people erroneously as under supervision.
        "10L6000",  # New CC Fed/State (Papers Only)
        "20L6000",  # CC Fed/State (Papers Only)-AC
        "30L6000",  # CC Fed/State(Papers Only)-Revt
    ],
    StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION: [
        "65I1099",  # Supervision Reinstated
        "65I2015",  # Court Probation Reinstated
        "65I3015",  # Court Parole Reinstated
        "65I6010",  # Inmate Reinstated EMP / RF
    ],
    StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION: [
        "65N9500",  # Offender re-engaged - from TAK026 BW$SCD
    ],
    StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE: [
        # Since a) we rank INVESTIGATION_START_STATUSES as being the lowest priority status to parse and b) we filter
        # out all portions of supervision periods that happen before an initial investigation is over, if we pick one of
        # these statuses as the primary status, it means a new investigation happened to open up on the same day as a
        # person transferred POs and we want to still count that as a transfer.
        *INVESTIGATION_START_STATUSES
    ],
    StateSupervisionPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION: [
        "75I3000",  # MO Field-Interstate Returned
        "75I3010",  # MO Board-Interstate Returned
    ],
}


STR_TO_SUPERVISION_PERIOD_ADMISSION_REASON_MAPPINGS: Dict[
    str, StateSupervisionPeriodAdmissionReason
] = invert_enum_to_str_mappings(SUPERVISION_PERIOD_ADMISSION_REASON_TO_STR_MAPPINGS)


STR_TO_SUPERVISION_PERIOD_TERMINATION_REASON_MAPPINGS: Dict[
    str, StateSupervisionPeriodTerminationReason
] = invert_enum_to_str_mappings(SUPERVISION_PERIOD_TERMINATION_REASON_TO_STR_MAPPINGS)


def parse_supervision_period_admission_reason(
    raw_text: str,
) -> Optional[StateSupervisionPeriodAdmissionReason]:
    """Maps |raw_text|, a space delimited list of statuses from TAK026, to the most relevant
    SupervisionPeriodAdmissionReason, when possible.

    If the status list is empty, we assume that this period ended because the person transferred between POs or offices.
    """
    if not raw_text:
        raise ValueError(
            "Unexpected empty/null status list - empty values should not be passed to this mapper"
        )

    if raw_text == "TRANSFER WITHIN STATE":
        return StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE

    statuses = sorted_list_from_str(raw_text, ",")

    def status_rank(status: str) -> int:
        """In the case that there are multiple statuses on the same day, we pick the status that is most likely to
        give us accurate info about the reason this supervision period was started. In the case of supervision
        period admissions, we pick statuses that have the pattern X5I* (e.g. '15I1000'), since those statuses are
        field (5) IN (I) statuses. In the absence if one of those statuses, we get our info from other statuses.
        """
        if status not in INVESTIGATION_START_STATUSES:
            if re.match(TAK026_STATUS_SUPERVISION_PERIOD_START_REGEX, status):
                return 0

            return 1

        # Since we filter out all portions of supervision periods that happen before an initial investigation is over,
        # if we find a period that starts with one of these statuses, it means a new investigation happened to open up
        # on the same day as a person transferred POs. We generally want to ignore this case and just treat it as a
        # transfer unless there are other statuses that give us more info.
        return 2

    sorted_statuses = sorted(
        statuses, key=lambda status: _status_rank_str(status, status_rank)
    )

    for sp_admission_reason_str in sorted_statuses:
        if (
            sp_admission_reason_str
            in STR_TO_SUPERVISION_PERIOD_ADMISSION_REASON_MAPPINGS
        ):
            return STR_TO_SUPERVISION_PERIOD_ADMISSION_REASON_MAPPINGS[
                sp_admission_reason_str
            ]

    return StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN


def parse_supervision_period_termination_reason(
    raw_text: str,
) -> Optional[StateSupervisionPeriodTerminationReason]:
    """Maps |raw_text|, a space delimited list of statuses from TAK026, to the most relevant
    SupervisionPeriodTerminationReason, when possible.

    If the status list is empty, we assume that this period ended because the person transferred between POs or offices.
    """
    if not raw_text:
        return None

    if raw_text == "TRANSFER WITHIN STATE":
        return StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE

    statuses = sorted_list_from_str(raw_text, ",")

    def status_rank(status: str) -> int:
        """In the case that there are multiple statuses on the same day, we pick the status that is most likely to
        give us accurate info about the reason this supervision period was terminated. In the case of supervision
        period terminations, we pick statuses first that have the pattern 99O* (e.g. '99O9020'), since those
        statuses always end a whole offender cycle, then statuses with pattern 95O* (sentence termination), then
        finally X5O*, since those statuses are field (5) OUT (O) statuses. In the absence if one of those statuses,
        we get our info from other statuses.
        """
        if re.match(TAK026_STATUS_CYCLE_TERMINATION_REGEX, status):
            return 0
        if re.match(TAK026_STATUS_SUPERVISION_SENTENCE_COMPLETION_REGEX, status):
            return 1
        if re.match(TAK026_STATUS_SUPERVISION_PERIOD_TERMINATION_REGEX, status):
            return 2
        return 3

    sorted_statuses = sorted(
        statuses, key=lambda status: _status_rank_str(status, status_rank)
    )

    for sp_termination_reason_str in sorted_statuses:
        if (
            sp_termination_reason_str
            in STR_TO_SUPERVISION_PERIOD_TERMINATION_REASON_MAPPINGS
        ):
            return STR_TO_SUPERVISION_PERIOD_TERMINATION_REASON_MAPPINGS[
                sp_termination_reason_str
            ]

    return StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN


def _status_rank_str(status: str, rank_fn: Callable[[str], int]) -> str:
    return f"{str({rank_fn(status)}).zfill(3)}{status}"


def parse_supervising_officer_type(raw_text: str) -> Optional[StateAgentType]:
    """Maps |raw_text|, a MO specific job title, to its corresponding StateAgentType."""
    if not raw_text:
        return None
    if (
        ("PROBATION" in raw_text and "PAROLE" in raw_text)
        or "P&P" in raw_text
        or "P & P" in raw_text
    ):
        return StateAgentType.SUPERVISION_OFFICER
    return StateAgentType.INTERNAL_UNKNOWN


DOMESTIC_VIOLENCE_CASE_TYPES = [
    "DVS",  # Domestic Violence Supervision
    "DOM",  # Domestic Violence
]

SERIOUS_MENTAL_ILLNESS_CASE_TYPES = [
    "SMI",  # Seriously Mentally Ill Caseload
]

SEX_OFFENSE_CASE_TYPES = [
    "DSO",  # Designated Sex Offenders
    "ISO",  # Interstate Sex Offenders
]


def parse_case_types(raw_text: str) -> Optional[StateSupervisionCaseType]:

    case_types = sorted_list_from_str(raw_text)

    for case_type in case_types:
        if case_type in DOMESTIC_VIOLENCE_CASE_TYPES:
            return StateSupervisionCaseType.DOMESTIC_VIOLENCE

        if case_type in SERIOUS_MENTAL_ILLNESS_CASE_TYPES:
            return StateSupervisionCaseType.SERIOUS_MENTAL_ILLNESS

        if case_type in SEX_OFFENSE_CASE_TYPES:
            return StateSupervisionCaseType.SEX_OFFENSE

    return None
