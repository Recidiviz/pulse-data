# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""US_MO specific enum helper methods."""
import re
from typing import Dict, Callable, Optional, List

from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodAdmissionReason
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodTerminationReason, \
    StateSupervisionPeriodAdmissionReason
from recidiviz.common.str_field_utils import sorted_list_from_str
from recidiviz.ingest.direct.direct_ingest_controller_utils import invert_enum_to_str_mappings
from recidiviz.ingest.direct.regions.us_mo.us_mo_constants import TAK026_STATUS_SUPERVISION_PERIOD_START_REGEX, \
    TAK026_STATUS_CYCLE_TERMINATION_REGEX, TAK026_STATUS_SUPERVISION_SENTENCE_COMPLETION_REGEX, \
    TAK026_STATUS_SUPERVISION_PERIOD_TERMINATION_REGEX


PAROLE_REVOKED_WHILE_INCARCERATED_STATUS_CODES: List[str] = [
    # All Parole Update (50N10*) statuses
    '50N1010',  # Parole Update - Tech Viol
    '50N1015',  # Parole Update - ITC Failure
    '50N1020',  # Parole Update - New Felony - Viol
    '50N1021',  # Parole Update - No Violation
    '50N1045',  # Parole Update - ITC Ineligible
    '50N1050',  # Parole Viol Upd - Fel Law Viol
    '50N1055',  # Parole Viol Upd - Misd Law Viol
    '50N1060',  # Parole Update - Treatment Center
    '50N1065',  # Parole Update - CRC

    # All Conditional Release Update (50N30*) statuses
    '50N3010',  # CR Update - Tech Viol
    '50N3015',  # CR Update - ITC Failure
    '50N3020',  # CR Update - New Felony - Viol
    '50N3021',  # CR Update - No Violation
    '50N3045',  # CR Update - ITC Ineligible
    '50N3050',  # CR Viol Update - Felony Law Viol
    '50N3055',  # CR Viol Update - Misd Law Viol
    '50N3060',  # CR Update - Treatment Center
    '50N3065',  # CR Update - CRC
]


SUPERVISION_PERIOD_TERMINATION_REASON_TO_STR_MAPPINGS: Dict[StateSupervisionPeriodTerminationReason, List[str]] = {
    StateSupervisionPeriodTerminationReason.ABSCONSION: [
        '65O1010',  # Offender declared absconder - from TAK026 BW$SCD
        '65O1020',  # Offender declared absconder - from TAK026 BW$SCD
        '65O1030',  # Offender declared absconder - from TAK026 BW$SCD
        '65L9100',  # Offender declared absconder - from TAK026 BW$SCD
    ],
    StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION: [
        '65N9500',  # Offender re-engaged - from TAK026 BW$SCD
    ],
    StateSupervisionPeriodTerminationReason.DEATH: [
        '99O9020',  # Suicide-Institution
        '99O9025',  # Suicide-Inst-Off Premises
        '99O9030',  # Accidental Death-Institution
        '99O9035',  # Accid Death-Inst-Off Premises
        '99O9040',  # Offense Related Death-Instit
        '99O9045',  # Offense Rel Death-Inst-Off Prm
        '99O9050',  # Natural Death-Institution
        '99O9055',  # Natural Death-Inst-Off Premise
        '99O9060',  # Death-Unknown Causes-Instit
        '99O9065',  # Death-Unk Cause-Inst-Off Prem
        '99O9070',  # Death Under Supervision-Field
        '99O9080',  # Death While Off DAI Premises
        '99O9520',  # Suicide-Field
        '99O9530',  # Accidental Death-Field
        '99O9540',  # Offense Related Death-Field
        '99O9550',  # Natural Death-Field
        '99O9560',  # Death-Unknown Causes-Field
        '99O9999',  # Execution
    ],
    StateSupervisionPeriodTerminationReason.DISCHARGE: [
        '99O0000',  # Converted Inactive-Institution
        '99O0010',  # Converted Off Records-Field
        '99O0020',  # Converted Revoked-Field
        '99O0989',  # Erroneous Commitment-Field
        '99O0999',  # Erroneous Commit-Institution
        '99O1000',  # Court Probation Discharge
        '99O1001',  # Court Probation ECC Discharge
        '99O1010',  # Court Prob Disc-CONFIDENTIAL
        '99O1011',  # Ct Prob ECC Disc-CONFIDENTIAL
        '99O1015',  # Court Prob-No Further Action
        '99O1016',  # No Further Action - Closed
        '99O1020',  # Institutional Commutation
        '99O1025',  # Field Commutation
        '99O1026',  # CONFIDENTIAL CLOSED-P&P
        '99O1027',  # Rev/Disch-DAI RETIRED
        '99O1030',  # Institutional Pardon
        '99O1035',  # Field Pardon
        '99O1040',  # Resentenced-Field Completion
        '99O1050',  # Reverse/Remand Discharge-DAI
        '99O1051',  # Reverse/Remand Discharge-P&P
        '99O1055',  # Court Ordered Disc-Institution
        '99O1060',  # Director's Discharge
        '99O1065',  # Director's Discharge-Field
        '99O1070',  # Director's Disc To Custody/Det
        '99O1080',  # CONFIDENTIAL CLOSED-DAI
        '99O1200',  # Court Parole Discharge
        '99O1210',  # Court Parole Revoked-Local
        '99O2000',  # Diversion Disc-CONFIDENTIAL
        '99O2005',  # Div-Term Services-CONFIDENTIAL
        '99O2010',  # Parole Discharge
        '99O2011',  # Parole Discharge-Institution
        '99O2012',  # Parole ECC Discharge
        '99O2013',  # Parole ECC Disc-Institution
        '99O2015',  # Parole Discharge-Admin
        '99O2020',  # Conditional Release Discharge
        '99O2021',  # CR Discharge-Institution
        '99O2022',  # Cond Release ECC Discharge
        '99O2023',  # Cond Rel ECC Disc-Institution
        '99O2025',  # CR Discharge-Admin
        '99O2030',  # Disc-Escape-Inst(Comment-ICOM)
        '99O2035',  # Disc-Absc-Field (Comment-POTR)
        '99O2040',  # Administrative Parole Disc
        '99O2041',  # Admin Parole Disc-Institution
        '99O2042',  # Administrative Parole ECC Disc
        '99O2043',  # Admin Parole ECC Disc-Inst
        '99O2045',  # Admin Parole Disc-Admin
        '99O2050',  # Inmate Field Discharge
        '99O2100',  # Prob Rev-Technical-Jail
        '99O2105',  # Prob Rev-New Felony Conv-Jail
        '99O2110',  # Prob Rev-New Misd Conv-Jail
        '99O2115',  # Prob Rev-Felony Law Viol-Jail
        '99O2120',  # Prob Rev-Codes Not Applicable
        '99O2215',  # Parole Disc-Retroactive
        '99O2225',  # CR Discharge-Retroactive
        '99O2245',  # Admin Parole Disc-Retroactive
        '99O3000',  # PreTrial Bond Supv Discharge
        '99O3100',  # PreTrial Bond-Close Interest
        '99O3130',  # Bond Supv-No Further Action
        '99O4000',  # IS Compact-Prob Discharge
        '99O4010',  # IS Compact-Prob Return/Tran
        '99O4020',  # IS Compact-Probation Revoked
        '99O4030',  # IS Comp-Unsup/Priv Prob-Disc
        '99O4040',  # IS Comp-Unsup/Priv PB-Ret/Tran
        '99O4050',  # IS Comp-Unsup/Priv Prob-Rev
        '99O4100',  # IS Compact-Parole Discharge
        '99O4110',  # IS Compact-Parole Ret/Tran
        '99O4120',  # IS Compact-Parole Revoked
        '99O4200',  # Discharge-Interstate Compact
        '99O4210',  # Interstate Compact Return
        '99O5005',  # PSI Other Disposition
        '99O5010',  # PSI Probation Denied-Other
        '99O5015',  # PSI Plea Withdrawn
        '99O5020',  # PSI Probation Denied-Jail
        '99O5030',  # PSI Cancelled by Court
        '99O5099',  # Investigation Close Interest
        '99O5300',  # Executive Clemency Denied
        '99O5305',  # Executive Clemency Granted
        '99O5310',  # Executive Clemency Inv Comp.
        '99O5405',  # Bond Invest-No Charge
        '99O5500',  # Diversion Denied
        '99O5605',  # SAR Other Disposition
        '99O5610',  # SAR Probation Denied-Other
        '99O5615',  # SAR Plea Withdrawn
        '99O5620',  # SAR Probation Denied-Jail
        '99O5630',  # SAR Cancelled by Court
        '99O6000',  # Discharge-Cell Leasing
        '99O7000',  # Relieved of Supv-Court Disc
    ],
    StateSupervisionPeriodTerminationReason.EXTERNAL_UNKNOWN: [],
    StateSupervisionPeriodTerminationReason.REVOCATION: [
        # TODO(2666): Consider reclassifying some of these as a status that
        #  indicates that it's a return but not a revocation
        '45O0ZZZ',  # Board Holdover     'MUST VERIFY'
        '45O0010',  # Emergency Board RF Housing
        '45O0050',  # Board Holdover
        '45O1ZZZ',  # Parole Return      'MUST VERIFY'
        '45O1010',  # Parole Ret-Tech Viol
        '45O1020',  # Parole Ret-New Felony-Viol
        '45O1021',  # Parole Ret-No Violation
        '45O1050',  # Parole Viol-Felony Law Viol
        '45O1055',  # Parole Viol-Misd Law Viol
        '45O1060',  # Parole Ret-Treatment Center
        '45O1070',  # Parole Return-Work Release
        '45O2ZZZ',  # Probation Revoked  'MUST VERIFY'
        '45O2000',  # Prob Rev-Technical
        '45O2005',  # Prob Rev-New Felony Conv
        '45O2010',  # Prob Rev-New Misd Conv
        '45O2015',  # Prob Rev-Felony Law Viol
        '45O2020',  # Prob Rev-Misd Law Viol
        '45O3ZZZ',  # CR Return          'MUST VERIFY'
        '45O3010',  # CR Ret-Tech Viol
        '45O3020',  # CR Ret-New Felony-Viol
        '45O3021',  # CR Ret-No Violation
        '45O3050',  # CR Viol-Felony Law Viol
        '45O3055',  # CR Viol-Misd Law Viol
        '45O3060',  # CR Ret-Treatment Center
        '45O3070',  # CR Return-Work Release
        '45O40ZZ',  # Resid Fac Return   'MUST VERIFY'
        '45O4010',  # Emergency Inmate RF Housing
        '45O4030',  # RF Return-Administrative
        '45O4035',  # RF Return-Treatment Center
        '45O4040',  # RF Return-Technical
        '45O4045',  # RF Return-New Felony Conv
        '45O4050',  # RF Return-New Misd Conv
        '45O41ZZ',  # EMP Return         'MUST VERIFY'
        '45O4130',  # EMP Return-Administrative
        '45O4135',  # EMP Return-Treatment Center
        '45O4140',  # EMP Return-Technical
        '45O4145',  # EMP Return-New Felony Conv
        '45O4150',  # EMP Return-New Misd Conv
        '45O42ZZ',  # IS Compact-Parole- 'MUST VERIFY'
        '45O4270',  # IS Compact-Parole-CRC Work Rel
        '45O490Z',  # CR Deferred Return 'MUST VERIFY'
        '45O4900',  # CR Deferred Return
        '45O4999',  # Inmate Return From EMP/RF
        '45O50ZZ',  # PSI-Prob Denied    'MUST VERIFY'
        '45O5000',  # PSI Probation Denied-DAI
        '45O51ZZ',  # Pre-Sentence Comm  'MUST VERIFY'
        '45O5100',  # Pre-Sent Assess Commit to DAI
        '45O56ZZ',  # SAR-Prob Denied    'MUST VERIFY'
        '45O5600',  # SAR Probation Denied-DAI
        '45O57ZZ',  # Resentenced-No Rev 'MUST VERIFY'
        '45O5700',  # Resentenced-No Revocation
        '45O59ZZ',  # Inv/Bnd Sup to DAI 'MUST VERIFY'
        '45O5999',  # Inv/Bnd Sup Complete- To DAI
        '45O700Z',  # To DAI-Other Sent  'MUST VERIFY'
        '45O7000',  # Field to DAI-Other Sentence
        '45O7001',  # Field Supv to DAI-Same Offense
        '45O701Z',  # Prob to DAI-Shock  'MUST VERIFY'
        '45O7010',  # Prob-DAI-Shock Incarceration
        '45O702Z',  # Prob-Ct Order Det--'MUST VERIFY'
        '45O7020',  # Prob-Ct Order Detention Sanctn
        '45O703Z',  # Prob-MH 120 Day--'MUST VERIFY'
        '45O7030',  # Prob-Mental Health 120 Day
        '45O706Z',  # Prob-Post Conv-Trt 'MUST VERIFY'
        '45O7060',  # Prob-Post Conv-Trt Pgm
        '45O7065',  # Prob-Post Conv-RDP
        '45O77ZZ',  # IS Cmpct-Err Commt 'MUST VERIFY'
        '45O7700',  # IS Compact-Erroneous Commit
        '45O7999',  # Err Release-P&P Return to DAI
        '45O8ZZZ',  # Admin Return       'MUST VERIFY'
        '45O8010',  # Adm Ret-Tech Viol
        '45O8020',  # Adm Ret-New Felony-Viol
        '45O8021',  # Adm Return-No Violation
        '45O8050',  # Adm Viol-Felony Law Viol
        '45O8055',  # Adm Viol-Misd Law Viol
        '45O8060',  # Adm Ret-Treatment Center
        '45O8070',  # Admin Par Return-Work Release
        '45O9998',  # Converted-Revoke DOC-No Vio
        '45O9999',  # Revocation-Code Not Applicable

        # All Parole Revocation (40I1*) statuses from TAK026
        '40I1010',  # Parole Ret-Tech Viol
        '40I1020',  # Parole Ret-New Felony-Viol
        '40I1021',  # Parole Ret-No Violation
        '40I1025',  # Medical Parole Ret - Rescinded
        '40I1040',  # Parole Ret-OTST Decision Pend
        '40I1050',  # Parole Viol-Felony Law Viol
        '40I1055',  # Parole Viol-Misd Law Viol
        '40I1060',  # Parole Ret-Treatment Center
        '40I1070',  # Parole Return-Work Release
        # All Conditional Release Return (40I3*) statuses from TAK026
        '40I3010',  # CR Ret-Tech Viol
        '40I3020',  # CR Ret-New Felony-Viol
        '40I3021',  # CR Ret-No Violation
        '40I3040',  # CR Ret-OTST Decision Pend
        '40I3050',  # CR Viol-Felony Law Viol
        '40I3055',  # CR Viol-Misd Law Viol
        '40I3060',  # CR Ret-Treatment Center
        '40I3070',  # CR Return-Work Release

    ],
    StateSupervisionPeriodTerminationReason.SUSPENSION: [
        '65O2015',  # Court Probation Suspension
        '65O3015',  # Court Parole Suspension
    ],

}


SUPERVISION_PERIOD_ADMISSION_REASON_TO_STR_MAPPINGS: Dict[StateSupervisionPeriodAdmissionReason, List[str]] = {
    StateSupervisionPeriodAdmissionReason.ABSCONSION: [
        '65O1010',  # Offender declared absconder - from TAK026 BW$SCD
        '65O1020',  # Offender declared absconder - from TAK026 BW$SCD
        '65O1030',  # Offender declared absconder - from TAK026 BW$SCD
        '99O2035',  # Offender declared absconder - from TAK026 BW$SCD
        '65L9100',  # Offender declared absconder - from TAK026 BW$SCD
    ],
    StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE: [
        # All 40O* statuses correspond to being released from an
        # institution to supervision.
        '40O0010',  # Emer Board RF Housing-Release
        '40O0050',  # Board Holdover Release
        '40O0055',  # Board Hold Rel To Custody/Det
        '40O1010',  # Parole Release
        '40O1015',  # Parolee Released From CRC
        '40O1020',  # Parole To Custody/Detainer
        '40O1025',  # Medical Parole Release
        '40O1030',  # Parole Re-Release
        '40O1040',  # Parole Return Rescinded
        '40O1060',  # Parolee Re-Rel From Inst Pgm
        '40O1065',  # Parolee Rel From Inst Pgm-New
        '40O1080',  # Parolee Released from Inst
        '40O2000',  # Prob Rev-Rel to Field-Spc Cir
        '40O3010',  # Conditional Release
        '40O3015',  # Conditional Released From CRC
        '40O3020',  # CR To Custody/Detainer
        '40O3030',  # Conditional Re-Release
        '40O3040',  # CR Return Rescinded
        '40O3060',  # CR Re-Release From Inst Pgm
        '40O3065',  # CR Released From Inst Pgm-New
        '40O3080',  # Cond Releasee Rel from Inst
        '40O4010',  # Emer Inmate RF Housing-Release
        '40O4099',  # Inmate Release to RF
        '40O4199',  # Inmate Release to EMP
        '40O4270',  # IS Compact-Parole-Rel From CRC
        '40O4900',  # CR Deferred-Release to Field
        '40O5000',  # Release to Field-Invest Pend
        '40O5100',  # Rel to Field-PSI Assess Comm
        '40O6000',  # Converted-CRC DAI to CRC Field
        '40O6010',  # Release for SVP Commit Hearing
        '40O6020',  # Release for Lifetime Supv
        '40O7000',  # Rel to Field-DAI Other Sent
        '40O7001',  # Rel to Field-Same Offense
        '40O7010',  # Rel to Prob-Shck Incarceration
        '40O7020',  # Rel to Prob-Ct Order Det Sanc
        '40O7030',  # Rel to Prob-MH 120 Day
        '40O7060',  # Rel to Prob-Post Conv-Trt Pgm
        '40O7065',  # Rel to Prob-Post-Conv-RDP
        '40O7400',  # IS Compact Parole to Missouri
        '40O7700',  # IS Compact-Err Commit-Release
        '40O8010',  # Admin Parole Release
        '40O8015',  # Adm Parolee Released from CRC
        '40O8020',  # Adm Parole To Custody/Detainer
        '40O8060',  # Adm Par Re-Rel From Inst Pgm
        '40O8065',  # Adm Par Rel From Inst Pgm-New
        '40O8080',  # Adm Parolee Rel from Inst
        '40O9010',  # Release to Probation
        '40O9020',  # Release to Prob-Custody/Detain
        '40O9030',  # Statutory Probation Release
        '40O9040',  # Stat Prob Rel-Custody/Detainer
        '40O9060',  # Release to Prob-Treatment Ctr
        '40O9070',  # Petition Probation Release
        '40O9080',  # Petition Prob Rel-Cust/Detain
        '40O9100',  # Petition Parole Release
        '40O9110',  # Petition Parole Rel-Cus/Detain
    ],
    StateSupervisionPeriodAdmissionReason.COURT_SENTENCE: [
        '15I1000',  # New Court Probation
        '15I1200',  # New Court Parole
        '15I2000',  # New Diversion Supervision
        '15I3000',  # New PreTrial Bond Supervision
    ],
    StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION: [
        '65I1099',  # Supervision Reinstated
        '65I2015',  # Court Probation Reinstated
        '65I3015',  # Court Parole Reinstated
        '65I6010',  # Inmate Reinstated EMP / RF
    ],
    StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION: [
        '65N9500',  # Offender re-engaged - from TAK026 BW$SCD
    ]
}


STR_TO_SUPERVISION_PERIOD_ADMISSION_REASON_MAPPINGS: Dict[str, StateSupervisionPeriodAdmissionReason] = \
    invert_enum_to_str_mappings(SUPERVISION_PERIOD_ADMISSION_REASON_TO_STR_MAPPINGS)


STR_TO_SUPERVISION_PERIOD_TERMINATION_REASON_MAPPINGS: Dict[str, StateSupervisionPeriodTerminationReason] = \
    invert_enum_to_str_mappings(SUPERVISION_PERIOD_TERMINATION_REASON_TO_STR_MAPPINGS)


def supervision_period_admission_reason_mapper(label: str) -> Optional[StateSupervisionPeriodAdmissionReason]:
    """Maps |label|, a space delimited list of statuses from TAK026, to the most relevant
    SupervisionPeriodAdmissionReason, when possible.

    If the status list is empty, we assume that this period ended because the person transferred between POs or offices.
    """
    # TODO(2865): Update enum normalization so that we separate by commas instead of spaces
    statuses = sorted_list_from_str(label, ' ')
    if not statuses:
        return StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE

    def status_rank(status: str) -> int:
        """In the case that there are multiple statuses on the same day, we pick the status that is most likely to
        give us accurate info about the reason this supervision period was started. In the case of supervision
        period admissions, we pick statuses that have the pattern X5I* (e.g. '15I1000'), since those statuses are
        field (5) IN (I) statuses. In the absence if one of those statuses, we get our info from other statuses.
        """
        if re.match(TAK026_STATUS_SUPERVISION_PERIOD_START_REGEX, status):
            return 0
        return 1

    sorted_statuses = sorted(statuses, key=lambda status: _status_rank_str(status, status_rank))

    for sp_admission_reason_str in sorted_statuses:
        if sp_admission_reason_str in STR_TO_SUPERVISION_PERIOD_ADMISSION_REASON_MAPPINGS:
            return STR_TO_SUPERVISION_PERIOD_ADMISSION_REASON_MAPPINGS[sp_admission_reason_str]

    return StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN


def supervision_period_termination_reason_mapper(label: str) -> Optional[StateSupervisionPeriodTerminationReason]:
    """Maps |label|, a space delimited list of statuses from TAK026, to the most relevant
    SupervisionPeriodTerminationReason, when possible.

    If the status list is empty, we assume that this period ended because the person transferred between POs or offices.
    """

    statuses = sorted_list_from_str(label, ' ')
    if not statuses:
        return StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE

    def status_rank(status: str):
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

    sorted_statuses = sorted(statuses, key=lambda status: _status_rank_str(status, status_rank))

    for sp_termination_reason_str in sorted_statuses:
        if sp_termination_reason_str in STR_TO_SUPERVISION_PERIOD_TERMINATION_REASON_MAPPINGS:
            return STR_TO_SUPERVISION_PERIOD_TERMINATION_REASON_MAPPINGS[sp_termination_reason_str]

    return StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN


def incarceration_period_admission_reason_mapper(label: str) -> Optional[StateIncarcerationPeriodAdmissionReason]:
    """Maps |label|, a space delimited list of statuses from TAK026, to the most relevant
    IncarcerationPeriodAdmissionReason, when possible.

    If multiple type of statuses are found, we first look for a non-null parole revocation status, then probation
    revocation status, then new admission status, and finally temporary custody statuses.
    """
    start_statuses = sorted_list_from_str(label, ' ')
    # TODO(2647): If both probation and parole statuses, return DUAL
    for status in start_statuses:
        if status.startswith((
                '40I1',  # Parole Revocation
                '40I3',  # Conditional Release Return (Parole Revocation)
        )) or status in PAROLE_REVOKED_WHILE_INCARCERATED_STATUS_CODES:
            return StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION
        if status.startswith(
                '40I2'  # Probation Revocation
        ):
            return StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION
        if status.startswith(
                '10I1'  # New Court Commitment (New Admission)
        ):
            return StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION
        if status == '40I0050':
            return StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY

    # No default here, bc we keep track of the IP-IX statuses whenever
    return None


def supervising_officer_mapper(label: str) -> Optional[StateAgentType]:
    """Maps |label|, a MO specific job title, to its corresponding StateAgentType."""
    if not label:
        return None
    # TODO(2865): Update enum normalization so that we separate by commas instead of spaces
    if ('PROBATION' in label and 'PAROLE' in label) or 'P P' in label:
        return StateAgentType.SUPERVISION_OFFICER
    return StateAgentType.INTERNAL_UNKNOWN


def _status_rank_str(status: str, rank_fn: Callable[[str], int]):
    return f'{str({rank_fn(status)}).zfill(3)}{status}'
