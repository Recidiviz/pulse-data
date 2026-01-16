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
from typing import Callable, Dict, List, Optional, Set

from more_itertools import one

from recidiviz.common.constants.enum_parser import EnumParsingError
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_employment_period import (
    StateEmploymentPeriodEmploymentStatus,
)
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentType,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
    StateSentencingAuthority,
)
from recidiviz.common.constants.state.state_staff_role_period import StateStaffRoleType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.str_field_utils import sorted_list_from_str
from recidiviz.ingest.direct.regions.custom_enum_parser_utils import (
    invert_enum_to_str_mappings,
)
from recidiviz.ingest.direct.regions.us_mo.ingest_views.templates_sentences import (
    BOND_STATUSES,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_sentence_classification import (
    UsMoSentenceStatus,
)

NEW_COURT_COMMITTMENT_STATUS_CODES: List[str] = [
    #  All New Court Commitment (10I10*) statuses from TAK026 (except erroneous committment)
    "10I1000",  # New Court Comm-Institution
    "10I1010",  # New Court Comm-120 Day
    "10I1020",  # New Court Comm-Long Term Treat
    "10I1030",  # New Court Comm-Reg Dis Prog
    "10I1040",  # New Court Comm-120 Day Treat
    "10I1050",  # New Court Commit-SOAU
    "10I1060",  # New Court Commit-MH 120 Day
    "10I4000",  # New Interstate Compact-Inst
]

NEW_ADMISSION_SECONDARY_STATUS_CODES: List[str] = [
    #  This status will sometimes show up as the only new admission status after an investigation is over
    "20I1000",  # Court Comm-Inst-Addl Charge
    #  Sometimes this is the only IN status after a Reverse/Remand Completion (90O1050)
    "20L6000",  # CC Fed/State (Papers Only)-AC
    "30L6000",  # CC Fed/State(Papers Only)-Revt
]

COURT_COMMITMENT_REVISIT_STATUS_CODES: List[str] = [
    #  All Court Commitment Revisit (30I10*) statuses from TAK026
    "30I1000",  # Court Comm-Institution-Revisit
    "30I1010",  # Court Comm-120 Day-Revisit
    "30I1020",  # Court Comm-Lng Trm Trt-Revisit
    "30I1030",  # Court Comm-Reg Dis Prg-Revisit
    "30I1040",  # Court Comm-120 Day Trt-Revisit
    "30I1050",  # Court Comm-SOAU-Revisit
    "30I1060",  # Court Comm-MH 120 Day-Revisit
]

RETURN_POST_REMAND_STATUS_CODES: List[str] = [
    #  This is a new admission that happens after someone has had a sentence remanded
    #  (i.e. sent back to court to be retried) and is re-committed.
    "20I1100",  # Reverse/Remand-Offense Reentry
    "30I1100",  # Reverse/Remand-Offense Revisit
    "60I1050",  # Rev/Rem Return-Original Sent
]

BOND_RETURN_STATUS_CODES: List[str] = [
    #  When is someone released on bond after they have already been committed then is returned
    "60I4010",  # Inmate Bond Return
]

PAROLE_REVOKED_REENTRY_STATUS_CODES: List[str] = [
    # All Parole Revocation (40I1*) statuses from TAK026 (except 40I1060, which is a
    # sanction admission, and 40I1040, which is a temporary custody admission pending an
    # out of state decision).
    "40I1010",  # Parole Ret-Tech Viol
    "40I1020",  # Parole Ret-New Felony-Viol
    "40I1021",  # Parole Ret-No Violation
    "40I1025",  # Medical Parole Ret - Rescinded
    "40I1050",  # Parole Viol-Felony Law Viol
    "40I1055",  # Parole Viol-Misd Law Viol
    "40I1070",  # Parole Return-Work Release
]

#  Note: Conditional Release is a type of discretionary parole
CONDITIONAL_RELEASE_RETURN_STATUS_CODES: List[str] = [
    # All Conditional Release Return (40I3*) statuses from TAK026
    "40I3010",  # CR Ret-Tech Viol
    "40I3020",  # CR Ret-New Felony-Viol
    "40I3021",  # CR Ret-No Violation
    "40I3050",  # CR Viol-Felony Law Viol
    "40I3055",  # CR Viol-Misd Law Viol
    "40I3060",  # CR Ret-Treatment Center
    "40I3070",  # CR Return-Work Release
]

PAROLE_REVOKED_WHILE_INCARCERATED_STATUS_CODES: List[str] = [
    # All Parole Update (50N10*) statuses
    "50N1010",  # Parole Update - Tech Viol
    "50N1020",  # Parole Update - New Felony - Viol
    "50N1021",  # Parole Update - No Violation
    "50N1045",  # Parole Update - ITC Ineligible
    "50N1050",  # Parole Viol Upd - Fel Law Viol
    "50N1055",  # Parole Viol Upd - Misd Law Viol
    "50N1065",  # Parole Update - CRC
    # All Conditional Release Update (50N30*) statuses
    "50N3010",  # CR Update - Tech Viol
    "50N3020",  # CR Update - New Felony - Viol
    "50N3021",  # CR Update - No Violation
    "50N3045",  # CR Update - ITC Ineligible
    "50N3050",  # CR Viol Update - Felony Law Viol
    "50N3055",  # CR Viol Update - Misd Law Viol
    "50N3065",  # CR Update - CRC
]

MID_INCARCERATION_TREATMENT_FAILURE_STATUSES: List[str] = [
    #  All statuses indicating a failure of treatment causing mandate to serve rest of
    #  sentence
    "50N1015",  # Parole Update - ITC Failure
    "50N3015",  # CR Update - ITC Failure
]

MID_INCARCERATION_TREATMENT_COMMITMENT_STATUSES: List[str] = [
    #  All statuses indicating a transition into treatment in the middle of a stint on
    # incarceration (usually following a board hold).
    "50N1060",  # Parole Update - Treatment Center
    "50N3060",  # CR Update - Treatment Center
]

ADMITTED_FOR_TREATMENT_STATUSES: List[str] = [
    # Statuses that indicate someone entered a facility for treatment
    # Probation sanction admission for treatment
    "20I1020",  # Court Comm-Lng Tm Trt-Addl Chg
    "20I1040",  # Court Comm-120 Day Treat-Addl
    "20I1060",  # Court Comm-MH 120 Day-Addl Chg
    "40I2100",  # Prob Rev-Tech-120 Day Treat
    "40I7060",  # Prob Adm-Post Conv-Trt Pgm
    "40I7030",  # Prob Adm-Mental Health 120 Day
    #  Parole returns for treatment
    "40I1060",  # Parole Ret-Treatment Center
]

PROBATION_REVOCATION_RETURN_STATUSES: List[str] = [
    #  All Probation Revocation (40I2*) statuses from TAK026
    "40I2000",  # Prob Rev-Technical
    "40I2005",  # Prob Rev-New Felony Conv
    "40I2010",  # Prob Rev-New Misd Conv
    "40I2015",  # Prob Rev-Felony Law Viol
    "40I2020",  # Prob Rev-Misd Law Viol
    "40I2025",  # Prob Rev-Tech-Long Term Treat
    "40I2030",  # Prob Rev-New Felon-Lng Trm Trt
    "40I2035",  # Prob Rev-New Misd-Lng Term Trt
    "40I2040",  # Prob Rev-Felony Law-Lng Tm Trt
    "40I2045",  # Prob Rev-Misd Law-Lng Trm Trt
    "40I2050",  # Prob Rev-Technical-120Day
    "40I2055",  # Prob Rev-New Felony-120Day
    "40I2060",  # Prob Rev-New Misd-120Day
    "40I2065",  # Prob Rev-Felony Law-120Day
    "40I2070",  # Prob Rev-Misd Law Viol-120Day
    "40I2075",  # Prob Rev-Tech-Reg Disc Program
    "40I2080",  # Prob Rev-New Fel Conv-Reg Dis
    "40I2085",  # Prob Rev-New Mis Conv-Reg Disc
    "40I2090",  # Prob Rev-Fel Law Vio-Reg Disc
    "40I2095",  # Prob Rev-Misd Law Vio-Reg Disc
    "40I2105",  # Prob Rev-New Felon-120 Day Trt
    "40I2110",  # Prob Rev-New Misd-120 Day Trt
    "40I2115",  # Prob Rev-Fel Law-120 Day Treat
    "40I2120",  # Prob Rev-Mis Law-120 Day Treat
    "40I2130",  # Prob Rev-Tech-SOAU
    "40I2135",  # Prob Rev-New Felony-SOAU
    "40I2145",  # Prob Rev-Felony Law-SOAU
    "40I2150",  # Prob Rev-Misdemeanor Law-SOAU
    "40I2160",  # Prob Rev-Tech-MH 120 Day
    "40I2300",  # Prob Rev Ret-Technical
    "40I2305",  # Prob Rev Ret-New Felony Conv
    "40I2310",  # Prob Rev Ret-New Misd Conv
    "40I2315",  # Prob Rev Ret-Felony Law Viol
    "40I2320",  # Prob Rev Ret-Misd Law Viol
    "40I2325",  # Prob Rev Ret-Tech-Lng Term Trt
    "40I2330",  # Prob Rev Ret-New Fel-Lg Tm Trt
    "40I2335",  # Prob Rev Ret-New Mis-Lg Tm Trt
    "40I2340",  # Prob Rev Ret-Fel Law-Lg Tm Trt
    "40I2345",  # Prob Rev Ret-Mis Law-Lg Tm Trt
    "40I2350",  # Prob Rev Ret-Technical-120 Day
    "40I2355",  # Prob Rev Ret-New Fel-120 Day
    "40I2365",  # Prob Rev Ret-Fel Law-120 Day
    "40I2370",  # Prob Rev Ret-Mis Law-120 Day
    "40I2375",  # Prob Rev Ret-Tech-Reg Disc Pgm
    "40I2380",  # Prob Rev Ret-New Fel-Reg Disc
    "40I2390",  # Prob Rev Ret-Fel Law-Reg Disc
    "40I2400",  # Prob Rev Ret-Tech-120-Day Trt
    "40I2405",  # Prob Rev Ret-New Fel-120-D Trt
    "40I2410",  # Prob Rev Ret-New Mis-120-D Trt
    "40I2415",  # Prob Rev Ret-Fel Law-120-D Trt
    "40I2420",  # Prob Rev Ret-Mis Law-120-D Trt
    "40I2430",  # Prob Rev Ret-Technical-SOAU
    "40I2435",  # Prob Rev Ret-New Fel-SOAU
    "40I2450",  # Prob Rev Ret-Mis Law-SOAU
]

LEGACY_PROBATION_REENTRY_STATUS_CODES: List[str] = [
    #  All Residential Facility Re-entry statuses - these don't show up after 2010
    "40I4030",  # RF Reentry-Administrative
    "40I4035",  # RF Reentry-Treatment Center
    "40I4040",  # RF Reentry-Technical
    "40I4045",  # RF Reentry-New Felony Conv
    "40I4050",  # RF Reentry-New Misd Conv
    #  All Electronic Monitoring Re-entry statuses - these don't show up after 2010
    "40I4130",  # EMP Reentry-Administrative
    "40I4135",  # EMP Reentry-Treatment Center
    "40I4140",  # EMP Reentry-Technical
    "40I4145",  # EMP Reentry-New Felony Conv
    "40I4150",  # EMP Reentry-New Misd Conv
    #  All Admin return statuses - these don't show up after 2010
    "40I8010",  # Adm Ret-Tech Viol
    "40I8020",  # Adm Ret-New Felony-Viol
    "40I8021",  # Adm Ret-No Violation
    "40I8040",  # Adm Ret-OTST Decision Pend
    "40I8050",  # Adm Viol-Felony Law Viol
    "40I8055",  # Adm Viol-Misd Law Viol
    "40I8060",  # Adm Ret-Treatment Center
    "40I8070",  # Admin Par Return-Work Release
]

PROBATION_REVOCATION_SECONDARY_STATUS_CODES: List[str] = [
    # These codes will sometimes show up as the only meaningful statuses indicating a
    # probation revocation
    "40I7000",  # Field Supv to DAI-Oth Sentence
    "40I7001",  # Field Supv to DAI-Same Sentence
]

TREATMENT_SANCTION_STATUS_CODES: List[str] = [
    *ADMITTED_FOR_TREATMENT_STATUSES,
    *MID_INCARCERATION_TREATMENT_COMMITMENT_STATUSES,
]

SHOCK_SANCTION_STATUS_CODES: List[str] = [
    # Probation sanction admissions for shock incarceration
    "20I1010",  # Court Comm-120 Day-Addl Charge
    "20I1030",  # Court Comm-Reg Dis Pgm-Addl Ch
    "20I1050",  # Court Comm-SOAU-Addl Charge
    "40I7010",  # Prob Adm-Shock Incarceration
    "40I7020",  # Prob Adm-Ct Order Det Sanction
    "40I7065",  # Prob Adm-Post Conv-RDP
]

SUPERVISION_SANCTION_COMMITMENT_FOR_TREATMENT_OR_SHOCK_STATUS_CODES: List[str] = (
    #  All commitment for treatment / shock additional sentences (20I10*) statuses from
    #  TAK026 (except 20I1000, which is always NEW_ADMISSION). These statuses may
    #  show up on the same day as another new admission status, but if they
    #  do not, then we can treat them as a sanction admission from supervision.
    TREATMENT_SANCTION_STATUS_CODES
    + SHOCK_SANCTION_STATUS_CODES
)


BOARD_HOLDOVER_ENTRY_STATUS_CODES: List[str] = [
    #  All Board Holdover incarceration admission statuses (40I*)
    "40I0050",  # Board Holdover
    "45O0ZZZ",  # Board Holdover     'MUST VERIFY'
    "40I1040",  # Parole Ret-OTST Decision Pend
    "40I3040",  # CR Ret-OTST Decision Pend
]

RETURN_FROM_ESCAPE_STATUS_CODES: List[str] = [
    "60I5010",  # Escapee Return-Tech Viol
    "60I5020",  # Escapee Return-New Felony Conv
    "60I5030",  # Escapee Return-Hearing W/O Chg
    "60I5040",  # Escapee Return-Pending Charges
    #  All Walkaway (60I6*) statuses - these aren't used after 2015
    "60I6010",  # Walkaway Return-Tech Viol
    "60I6020",  # Walkaway Return-New Felony Con
    "60I6030",  # Walkaway Return-Hear W/O Chg
    "60I6040",  # Walkaway Return-Pending Chgs
    "60I6110",  # BD Walkaway Ret-Technical Vio
    "60I6120",  # BD Walkaway Ret-New Felony Con
    "60I6130",  # BD Walkaway Ret-Hear W/O Chg
    "60I6140",  # BD Walkaway Ret-Pending Chgs
    "60I6210",  # PB Walkaway Ret-Technical Vio
    "60I6220",  # PB Walkaway Ret-New Felony Con
    "60I6230",  # PB Walkaway Ret-Hear W/O Chg
    "60I6240",  # PB Walkaway Ret-Pending Chgs
]

RETURN_FROM_ERRONEOUS_RELEASE_STATUS_CODES: List[str] = [
    "60I3040",  # Erroneous Release Return-Other
    "60I3050",  # Erroneous Release Return-DAI
]

INSTITUTIONAL_TRANSFER_FROM_OUT_OF_STATE_STATUS_CODES: List[str] = [
    #  This always follows a 10L6000/20L6000/20L6000 (New CC Fed/State (Papers Only)), which means the person was being
    #  held in another federal / out of state facility but this is the first time they are transferred into an MO
    #  facility.
    "40I6000",  # CC Fed/State (Offender Rec)
    "70I3010",  # MO Inmate-Interstate Return
    "70I3020",  # Federal Transfer Return
]

ADMITTED_IN_ERROR_STATUS_CODES: List[str] = [
    "10I1099",  # New Erroneous Commitment
]

REDUCTION_OF_SENTENCE_REENTRY_STATUS_CODES: List[str] = [
    #  This happens when someone has already been admitted while they are undergoing a sentencing assessment
    #  (05I5600/35I5600?) and then they are resentenced, even though they may never move facilities. MO treats
    #  this as NEW_ADMISSION but we map this to TRANSFER since the person is already in prison.
    "20I1140",  # Reduction of Sentence-Reentry
]

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

PAPERS_ONLY_SUPERVISION_START_STATUSES = [
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
]


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
    StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN: [
        "65O1050",  # Rev/Rem Conv-Pending
        "65O6010",  # Inmate Walkaway from EMP/RF
        "95O0989",  # Erroneous Commit-Completion
        "95O1000",  # Court Probation Completion
        "95O1011",  # Ct Prob ECC Comp-CONFIDENTIAL
        "95O1015",  # Court Prob-No Further Action
        "95O1026",  # CONFIDENTIAL CLOSED-P&P
        "95O1050",  # Reverse/Remand Completion
        "95O2005",  # Div Term of Service-CONFIDENTI
        "95O2010",  # Parole Completion
        "95O2015",  # Parole Completion-Admin
        "95O2110",  # Prob Rev-New Misd Conv-Jail
        "95O7000",  # Relieved of Supv-Court
        "95O1040",  # Resentenced
        "99O7110",  # DOES NOT EXIST IN TAK146 but likely another DAGTA ERROR status
        "99O7115",  # DATA ERROR-Probation Denied
        "99O7125",  # DATA ERROR-Revoke Jail Other
        "99O7130",  # DATA ERROR-Diversion
        "99O7135",  # DATA ERROR-No Code
        "99O7140",  # DATA ERROR-Revoke DOC
        "99O7145",  # DATA ERROR-Suspended
        "99O7150",  # DATA ERROR-Transfer OTST
        "99O7155",  # DATA ERROR-Roll Back or Escape
        "99O7160",  # DATA ERROR-Diversion-Rev-Other
        *RETURN_FROM_ESCAPE_STATUS_CODES,
        *RETURN_FROM_ERRONEOUS_RELEASE_STATUS_CODES,
        *LEGACY_PROBATION_REENTRY_STATUS_CODES,
    ],
    StateSupervisionPeriodTerminationReason.ADMITTED_TO_INCARCERATION: [
        *NEW_COURT_COMMITTMENT_STATUS_CODES,
        "20I1000",  # Court Comm-Inst-Addl Charge
        *COURT_COMMITMENT_REVISIT_STATUS_CODES,
        *RETURN_POST_REMAND_STATUS_CODES,
        *ADMITTED_FOR_TREATMENT_STATUSES,
        *SHOCK_SANCTION_STATUS_CODES,
        *BOARD_HOLDOVER_ENTRY_STATUS_CODES,
        *INSTITUTIONAL_TRANSFER_FROM_OUT_OF_STATE_STATUS_CODES,
        *PROBATION_REVOCATION_SECONDARY_STATUS_CODES,
        *REDUCTION_OF_SENTENCE_REENTRY_STATUS_CODES,
        "70I3030",  # Out of Custody Returned
    ],
    StateSupervisionPeriodTerminationReason.REVOCATION: [
        *PROBATION_REVOCATION_RETURN_STATUSES,
        # TODO(#10498): Consider reclassifying some of these as a status that indicates
        # that it's a return but not a revocation
        "45O0010",  # Emergency Board RF Housing
        "45O0050",  # Board Holdover
        "45O1010",  # Parole Ret-Tech Viol
        "45O1020",  # Parole Ret-New Felony-Viol
        "45O1021",  # Parole Ret-No Violation
        "45O1050",  # Parole Viol-Felony Law Viol
        "45O1055",  # Parole Viol-Misd Law Viol
        "45O1060",  # Parole Ret-Treatment Center
        "45O1070",  # Parole Return-Work Release
        "45O2000",  # Prob Rev-Technical
        "45O2005",  # Prob Rev-New Felony Conv
        "45O2010",  # Prob Rev-New Misd Conv
        "45O2015",  # Prob Rev-Felony Law Viol
        "45O2020",  # Prob Rev-Misd Law Viol
        "45O3010",  # CR Ret-Tech Viol
        "45O3020",  # CR Ret-New Felony-Viol
        "45O3021",  # CR Ret-No Violation
        "45O3050",  # CR Viol-Felony Law Viol
        "45O3055",  # CR Viol-Misd Law Viol
        "45O3060",  # CR Ret-Treatment Center
        "45O3070",  # CR Return-Work Release
        "45O4010",  # Emergency Inmate RF Housing
        "45O4030",  # RF Return-Administrative
        "45O4035",  # RF Return-Treatment Center
        "45O4040",  # RF Return-Technical
        "45O4045",  # RF Return-New Felony Conv
        "45O4050",  # RF Return-New Misd Conv
        "45O4130",  # EMP Return-Administrative
        "45O4135",  # EMP Return-Treatment Center
        "45O4140",  # EMP Return-Technical
        "45O4145",  # EMP Return-New Felony Conv
        "45O4150",  # EMP Return-New Misd Conv
        "45O4270",  # IS Compact-Parole-CRC Work Rel
        "45O4900",  # CR Deferred Return
        "45O4999",  # Inmate Return From EMP/RF
        "45O5000",  # PSI Probation Denied-DAI
        "45O5100",  # Pre-Sent Assess Commit to DAI
        "45O5600",  # SAR Probation Denied-DAI
        "45O5700",  # Resentenced-No Revocation
        "45O7000",  # Field to DAI-Other Sentence
        "45O7001",  # Field Supv to DAI-Same Offense
        "45O7010",  # Prob-DAI-Shock Incarceration
        "45O7020",  # Prob-Ct Order Detention Sanctn
        "45O7030",  # Prob-Mental Health 120 Day
        "45O7060",  # Prob-Post Conv-Trt Pgm
        "45O7065",  # Prob-Post Conv-RDP
        "45O7700",  # IS Compact-Erroneous Commit
        "45O7999",  # Err Release-P&P Return to DAI
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
        "40I1050",  # Parole Viol-Felony Law Viol
        "40I1055",  # Parole Viol-Misd Law Viol
        "40I1070",  # Parole Return-Work Release
        # All Conditional Release Return (40I3*) statuses from TAK026
        "40I3010",  # CR Ret-Tech Viol
        "40I3020",  # CR Ret-New Felony-Viol
        "40I3021",  # CR Ret-No Violation
        "40I3050",  # CR Viol-Felony Law Viol
        "40I3055",  # CR Viol-Misd Law Viol
        "40I3060",  # CR Ret-Treatment Center
        "40I3070",  # CR Return-Work Release
        "95O2100",  # Prob Rev-Technical-Jail
        "95O2105",  # Prob Rev-New Felony Conv-Jail
        "95O2120",  # Prob Rev-Codes Not Applicable
    ],
    StateSupervisionPeriodTerminationReason.SUSPENSION: [
        "65O2015",  # Court Probation Suspension
        "65O3015",  # Court Parole Suspension
    ],
    StateSupervisionPeriodTerminationReason.TRANSFER_TO_OTHER_JURISDICTION: [
        "75O3000",  # MO Field-Interstate Transfer
        "75O3010",  # MO Board-Interstate Transfer
    ],
    StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE: [
        # This person has not actually been discharged -they are on lifetime supervision
        "95O1020",  # Court Prob Comp-Lifetime Supv
        "95O2060",  # Parole / CR Comp-Lifetime Supv
    ],
}


SUPERVISION_PERIOD_ADMISSION_REASON_TO_STR_MAPPINGS: Dict[
    StateSupervisionPeriodAdmissionReason, List[str]
] = {
    StateSupervisionPeriodAdmissionReason.ABSCONSION: [
        "65O1010",  # Parole Absconder
        "65O1020",  # Conditional Release Absconder
        "65O1030",  # Adm Parole Release Absconder
        "65L9100",  # Offender Declared Absconder
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
        "90O1070",  # Director's Rel Comp-Life Supv
    ],
    StateSupervisionPeriodAdmissionReason.COURT_SENTENCE: [
        "15I1000",  # New Court Probation
        "15I1200",  # New Court Parole
        "15I2000",  # New Diversion Supervision
        "25I1000",  # Court Probation-Addl Charge
        "25I1200",  # Court Parole-Addl Charge
        "25I2000",  # Diversion Supv-Addl Charge
        "35I1000",  # Court Probation-Revisit
        "35I1200",  # Court Parole-Revisit
        "35I2000",  # Diversion Supv-Revisit
        "35I4000",  # IS Compact-Prob-Revisit
        "35I4100",  # IS Compact-Parole-Revisit
        *PAPERS_ONLY_SUPERVISION_START_STATUSES,
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
    StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN: [
        "35I4010",  # IS Comp-Unsup/Priv PB-Revisit
        "35I6010",  # Release from DMH for SVP Supv
        "35I6020",  # Lifetime Supervision Revisit
        "60O1050",  # Rev/Rem Conv-Pending
        "60O3040",  # Erroneous Rel-Oth Jurisdiction
        "60O3050",  # Erroneous Rel-DAI Direct Rel
        "60O5010",  # Escape-Perimeter Confinement
        "60O5020",  # Escape-While in Transit
        "60O5030",  # Escape-Supervised Outcount
        "60O5040",  # Escape-Court Appearance
        "60O5050",  # Escape-Off Site Hospital
        "60O6010",  # Walkaway From CRC
        "60O6020",  # Furlough Return Failure
        "60O6025",  # Extended Limits Return Failure
        "60O6030",  # Work Release Return Failure
        "60O6040",  # Unsupervised Outcount Failure
        "60O6050",  # Other Release Return Failure
        "60O6110",  # BD Walkaway From CRC
        "60O6125",  # BD Ext Limits Return Failure
        "60O6130",  # BD Work Release Return Failure
        "60O6140",  # BD Unsupv Outcount Return Fail
        "60O6150",  # BD Other Release Return Fail
        "60O6210",  # PB Walkaway from CRC
        "60O6225",  # PB Ext Limits Return Failure
        "60O6230",  # PB Work Release Return Failure
        "60O6240",  # PB Unsupv Outcount Return Fail
        "60O6250",  # PB Other Release Return Fail
        "70O3010",  # MO Inmate-Interstate Transfer
        "70O3020",  # Federal Transfer
        "70O3030",  # Out of Custody Arrested
        "70O3040",  # CC Papers Only - Erron Rel
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

    statuses = [
        UsMoSentenceStatus(
            status_date=None, status_code=status, status_description="", sequence_num=0
        )
        for status in sorted_list_from_str(raw_text, ",")
    ]

    filtered_statuses = [
        status
        for status in statuses
        if (
            status.is_absconsion_status
            or status.is_supervision_in_status
            or status.is_incarceration_out_status
            or status.status_code in PAPERS_ONLY_SUPERVISION_START_STATUSES
        )
        and not status.is_investigation_status
        and status.status_code not in BOND_STATUSES
        and not status.is_sentence_termimination_status
    ]
    if not filtered_statuses:
        return StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE

    def status_rank(status: UsMoSentenceStatus) -> int:
        """In the case that there are multiple statuses on the same day, we pick the
        status that is most likely to give us accurate info about the reason this
        supervision period was started. In the case of supervision period admissions,
        we pick statuses that have the pattern X5I* (e.g. '15I1000'), since those
        statuses are field (5) IN (I) statuses. In the absence if one of those statuses,
        we get our info from other statuses.
        """
        if status.is_must_verify_status:
            return 4

        if status.status_code in PAPERS_ONLY_SUPERVISION_START_STATUSES:
            return 3

        if status.is_absconsion_status:
            return 2

        if status.is_incarceration_out_status:
            # Sometimes people just have a status that indicates they left incarceration
            # and entering supervision is implied, but we don't actually see a *5I*
            # supervision in IN status.
            return 1

        if status.is_supervision_in_status:
            return 0

        raise ValueError(
            f"Found status code which does not fall into one of the expected "
            f"categories: [{status.status_code}]"
        )

    sorted_statuses = sorted(
        filtered_statuses,
        key=lambda status: _status_rank_str(
            status,
            status_rank,
        ),
    )

    primary_status = sorted_statuses[0]
    status_code = primary_status.status_code

    # These statuses are usually ephemeral and will be updated to a more correct status
    # soon, but indicate that MO is not yet certain about what happened.
    if primary_status.is_must_verify_status:
        return StateSupervisionPeriodAdmissionReason.EXTERNAL_UNKNOWN

    if status_code not in STR_TO_SUPERVISION_PERIOD_ADMISSION_REASON_MAPPINGS:
        raise ValueError(
            f"Found primary status code with no known admission reason mapping: "
            f"[{status_code}]. Original raw text: [{raw_text}]"
        )
    return STR_TO_SUPERVISION_PERIOD_ADMISSION_REASON_MAPPINGS[status_code]


def parse_supervision_period_termination_reason(
    raw_text: str,
) -> Optional[StateSupervisionPeriodTerminationReason]:
    """Maps |raw_text|, a space delimited list of statuses from TAK026, to the most relevant
    SupervisionPeriodTerminationReason, when possible.

    If the status list is empty, we assume that this period ended because the person transferred between POs or offices.
    """
    if not raw_text:
        return None

    statuses = [
        UsMoSentenceStatus(
            status_date=None, status_code=status, status_description="", sequence_num=0
        )
        for status in sorted_list_from_str(raw_text, ",")
    ]

    filtered_statuses = [
        status
        for status in statuses
        if (
            status.is_absconsion_status
            or status.is_incarceration_in_status
            or status.is_supervision_out_status
            or status.is_cycle_termination_status
        )
        and not status.is_investigation_status
        and status.status_code not in BOND_STATUSES
    ]
    if not filtered_statuses:
        return StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE

    def status_rank(status: UsMoSentenceStatus) -> int:
        """In the case that there are multiple statuses on the same day, we pick the
        status that is most likely to give us accurate info about the reason this
        supervision period was terminated. In the case of supervision period
        terminations, we pick statuses first that have the pattern 99O* (e.g. '99O9020'),
        since those statuses always end a whole offender cycle, then statuses with
        pattern X5O*, since those statuses are field (5) OUT (O) statuses. In the
        absence if one of those statuses, we get our info from other statuses.
        """
        if status.is_cycle_termination_status:
            return 0
        if (
            status.is_supervision_out_status
            # We deprioritize sentence termination statuses because these usually do not
            # give us meaningful information about why this period ended because, unlike
            # cycle termination statuses, they do not indicate that the person has left
            # supervision.
            and not status.is_sentence_termimination_status
        ):
            return 1

        if status.is_incarceration_in_status:
            return 2

        if status.is_absconsion_status:
            return 3

        if status.is_must_verify_status:
            return 4

        if status.is_sentence_termimination_status:
            return 5

        raise ValueError(
            f"Found status code which does not fall into one of the expected "
            f"categories: [{status.status_code}]"
        )

    sorted_statuses = sorted(
        filtered_statuses, key=lambda status: _status_rank_str(status, status_rank)
    )

    primary_status = sorted_statuses[0]
    status_code = primary_status.status_code

    # If we see a sentence termination status but no cycle termination status on a given
    # day, usually this means something else is going on (e.g. a revocation that
    # terminates a sentence or a sentence investigation finishing while a new sentence
    # starts). In somewhat rare occasions we see no other helpful statuses other than
    # the sentence termination status and in this case we cannot determine a termination
    # reason.
    if (
        not primary_status.is_cycle_termination_status
        and primary_status.is_sentence_termimination_status
    ):
        return StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN

    # These statuses are usually ephemeral and will be updated to a more correct status
    # soon, but indicate that MO is not yet certain about what happened.
    if primary_status.is_must_verify_status:
        return StateSupervisionPeriodTerminationReason.EXTERNAL_UNKNOWN

    if status_code not in STR_TO_SUPERVISION_PERIOD_TERMINATION_REASON_MAPPINGS:
        print(
            f"Found primary status code with no known termination reason mapping: \n"
            f"[{status_code}]. Original raw text: [{raw_text}]"
        )
        raise ValueError(
            f"Found primary status code with no known termination reason mapping: "
            f"[{status_code}]. Original raw text: [{raw_text}]"
        )
    return STR_TO_SUPERVISION_PERIOD_TERMINATION_REASON_MAPPINGS[status_code]


def _status_rank_str(
    status: UsMoSentenceStatus, rank_fn: Callable[[UsMoSentenceStatus], int]
) -> str:
    return f"{str({rank_fn(status)}).zfill(3)}{status}"


def parse_staff_role_type(raw_text: str) -> Optional[StateStaffRoleType]:
    """Maps |raw_text|, a MO specific job title, to its corresponding StateStaffRoleType."""
    if not raw_text:
        return None
    if (
        ("PROBATION" in raw_text and "PAROLE" in raw_text)
        or "P&P" in raw_text
        or "P & P" in raw_text
    ):
        return StateStaffRoleType.SUPERVISION_OFFICER
    return StateStaffRoleType.INTERNAL_UNKNOWN


DOMESTIC_VIOLENCE_CASE_TYPES = [
    "DVS",  # Domestic Violence Supervision
    "DOM",  # Domestic Violence
    "Domestic Violence Offender",
]

SERIOUS_MENTAL_ILLNESS_CASE_TYPES = [
    "SMI",  # Seriously Mentally Ill Caseload
    "Seriously Mentally Ill Offender",
]

SEX_OFFENSE_CASE_TYPES = [
    "DSO",  # Designated Sex Offenders
    "ISO",  # Interstate Sex Offenders
    "Sex Offender",
]

UNCATEGORIZED_CASE_TYPES = [
    "ICTS Treatment Offender",
    "JRI Treatment Pilot Offender",
    "Parole or Condition Release Less than 12 months",
    "Specialty Court Offender",
    "Treatment Court Offender",
]


def parse_case_types(raw_text: str) -> Optional[StateSupervisionCaseType]:
    case_types = sorted_list_from_str(raw_text)

    for case_type in case_types:
        if case_type in DOMESTIC_VIOLENCE_CASE_TYPES:
            return StateSupervisionCaseType.DOMESTIC_VIOLENCE

        if case_type in SERIOUS_MENTAL_ILLNESS_CASE_TYPES:
            return StateSupervisionCaseType.SERIOUS_MENTAL_ILLNESS_OR_DISABILITY

        if case_type in SEX_OFFENSE_CASE_TYPES:
            return StateSupervisionCaseType.SEX_OFFENSE

        if case_type in UNCATEGORIZED_CASE_TYPES:
            return StateSupervisionCaseType.INTERNAL_UNKNOWN

    return None


INCARCERATION_PERIOD_ADMISSION_REASON_TO_STR_MAPPINGS: Dict[
    StateIncarcerationPeriodAdmissionReason, List[str]
] = {
    StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION: [
        *NEW_COURT_COMMITTMENT_STATUS_CODES,
        *COURT_COMMITMENT_REVISIT_STATUS_CODES,
        *BOND_RETURN_STATUS_CODES,
        *RETURN_POST_REMAND_STATUS_CODES,
        *NEW_ADMISSION_SECONDARY_STATUS_CODES,
    ],
    StateIncarcerationPeriodAdmissionReason.REVOCATION: [
        *PROBATION_REVOCATION_RETURN_STATUSES,
        *LEGACY_PROBATION_REENTRY_STATUS_CODES,
        *PROBATION_REVOCATION_SECONDARY_STATUS_CODES,
        *PAROLE_REVOKED_REENTRY_STATUS_CODES,
        *CONDITIONAL_RELEASE_RETURN_STATUS_CODES,
        *PAROLE_REVOKED_WHILE_INCARCERATED_STATUS_CODES,
    ],
    StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY: [
        *BOARD_HOLDOVER_ENTRY_STATUS_CODES
    ],
    StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION: [
        *SUPERVISION_SANCTION_COMMITMENT_FOR_TREATMENT_OR_SHOCK_STATUS_CODES,
    ],
    StateIncarcerationPeriodAdmissionReason.ADMITTED_IN_ERROR: [
        *ADMITTED_IN_ERROR_STATUS_CODES
    ],
    StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE: [
        *RETURN_FROM_ESCAPE_STATUS_CODES
    ],
    StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE: [
        *RETURN_FROM_ERRONEOUS_RELEASE_STATUS_CODES
    ],
    StateIncarcerationPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION: [
        *INSTITUTIONAL_TRANSFER_FROM_OUT_OF_STATE_STATUS_CODES
    ],
    StateIncarcerationPeriodAdmissionReason.TRANSFER: [
        *REDUCTION_OF_SENTENCE_REENTRY_STATUS_CODES
    ],
    StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE: [
        *MID_INCARCERATION_TREATMENT_FAILURE_STATUSES
    ],
}


STR_TO_INCARCERATION_PERIOD_ADMISSION_REASON_MAPPINGS: Dict[
    str, StateIncarcerationPeriodAdmissionReason
] = invert_enum_to_str_mappings(INCARCERATION_PERIOD_ADMISSION_REASON_TO_STR_MAPPINGS)


def rank_incarceration_period_admission_reason_status_str(
    status_str: str,
) -> Optional[int]:
    """Assigns an integer rank for a given status, where lower ranks are statuses that should be considered first when
    determining the admission reason.
    """
    if (
        status_str in PAROLE_REVOKED_REENTRY_STATUS_CODES
        or status_str in CONDITIONAL_RELEASE_RETURN_STATUS_CODES
        or status_str in PAROLE_REVOKED_WHILE_INCARCERATED_STATUS_CODES
        or status_str in PROBATION_REVOCATION_RETURN_STATUSES
        or status_str in LEGACY_PROBATION_REENTRY_STATUS_CODES
    ):
        # These are the main probation and parole revocation statuses
        return 0

    if (
        status_str in NEW_COURT_COMMITTMENT_STATUS_CODES
        or status_str in COURT_COMMITMENT_REVISIT_STATUS_CODES
        or status_str in BOND_RETURN_STATUS_CODES
        or status_str in RETURN_POST_REMAND_STATUS_CODES
    ):
        # These are the main NEW_ADMISSION statuses
        return 1

    if (
        status_str
        in SUPERVISION_SANCTION_COMMITMENT_FOR_TREATMENT_OR_SHOCK_STATUS_CODES
    ):
        # These are codes that count as a SANCTION_ADMISSION when there are no other
        # new admission or revocation statuses present.
        return 2

    if status_str in PROBATION_REVOCATION_SECONDARY_STATUS_CODES:
        # These are codes that count as a PROBATION_REVOCATION when there are no other
        # new admission, revocation, or sanction admission statuses present.
        return 3

    if status_str in MID_INCARCERATION_TREATMENT_FAILURE_STATUSES:
        #  These are codes that indicate a STATUS_CHANGE, such as a failure of
        #  treatment, when there are no other new admission or revocation statuses
        #  present
        return 4

    if status_str in NEW_ADMISSION_SECONDARY_STATUS_CODES:
        # These status codes are sometimes the only real admissions status, though are more rare. Should not take
        # precedent over other new admission / revocation statuses.
        return 5

    if status_str in BOARD_HOLDOVER_ENTRY_STATUS_CODES:
        # These are TEMPORARY_CUSTODY statuses
        return 6

    if status_str in INSTITUTIONAL_TRANSFER_FROM_OUT_OF_STATE_STATUS_CODES:
        return 7

    if (
        status_str in RETURN_FROM_ESCAPE_STATUS_CODES
        or status_str in ADMITTED_IN_ERROR_STATUS_CODES
        or status_str in RETURN_FROM_ERRONEOUS_RELEASE_STATUS_CODES
    ):
        # These are other statuses that don't / shouldn't show up together, should be considered after revocation,
        # transfer, and new admission statuses.
        return 8

    if status_str in REDUCTION_OF_SENTENCE_REENTRY_STATUS_CODES:
        return 9

    # This status code does not give us good info about the admission reason
    return None


def parse_incarceration_period_admission_reason(
    raw_text: str,
) -> StateIncarcerationPeriodAdmissionReason:
    """Converts a string with a list of TAK026 MO status codes into a valid incarceration period admission reason."""
    start_statuses = sorted_list_from_str(raw_text, ",")

    ranked_status_map: Dict[int, List[str]] = {}

    # First rank all statuses individually
    for status_str in start_statuses:
        status_rank = rank_incarceration_period_admission_reason_status_str(status_str)
        if status_rank is None:
            # If None, this is not an status code for determining the admission status
            continue
        if status_rank not in ranked_status_map:
            ranked_status_map[status_rank] = []
        ranked_status_map[status_rank].append(status_str)

    if not ranked_status_map:
        # None of the statuses can meaningfully tell us what the admission reason is (rare)
        return StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN

    # Find the highest order status(es) and use those to determine the admission reason
    highest_rank = sorted(list(ranked_status_map.keys()))[0]
    statuses_at_rank = ranked_status_map[highest_rank]

    potential_admission_reasons: Set[StateIncarcerationPeriodAdmissionReason] = set()
    for status_str in statuses_at_rank:
        if status_str not in STR_TO_INCARCERATION_PERIOD_ADMISSION_REASON_MAPPINGS:
            raise ValueError(
                f"No mapping for incarceration admission status {status_str}"
            )
        potential_admission_reasons.add(
            STR_TO_INCARCERATION_PERIOD_ADMISSION_REASON_MAPPINGS[status_str]
        )

    if len(potential_admission_reasons) > 1:
        raise EnumParsingError(
            StateIncarcerationPeriodAdmissionReason,
            f"Found status codes with conflicting information: [{statuses_at_rank}], which evaluate to "
            f"[{potential_admission_reasons}]",
        )

    return one(potential_admission_reasons)


INCARCERATION_RELEASE_REASONS_SENTENCE_SERVED: List[str] = [
    # TODO(#2898) - Use TAK026 statuses to populate release reason
    "DC-DC",  # Discharge
    "DC-DO",  # Inst. Converted-Inactive
    "DC-XX",  # Discharge - Unknown
    "ID-DC",  # Institutional Discharge - Discharge
    "ID-DO",  # Institutional Discharge - Other
    "ID-DR",  # Institutional Discharge - Director's Release
    "ID-ID",  # Institutional Discharge - Institutional Discharge
    "ID-PD",  # Institutional Discharge - Pardoned
    "ID-RR",  # Institutional Discharge - Reversed and Remanded
    "ID-XX",  # Institutional Discharge - Unknown
]

INCARCERATION_RELEASE_REASONS_INTERNAL_UNKNOWN: List[str] = [
    "CN-FB",  # Committed New Charge- No Vio: seems erroneous
    "CN-NV",
    "RV-FF",  # Revoked: seems erroneous
    "RV-FM",
    "RV-FT",
]

INCARCERATION_RELEASE_REASONS_EXTERNAL_UNKNOWN: List[str] = [
    # TODO(#2898) - Use TAK026 statuses to populate release reason
    "XX-XX",  # Unknown (Not Associated)
    "??-??",  # Code Unknown
    "??-XX",
]

INCARCERATION_RELEASE_REASONS_EXECUTION: List[str] = [
    # TODO(#2898) - Use TAK026 statuses to populate release reason
    "DE-EX",  # Execution
]

INCARCERATION_RELEASE_REASONS_ESCAPE: List[str] = [
    # TODO(#2898) - Use TAK026 statuses to populate release reason
    "IE-IE",  # Institutional Escape
    "IE-XX",
    "IW-IW",  # Institutional Walkaway
    "IW-XX",
]

INCARCERATION_RELEASE_REASONS_DEATH: List[str] = [
    # TODO(#2898) - Use TAK026 statuses to populate release reason
    "DE-DE",  # Death
    "DE-XX",
]

INCARCERATION_RELEASE_REASONS_COURT_ORDER: List[str] = [
    # TODO(#2898) - Use TAK026 statuses to populate release reason
    "IB-EI",  # Institutional Administrative
    "IB-ER",
    "IB-IB",
    "IB-RB",
    "IB-RR",
    "IB-TR",
    "IB-XX",
    "OR-OR",  # Off Records; Suspension
]

INCARCERATION_RELEASE_REASONS_CONDITIONAL_RELEASE: List[str] = [
    # TODO(#2898) - Use TAK026 statuses to populate release reason
    "BP-FF",  # Board Parole
    "BP-FM",
    "BP-FT",
    "FB-TR",  # Field Administrative
    "CR-FF",  # Conditional Release
    "CR-FT",
    "IT-BD",  # Institutional Release to Supervision
    "IT-BH",
    "IT-BP",
    "IT-CD",
    "IT-CR",
    "IT-EM",
    "IT-IC",
    "IT-IN",
    "IT-RF",
    "IC-IC",  # Institutional Release to Probation
    "RT-BH",  # Board Return
    # Inmate Release to EMP (electronic monitoring program). Only 1
    # occurrence of any of these in the last 10 years.
    "EM-FB",
    "EM-FF",
    "EM-FM",
    "EM-FT",
]


def parse_incarceration_period_termination_reason(
    raw_text: str,
) -> StateIncarcerationPeriodReleaseReason:
    """Parse admission reason from raw text"""

    if raw_text.upper() == "NONE@@NONE@@NONE":
        return StateIncarcerationPeriodReleaseReason.TRANSFER
    (
        END_SCD_CODES,
        END_STATUS_CODE,
        END_STATUS_SUBTYPE,
    ) = raw_text.upper().split("@@")

    end_statuses = sorted_list_from_str(END_SCD_CODES, ",")

    end_status_code_subtype = END_STATUS_CODE + "-" + END_STATUS_SUBTYPE

    for status_str in end_statuses:
        if (
            status_str
            in PAROLE_REVOKED_WHILE_INCARCERATED_STATUS_CODES
            + MID_INCARCERATION_TREATMENT_COMMITMENT_STATUSES
        ):
            return StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY
        if status_str in MID_INCARCERATION_TREATMENT_FAILURE_STATUSES:
            return StateIncarcerationPeriodReleaseReason.STATUS_CHANGE

    if end_status_code_subtype in INCARCERATION_RELEASE_REASONS_SENTENCE_SERVED:
        return StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED

    if end_status_code_subtype in INCARCERATION_RELEASE_REASONS_INTERNAL_UNKNOWN:
        return StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN

    if end_status_code_subtype in INCARCERATION_RELEASE_REASONS_EXTERNAL_UNKNOWN:
        return StateIncarcerationPeriodReleaseReason.EXTERNAL_UNKNOWN

    if end_status_code_subtype in INCARCERATION_RELEASE_REASONS_EXECUTION:
        return StateIncarcerationPeriodReleaseReason.EXECUTION

    if end_status_code_subtype in INCARCERATION_RELEASE_REASONS_ESCAPE:
        return StateIncarcerationPeriodReleaseReason.ESCAPE

    if end_status_code_subtype in INCARCERATION_RELEASE_REASONS_DEATH:
        return StateIncarcerationPeriodReleaseReason.DEATH

    if end_status_code_subtype in INCARCERATION_RELEASE_REASONS_COURT_ORDER:
        return StateIncarcerationPeriodReleaseReason.COURT_ORDER

    if end_status_code_subtype in INCARCERATION_RELEASE_REASONS_CONDITIONAL_RELEASE:
        return StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE

    return StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN


INCARCERATION_INCIDENT_TYPE_TO_STR_MAPPINGS: Dict[
    StateIncarcerationIncidentType, List[str]
] = {
    StateIncarcerationIncidentType.CONTRABAND: [
        "3",  # DANGEROUS CONTRABAND
        "13",  # POSSESSION OF MONEY OR LEGAL TENDER
        "24",  # CONTRABAND
    ],
    StateIncarcerationIncidentType.DISORDERLY_CONDUCT: [
        "6",  # RIOT
        "9",  # ORGANIZED DISOBEDIENCE
        "19",  # CREATING A DISTURBANCE
        "20",  # DISOBEYING AN ORDER
        "30",  # OUT OF BOUNDS
        "34",  # UNAUTHORIZED ORGANIZATIONS
    ],
    StateIncarcerationIncidentType.ESCAPE: ["4"],  # ESCAPE
    StateIncarcerationIncidentType.VIOLENCE: [
        "1",  # MURDER OR MANSLAUGHTER
        "2",  # ASSAULT
        "5",  # HOSTAGE OR RESTRAINT
        "7",  # FORCIBLE SEXUAL ABUSE
        "10",  # MINOR ASSAULT
        "12",  # THREATS
        "25",  # FIGHTING
    ],
}

STR_TO_INCARCERATION_INCIDENT_TYPE_MAPPINGS: Dict[
    str, StateIncarcerationIncidentType
] = invert_enum_to_str_mappings(INCARCERATION_INCIDENT_TYPE_TO_STR_MAPPINGS)


def parse_incarceration_incident_type(raw_text: str) -> StateIncarcerationIncidentType:
    """Parse incarceration incident type from raw text."""

    major_rule = raw_text.split(".")[0]

    if raw_text in ["11.1", "11.2", "11.3", "11.4", "11.9", "11.10", "16.2", "28.2"]:
        return StateIncarcerationIncidentType.CONTRABAND

    if major_rule in STR_TO_INCARCERATION_INCIDENT_TYPE_MAPPINGS:
        return STR_TO_INCARCERATION_INCIDENT_TYPE_MAPPINGS[major_rule]

    return StateIncarcerationIncidentType.INTERNAL_UNKNOWN


# TODO(#28189): Expand status code knowledge/coverage for supervision type classification
def get_recidiviz_sentence_status(raw_text: str) -> StateSentenceStatus:
    """This is a custom parser to get a StateSentenceStatus.
    If a status is not commuted, suspended, or completed, then it is serving.
    Args:
        raw_text: (str) Concatenation of status code and description, separated by '@@'
                  We pass in the status description to use in normalization!
    Returns:
        StateSentenceStatus
    """
    code, description = raw_text.split("@@")
    status = UsMoSentenceStatus(
        status_date=None,
        status_code=code,
        status_description=description,
        sequence_num=0,
    )
    return status.get_state_sentence_status()


# TODO(#28189): Expand status code knowledge/coverage for supervision type classification.
# We default to PROBATION
def parse_state_sentence_type_from_supervision_status(
    raw_text: str,
) -> StateSentenceType:
    """Returns the StateSentenceType for a StateSentence based on the status code at imposition."""
    status_code, status_description = raw_text.split("@@")
    status = UsMoSentenceStatus(
        status_date=None,
        sequence_num=0,
        status_code=status_code,
        status_description=status_description,
    )
    sentence_type = status.state_sentence_type
    if (
        sentence_type != StateSentenceType.INTERNAL_UNKNOWN
        and sentence_type is not None
    ):
        return sentence_type
    # We assume everything from the TAK024 (Sentence Prob) is
    # in fact, PROBATION. Unless it has been parsed otherwise above.
    return StateSentenceType.PROBATION


def get_sentence_type(raw_text: str) -> StateSentenceType:
    """Returns the sentence type at imposition.

    Defaults to INTERNAL_UNKOWN, which happens if
    it the initial status is not an incarceration in/out status
    AND we couldn't distinguish from PROBATION or PAROLE.

    Args:
        raw_text: initial_status_code@@initial_status_desc
    """
    initial_status_code, initial_status_desc = raw_text.split("@@")
    status = UsMoSentenceStatus(
        status_date=None,
        sequence_num=0,
        status_code=initial_status_code,
        status_description=initial_status_desc,
    )
    return status.state_sentence_type


def parse_supervision_sentencing_authority(raw_text: str) -> StateSentencingAuthority:
    """
    Returns the StateSentencingAuthority based on the given
    sentencing county code and imposition status description.
    MODOC denotes interstate compact sentences by:
        - Marking text field BS_CNS as "OTST" (most of the time)
        - Having a sentencing status begin with "IS Comp"
    For debugging, interstate compact sentences should also have projected
    dates of all '8's. However, "indeterminate" sentences may also have
    those dates-so it is not a guarantee for parsing.
    """
    county_code, imposition_status_desc = raw_text.split("@@")
    if "IS COMP" in imposition_status_desc.upper():
        return StateSentencingAuthority.OTHER_STATE
    return sentencing_authority_from_county(county_code)


def sentencing_authority_from_county(raw_text: str) -> StateSentencingAuthority:
    """Returns sentencing authority assuming the given raw_text is a MODOC county code."""
    if raw_text.upper() == "OTST":
        return StateSentencingAuthority.OTHER_STATE
    if not raw_text:
        return StateSentencingAuthority.PRESENT_WITHOUT_INFO
    return StateSentencingAuthority.COUNTY


def parse_employment_status(raw_text: str) -> StateEmploymentPeriodEmploymentStatus:
    """
    Maps job type / category codes to employment status enums.
    IMPORTANT: The ALTERNATE_INCOME_SOURCE and UNABLE_TO_WORK statuses are relevant for
    MO Tasks contact requirements. If mapping additional raw text values to these enums
    in the future, make sure to also add them to EMPLOYMENT_TYPES_TO_PRIORITIZE in the employment
    periods view, to ensure that if statuses overlap, we prioritize the status with downstream significance.
    """
    employment_type, employment_category = raw_text.split("@@")
    if employment_type in [
        "L15",  # STUDENT-ACADEMIC
        "L39",  # STUDENT-VOCATIONAL
        "L48",  # STUDENT-YOUTHFUL OFFENDER PROGRAM
    ]:
        return StateEmploymentPeriodEmploymentStatus.STUDENT

    if employment_type in [
        "L06",  # GOVERNMENT ASSISTANCE
        "L09",  # HOMEMAKER
        "L12",  # RETIREE
    ]:
        return StateEmploymentPeriodEmploymentStatus.ALTERNATE_INCOME_SOURCE

    if employment_type == "L03":  # DISABLED
        return StateEmploymentPeriodEmploymentStatus.UNABLE_TO_WORK

    # If an employment type code doesn't fall into the above categories, but has a category
    # of NON ("non-workforce"), then map to UNEMPLOYED.
    if employment_category == "NON":  # NON-WORKFORCE
        return StateEmploymentPeriodEmploymentStatus.UNEMPLOYED

    return StateEmploymentPeriodEmploymentStatus.EMPLOYED_UNKNOWN_AMOUNT
