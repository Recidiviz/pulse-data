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
"""US_MO specific enum helper methods.

TODO(#8899): This file should become empty and be deleted when we have fully migrated
 this state to new ingest mappings version.
"""
from typing import Callable, Dict, List, Optional, Set

from more_itertools import one

from recidiviz.common.constants.entity_enum import EnumParsingError
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.str_field_utils import sorted_list_from_str
from recidiviz.ingest.direct.legacy_ingest_mappings.direct_ingest_controller_utils import (
    invert_enum_to_str_mappings,
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
]

COURT_COMMITMENT_REVIST_STATUS_CODES: List[str] = [
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
    # Probation sanction admission for treatment
    "20I1020",  # Court Comm-Lng Tm Trt-Addl Chg
    "20I1040",  # Court Comm-120 Day Treat-Addl
    "20I1060",  # Court Comm-MH 120 Day-Addl Chg
    "40I2100",  # Prob Rev-Tech-120 Day Treat
    "40I7060",  # Prob Adm-Post Conv-Trt Pgm
    "40I7030",  # Prob Adm-Mental Health 120 Day
    #  Parole returns for treatment
    "40I1060",  # Parole Ret-Treatment Center
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


INCARCERATION_PERIOD_ADMISSION_REASON_TO_STR_MAPPINGS: Dict[
    StateIncarcerationPeriodAdmissionReason, List[str]
] = {
    StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION: [
        *NEW_COURT_COMMITTMENT_STATUS_CODES,
        *COURT_COMMITMENT_REVIST_STATUS_CODES,
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
        or status_str in COURT_COMMITMENT_REVIST_STATUS_CODES
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


def incarceration_period_admission_reason_mapper(
    status_list_str: str,
) -> StateIncarcerationPeriodAdmissionReason:
    """Converts a string with a list of TAK026 MO status codes into a valid incarceration period admission reason."""
    start_statuses = sorted_list_from_str(status_list_str, " ")

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


def supervising_officer_mapper(label: str) -> Optional[StateAgentType]:
    """Maps |label|, a MO specific job title, to its corresponding StateAgentType."""
    if not label:
        return None

    if ("PROBATION" in label and "PAROLE" in label) or "P P" in label:
        return StateAgentType.SUPERVISION_OFFICER
    return StateAgentType.INTERNAL_UNKNOWN


def _status_rank_str(status: str, rank_fn: Callable[[str], int]) -> str:
    return f"{str({rank_fn(status)}).zfill(3)}{status}"
