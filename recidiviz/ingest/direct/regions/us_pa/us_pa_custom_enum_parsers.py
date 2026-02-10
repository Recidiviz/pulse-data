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
"""Custom enum parsers functions for US_PA. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_pa_custom_enum_parsers.<function name>
"""
import datetime
from collections import OrderedDict
from typing import Dict, List, Optional

from recidiviz.common import str_field_utils
from recidiviz.common.constants.state.state_assessment import StateAssessmentLevel
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_person import StateResidencyStatus
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_staff_role_period import (
    StateStaffRoleSubtype,
)
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    get_most_relevant_supervision_type,
)
from recidiviz.common.str_field_utils import parse_int
from recidiviz.ingest.direct.regions.custom_enum_parser_utils import (
    invert_enum_to_str_mappings,
)

# dictionary of specific dates for set_date_specific_lsir_fields
_DATE_SPECIFIC_ORDERED_LSIR_LEVELS = OrderedDict(
    [
        (
            datetime.date(2008, 12, 31),
            {
                20: StateAssessmentLevel.LOW,
                28: StateAssessmentLevel.MEDIUM,
                54: StateAssessmentLevel.HIGH,
            },
        ),
        (
            datetime.date(2014, 12, 3),
            {
                17: StateAssessmentLevel.LOW,
                26: StateAssessmentLevel.MEDIUM,
                54: StateAssessmentLevel.HIGH,
            },
        ),
        (
            datetime.date.max,
            {
                19: StateAssessmentLevel.LOW,
                27: StateAssessmentLevel.MEDIUM,
                54: StateAssessmentLevel.HIGH,
            },
        ),
    ]
)

INCARCERATION_PERIOD_ADMISSION_REASON_TO_MOVEMENT_CODE_MAPPINGS: Dict[
    StateIncarcerationPeriodAdmissionReason, List[str]
] = {
    StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN: [
        # SCI CODES
        "X",  # Unknown
    ],
    StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN: [
        # SCI CODES
        "RTN",  # (Not in PA data dictionary, no instances after 1996)
        # SCI CODES
        # TODO(#8346): There are a small amount of SCI admissions in US_PA that have
        #  revocation statuses but are not classified as revocations due to the previous
        #  SCI status for the person also being a revocations status. This represents
        #  less than 1% of all non-transfer admissions, and we are classifying them as
        #  INTERNAL_UNKNOWN because it's not actually clear what's happening when we
        #  see these statuses on admissions that are neither parole board holds nor
        #  revocations.
        "APD",  # Parole Detainee
    ],
    StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION: [
        # SCI CODES
        "AB",  # Bail
        "AC",  # Court Commitment
        "ACT",  # County Transfer (transferred from a county jail, newly in DOC custody)
        "ADET",  # Detentioner
        "AFED",  # Federal Commitment
        "AOTH",  # Other - Use Sparingly
    ],
    StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE: [
        # SCI CODES
        "AE",  # Escape
    ],
    StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION: [
        # SCI CODES
        # TODO(#8346): There are a small amount of SCI admissions in US_PA that have
        #  revocation statuses but are not classified as revocations due to the previous
        #  SCI status for the person also being a revocations status. This represents
        #  less than 1% of all non-transfer admissions, and we previously classified them as
        #  INTERNAL_UNKNOWN because it's not actually clear what's happening when we
        #  see these statuses on admissions that are neither parole board holds nor
        #  revocations. With span refactoring, we are moving this to sanction admission, but may need investigation in the future.
        "APV",  # Parole Violator
    ],
    StateIncarcerationPeriodAdmissionReason.TRANSFER: [
        # CCIS CODES
        # Return from DPW: This is used a status change indicating the person's stay in the facility is now being
        # funded by the DOC
        "DPWF",
        # In Residence: Admitted to CCIS facility. If this has not been classified as a revocation admission, then
        # this is a transfer from another CCIS facility.
        "INRS",
        # Program Change: Transfer between programs
        "PRCH",
        # Return to Residence: Returned to facility from either a temporary medical transfer or after absconding
        # from the facility
        "RTRS",
        "TRRC",  # Transfer Received: Transferred to facility from another DOC-funded facility
        # SCI CODES
        # This is an 'Add Administrative' which will generally follow a 'Delete Administrative' ('DA') directly
        # and is used to do some sort of record-keeping change without meaning this person went anywhere.
        "AA",  # Add - Administrative
        # Old, similar usage to 'AA'
        "AIT",  # Add - In Transit
        # TODO(#2002): This status represents that this person was returning from a long stay in a state hospital,
        #  it generally follows a 'D' movement code with sentence status code 'SH'. Ideally we'd specify that this
        #  was a transfer from a hospital
        "ASH",  # Add - State Hospital
        "ATT",  # Add - [Unlisted Transfer]
        # It was unclear from talking to PA what this means, but since it hasn't shown up in years, we're leaving
        # it here.
        "AW",  # Add - WRIT/ATA (Writ of Habeas Corpus Ad Prosequendum)
        "PLC",  # Permanent Location Change
        "DTT",  # Unlisted Transfer
        "RTT",  # Return Temporary Transfer
        "STT",  # Send Temporary Transfer
        "TFM",  # From Medical Facility
        "TRN",  # To Other Institution Or CCC
        "TTM",  # To Medical Facility
        "XPT",  # Transfer Point
        # In this context, SC is being used as a transfer from one type of
        # incarceration to another, either between facilities or within the same facility
        "SC",  # Status Change
    ],
    StateIncarcerationPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION: [
        # SCI CODES
        # TODO(#10502): I think this is a person from another state who PA is holding for some short-ish
        #  period of time until they are sent back to that state - need to confirm. They are not being tried by the
        #  PA Parole Board and will have a 'NA' (Not Applicable) parole_stat_cd (parole status code). (ASK PA)
        "AOPV",  # Out Of State Probation/Parole Violator
    ],
}

INCARCERATION_PERIOD_RELEASE_REASON_TO_STR_MAPPINGS: Dict[
    StateIncarcerationPeriodReleaseReason, List[str]
] = {
    StateIncarcerationPeriodReleaseReason.COMMUTED: [
        # SCI CODES
        "RD",  # Release Detentioner
    ],
    StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE: [
        # CCIS CODES
        "DC2P",  # Discharge to Parole: Released from a CCIS facility, still on supervision
        "PTST",  # Parole to Street: Released from a facility to a PBPP approved home plan
        # SCI CODES
        "RP",  # Re-Parole (Paroled for the non-first time in this sentence group)
        "SP",  # State Parole (Paroled for the first time in this sentence group)
        "P",  # Paroled (relatively rare)
    ],
    StateIncarcerationPeriodReleaseReason.DEATH: [
        # CCIS CODES
        "DECA",  # Deceased - Assault
        "DECN",  # Deceased - Natural
        "DECS",  # Deceased - Suicide
        "DECX",  # Deceased - Accident
        # SCI CODES
        "DA",  # Deceased - Assault
        "DN",  # Deceased - Natural
        "DS",  # Deceased - Suicide
        "DX",  # Deceased - Accident
        "DZ",  # Deceased - Non DOC Location
    ],
    StateIncarcerationPeriodReleaseReason.EXECUTION: [
        # SCI CODES
        "EX",  # Executed
    ],
    StateIncarcerationPeriodReleaseReason.EXTERNAL_UNKNOWN: [
        # SCI CODES
        "AOTH",  # Other - Use Sparingly
        "X",  # Unknown
        "O",  # Unknown
    ],
    StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN: [
        # CCIS CODES
        "ATA",  # Authorized Temporary Absence (very rare, supposed to be followed by a RTRS status, but usually isn't)
        # SCI CODES
        "AA",  # Administrative
        "AB",  # Bail
        "AC",  # Court Commitment
        "ADET",  # Detentioner
        "AFED",  # Federal Commitment
        "APD",  # Parole Detainee
        "AS",  # Actively Serving
        "CS",  # Change Other Sentence
        "RTN",  # (Not in PA data dictionary, no instances after 1996)
        "W",  # Waiting
    ],
    StateIncarcerationPeriodReleaseReason.ESCAPE: [
        # CCIS CODES
        "ABSC",  # Parole Absconder: They have left the facility
        # Awaiting Transfer - Non Report: This person was released from a facility and never showed up at the next
        # facility
        "AWNR",
        "AWOL",  # Unauthorized temporary Absence: Didn't return after being temporarily released
        "ESCP",  # Escape
        # SCI CODES
        "AE",  # Escape
        "EC",  # Escape CSC
        "EI",  # Escape Institution
    ],
    StateIncarcerationPeriodReleaseReason.PARDONED: [
        # SCI CODES
        "PD",  # Pardoned
    ],
    StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY: [
        # SCI CODES
        "APV",  # Parole Violator
        "AOPV",  # Out Of State Probation/Parole Violator
    ],
    StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION: [
        # SCI CODES
        "RE",  # Received In Error
    ],
    StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED: [
        # CCIS CODES
        "SENC",  # Sentence Completed
        # SCI CODES
        "B",  # Bailed
        "FR",  # Federal Release
        "NC",  # Non-return CSC
        "NF",  # Non-return Furlough
        "NR",  # [Unlisted]
        "NW",  # Non-return Work Release
        "PC",  # Program Completed
    ],
    StateIncarcerationPeriodReleaseReason.TRANSFER: [
        # CCIS CODES
        # Transfer to DPW: This is used a status change indicating the person's stay in the facility is now being
        # funded by the DOC
        "DPWT",
        # TODO(#2002): Count people on temporary medical transfers in the DOC population
        "HOSP",  # Hospital: Temporary medical transfer
        # Program Change: Transfer between programs
        "PRCH",
        # Transfer from Group Home: Although this sounds like an admission, this is a transfer out to another kind of
        # facility
        "TRGH",
        "TRSC",  # Transfer to SCI: Transfer to SCI for violations, investigations, medical, etc.
        # Unsuccessful Discharge: Removed from parole for rule violations or significant incidents. Sent to an SCI.
        "UDSC",
        # Transfer to County: Transferred from a contracted community facility that is not a county jail to a
        # contracted county jail
        "TRTC",
        # TODO(#2002): Count people on temporary medical transfers in the DOC population
        "TTRN",  # Temporary Transfer - Medical
        # SCI CODES
        "ACT",  # County Transfer
        "AIT",  # In Transit
        "ASH",  # State Hospital
        "ATT",  # [Unlisted Transfer]
        "AW",  # WRIT/ATA (Writ of Habeas Corpus Ad Prosequendum)
        "PLC",  # Permanent Location Change
        "RTT",  # Return Temporary Transfer
        "STT",  # Send Temporary Transfer
        "TFM",  # From Medical Facility
        "TRN",  # To Other Institution Or CCC
        "TTM",  # To Medical Facility
        "XPT",  # Transfer Point
        # In this context, SC is being used as a transfer from one type of
        # incarceration to another, either between facilities or within the same facility
        "SC",  # Status Change
        # From Sentence Status Codes
        "F",  # Furloughed
        "SH",  # State Hospital
        "TC",  # Transfer to County
        "WT",  # WRIT/ATA
    ],
    StateIncarcerationPeriodReleaseReason.TRANSFER_TO_OTHER_JURISDICTION: [
        # SCI CODES
        "IC",  # In Custody Elsewhere
        "TS",  # Transfer to Other State
    ],
    StateIncarcerationPeriodReleaseReason.VACATED: [
        # SCI CODES
        "VC",  # Vacated Conviction
        "VS",  # Vacated Sentence
    ],
}

STR_TO_INCARCERATION_PERIOD_RELEASE_REASON_MAPPINGS: Dict[
    str, StateIncarcerationPeriodReleaseReason
] = invert_enum_to_str_mappings(INCARCERATION_PERIOD_RELEASE_REASON_TO_STR_MAPPINGS)


def _retrieve_release_reason_mapping(
    code: str,
) -> StateIncarcerationPeriodReleaseReason:
    release_reason = STR_TO_INCARCERATION_PERIOD_RELEASE_REASON_MAPPINGS.get(code, None)
    if not release_reason:
        raise ValueError(f"No mapping for incarceration period release reason {code}")
    return release_reason


SUPERVISION_PERIOD_CUSTODIAL_AUTHORITY_TO_STR_MAPPINGS: Dict[
    StateCustodialAuthority, List[str]
] = {
    # mappings based on supervision type
    StateCustodialAuthority.SUPERVISION_AUTHORITY: [
        "02",  # Paroled from SCI to PBPP Supervision
        "03",  # Reparoled from SCI to PBPP Supervision
        "04",  # Sentenced to Probation by County Judge and Supervised by PBPP
        "05",  # Special Parole sentenced by County and Supervised by PBPP
        "4A",  # ARD (Accelerated Rehabilitative Disposition) case - Sentenced by County Judge, Supervised by PBPP
        "4B",  # PWV (Probation Without Verdict) case - Sentenced by County Judge and Supervised by PBPP
        "4C",  # COOP case - Offender on both PBPP and County Supervision
        "B2",  # Released according to Boot Camp Law
        "C2",  # CCC Parole
        "C3",  # CCC Reparole
        "R2",  # RSAT Parole
        "R3",  # RSAT Reparole
        "06",  # Paroled/Reparoled by other state and transferred to PA
        "07",  # Sentenced to Probation by other state and transferred to PA
    ],
    StateCustodialAuthority.INTERNAL_UNKNOWN: [
        "08",  # Other States’ Deferred Sentence
        "09",  # Emergency Release - used for COVID releases
        "10",  # TODO(#58965)
    ],
    # mappings based on supervision county
    StateCustodialAuthority.OTHER_COUNTRY: ["FOREIG"],
    StateCustodialAuthority.FEDERAL: ["FEDERA"],
    StateCustodialAuthority.OTHER_STATE: [
        "ALABAM",
        "ALASKA",
        "ARIZON",
        "ARKANS",
        "CALIFO",
        "COLORA",
        "CONNEC",
        "DEL ST",
        "FLORID",
        "GEORGI",
        "HAWAII",
        "IDAHO",
        "ILLINO",
        "IND ST",
        "IOWA",
        "KANSAS",
        "KENTUC",
        "LOUISI",
        "MAINE",
        "MARYLA",
        "MASSAC",
        "MICHIG",
        "MINNES",
        "MISSIS",
        "MISSOU",
        "MONTAN",
        "NEBRAS",
        "NEVADA",
        "NEW HA",
        "NEW JE",
        "NEW ME",
        "NEW YO",
        "N CARO",
        "OHIO",
        "OKLAHO",
        "OREGON",
        "PENNSL",
        "PUERTO",
        "RHODE",
        "S CARO",
        "S DAKO",
        "TENNES",
        "TEXAS",
        "UTAH",
        "VERMON",
        "VIRGIN",
        "VIRISL",
        "W VIRG",
        "WAS ST",
        "WASHDC",
        "WISCON",
        "WYO ST",
    ],
}

SUPERVISION_PERIOD_SUPERVISION_TYPE_TO_STR_MAPPINGS: Dict[
    StateSupervisionPeriodSupervisionType, List[str]
] = {
    StateSupervisionPeriodSupervisionType.DUAL: [
        "4C",  # COOP case - Offender on both PBPP and County Supervision
    ],
    StateSupervisionPeriodSupervisionType.PAROLE: [
        "02",  # Paroled from SCI to PBPP Supervision
        "B2",  # Released according to Boot Camp Law
        "R2",  # RSAT Parole
        "C2",  # CCC Parole
        "03",  # Reparoled from SCI to PBPP Supervision
        "R3",  # RSAT Reparole
        "C3",  # CCC Reparole
        "05",  # Special Parole sentenced by County and Supervised by PBPP
        "06",  # Paroled/Reparoled by other state and transferred to PA
    ],
    StateSupervisionPeriodSupervisionType.PROBATION: [
        "04",  # Sentenced to Probation by County Judge and Supervised by PBPP
        "4A",  # ARD (Accelerated Rehabilitative Disposition) case - Sentenced by County Judge, Supervised by PBPP
        "4B",  # PWV (Probation Without Verdict) case - Sentenced by County Judge and Supervised by PBPP
        "07",  # Sentenced to Probation by other state and transferred to PA
    ],
    StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN: [
        "08",  # Other States’ Deferred Sentence
        "09",  # Emergency Release - used for COVID releases
        "10",  # TODO(#58965)
    ],
}

SUPERVISION_PERIOD_ADMISSION_REASON_TO_STR_MAPPINGS: Dict[
    StateSupervisionPeriodAdmissionReason, List[str]
] = {
    StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION: [
        "02",  # Paroled from SCI to PBPP Supervision
        "03",  # Reparoled from SCI to PBPP Supervision
        "B2",  # Released according to Boot Camp Law
        "C2",  # CCC Parole
        "C3",  # CCC Reparole
        "R2",  # RSAT Parole
        "R3",  # RSAT Reparole
    ],
    StateSupervisionPeriodAdmissionReason.COURT_SENTENCE: [
        "04",  # Sentenced to Probation by County Judge and Supervised by PBPP
        "05",  # Special Parole sentenced by County and Supervised by PBPP
        "4A",  # ARD case - Sentenced by County Judge and Supervised by PBPP
        "4B",  # PWV case - Sentenced by County Judge and Supervised by PBPP
        "4C",  # COOP case - Offender on both PBPP and County Supervision
    ],
    StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN: [
        "08",  # Other States' Deferred Sentence
        "09",  # Emergency Release - used for COVID releases
        "10",  # TODO(#58965)
    ],
    StateSupervisionPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION: [
        "06",  # Paroled/Reparoled by other state and transferred to PA
        "07",  # Sentenced to Probation by other state and transferred to PA
    ],
}

MOVEMENT_CODE_TO_INCARCERATION_PERIOD_ADMISSION_REASON_MAPPINGS: Dict[
    str, StateIncarcerationPeriodAdmissionReason
] = invert_enum_to_str_mappings(
    INCARCERATION_PERIOD_ADMISSION_REASON_TO_MOVEMENT_CODE_MAPPINGS
)

STR_TO_SUPERVISION_PERIOD_CUSTODIAL_AUTHORITY_MAPPINGS: Dict[
    str, StateCustodialAuthority
] = invert_enum_to_str_mappings(SUPERVISION_PERIOD_CUSTODIAL_AUTHORITY_TO_STR_MAPPINGS)


STR_TO_SUPERVISION_PERIOD_SUPERVISION_TYPE_MAPPINGS: Dict[
    str, StateSupervisionPeriodSupervisionType
] = invert_enum_to_str_mappings(SUPERVISION_PERIOD_SUPERVISION_TYPE_TO_STR_MAPPINGS)

STR_TO_SUPERVISION_PERIOD_ADMISSION_REASON_MAPPINGS: Dict[
    str, StateSupervisionPeriodAdmissionReason
] = invert_enum_to_str_mappings(SUPERVISION_PERIOD_ADMISSION_REASON_TO_STR_MAPPINGS)


def residency_status_from_address(raw_text: str) -> StateResidencyStatus:
    normalized_address = raw_text.upper()
    no_stable_housing_indicators = ["HOMELESS", "TRANSIENT"]
    for indicator in no_stable_housing_indicators:
        if indicator in normalized_address:
            # TODO(#9301): Use the term NO_STABLE_HOUSING in the schema instead of
            #  HOMELESS / TRANSIENT.
            return StateResidencyStatus.HOMELESS

    return StateResidencyStatus.PERMANENT


def supervision_contact_location_mapper(
    raw_text: str,
) -> StateSupervisionContactLocation:
    """Maps a supervision_contact_location_raw_text to the corresponding
    StateSupervisionContactLocation, if applicable."""
    if raw_text:
        collateral_type, method = raw_text.split("†")
        if collateral_type == "Treatment Provider":
            return StateSupervisionContactLocation.TREATMENT_PROVIDER
        if collateral_type == "Employer":
            return StateSupervisionContactLocation.PLACE_OF_EMPLOYMENT
        if collateral_type == "Court/Probation Staf":
            return StateSupervisionContactLocation.COURT
        if collateral_type == "Law Enforcement":
            return StateSupervisionContactLocation.LAW_ENFORCEMENT_AGENCY
        if method == "Field":
            return StateSupervisionContactLocation.FIELD
        if method == "Office":
            return StateSupervisionContactLocation.SUPERVISION_OFFICE
        if method == "Home":
            return StateSupervisionContactLocation.RESIDENCE
        if method == "Work":
            return StateSupervisionContactLocation.PLACE_OF_EMPLOYMENT
    return StateSupervisionContactLocation.INTERNAL_UNKNOWN


def assessment_level_mapper(raw_text: str) -> StateAssessmentLevel:
    """Maps an assessment_level_raw_text code to the corresponding StateAssessmentLevel,
    if applicable.

    Over time, US_PA has updated the mapping between an LSIR score and the associated
    assessment level. This function returns the appropriate assessment_level according
    to the score and the date of the |assessment|, as defined by
    _DATE_SPECIFIC_ORDERED_LSIR_LEVELS.
    """
    assessment_date_raw, _desc, assessment_score_raw = raw_text.split("††")

    if not assessment_score_raw or "NONE" in assessment_score_raw:
        return StateAssessmentLevel.EXTERNAL_UNKNOWN

    assessment_score = parse_int(assessment_score_raw)
    if assessment_score == 60:
        # This value indicates the scoring was not completed (ATTEMPTED INCOMPLETE)
        return StateAssessmentLevel.EXTERNAL_UNKNOWN
    if assessment_score == 70:
        # This person either refused to be assessed or did not need to be assessed
        # because they chose not to be released onto parole (REFUSED)
        return StateAssessmentLevel.EXTERNAL_UNKNOWN
    if assessment_score > 55:
        # Assessment score number is over the max value of 54, and isn't one of the
        # expected special-case codes (60, 70, 55) (SCORE_OUT_OF_RANGE)
        return StateAssessmentLevel.EXTERNAL_UNKNOWN

    if assessment_score == 55:
        # This should be treated as a 54
        assessment_score = 54
    assessment_date = (
        str_field_utils.parse_date(assessment_date_raw) if assessment_date_raw else None
    )
    if not assessment_date:
        # At this point we need a valid assessment_date to determine the date-specific
        # LSIR level
        return StateAssessmentLevel.EXTERNAL_UNKNOWN
    for cutoff_date, score_level_map in _DATE_SPECIFIC_ORDERED_LSIR_LEVELS.items():
        if assessment_date <= cutoff_date:
            for cutoff_score, level in score_level_map.items():
                if assessment_score <= cutoff_score:
                    return level
    raise ValueError(
        f"Unhandled assessment_score {assessment_score} with assessment_date {assessment_date}"
    )


def force_adm_reason_internal_unknown(
    raw_text: str,
) -> StateIncarcerationPeriodAdmissionReason:
    """Maps admission reason raw text to INTERNAL_UNKNOWN; used instead of a literal enum
    so that raw text can be preserved."""
    if raw_text:
        return StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN
    raise ValueError("This parser should never be called on missing raw text.")


def incarceration_period_purpose_mapper(
    raw_text: str,
) -> StateSpecializedPurposeForIncarceration:
    """Maps a combination of the incarceration period codes to a formal specialized purpose for incarceration.

    Codes from CCIS tables are handled separately from codes from SCI tables. CCIS codes are prefixed with `CCIS`.
    CCIS codes will either be mapped to a specific purpose for incarceration or INTERNAL_UNKNOWN.

    For SCI codes, the two codes are start_parole_status_code and sentence_type. They are concatenated together in that
    order, separated by whitespace, in us_pa_controller. Here, we split them up and select a purpose for incarceration
    based on the following logic:

    1. If the start_parole_status_code indicates the person has a parole status pending, we choose a static mapping
    based on the start_parole_status_code
    2. If the sentence_type indicates the person is in some sort of treatment program, we choose a static mapping based
    on the sentence_type
    3. If none of the above are true, we return GENERAL
    """
    if raw_text.startswith("CCIS"):
        # Handle incarceration period purpose codes from CCIS tables
        _, purpose_for_incarceration = raw_text.split("-")

        if purpose_for_incarceration in ("26", "46"):
            return StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION
        if purpose_for_incarceration in (
            # Detox
            "51",
            # State Drug Treatment Programs
            "62",
            "63",
            "64",
            "65",
            "66",
        ):
            return StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
        # TODO(#9421): Need to revisit and update this once there is a solid design plan
        #  for how to represent community centers
        return StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN

    start_parole_status_code, start_movement_code, sentence_type = raw_text.split("-")

    # TODO(#10502): There are 4 cases (ML0641, HJ9463, HM6768, JH9458) where there is a PVP parole status and a 'P'
    #  sentence type associated with that inmate number. What does it mean for a parole violator to be in on SIP
    #  Program? Is this just an error?
    is_parole_violation_pending = start_parole_status_code == "PVP"
    is_shock_incarceration = start_movement_code == "APV"

    if is_parole_violation_pending:
        return StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD

    is_treatment_program = sentence_type in (
        "E",  # SIP Evaluation
        "P",  # SIP Program
    )
    if is_treatment_program:
        return StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON

    if is_shock_incarceration:
        return StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION

    return StateSpecializedPurposeForIncarceration.GENERAL


def incarceration_period_release_reason_mapper(
    raw_text: str,
) -> StateIncarcerationPeriodReleaseReason:
    """Maps three key incarceration period end codes to a formal release reason.

    End codes coming from CCIS are prefixed with `CCIS`. For these codes, the standard enum mapping is used.

    For end codes coming from SCI, the three concatenated codes are end_sentence_status_code, end_parole_status_code,
    and end_movement_code. They are concatenated together in that order, separated by whitespace, in us_pa_controller.
    Here, we split them up and select a release reason based on the following logic:

    1. If end_is_admin_edge indicates these period was ended for merely administrative reasons, we return STATUS_CHANGE.
    2. If the end_parole_status_code indicates the person was released to parole, we choose a static mapping based on
    that code specifically
    3. If the end_sentence_status_code indicates the sentence was just completed,
    we return SENTENCED_SERVED
    4. If the end_sentence_status_code indicates a conflict of meaning with the movement code,
    we return INTERNAL_UNKNOWN
    5. If the end_movement_code is a generic release reason, we choose a static mapping based on the
    end_sentence_status_code, which will be more informative
    6. If none of the above are true, we choose a static mapping based on end_movement_code
    """
    raw_text = raw_text.upper()
    if raw_text.startswith("CCIS"):
        _, end_movement_code = raw_text.split("-")
    else:
        # Handle release reason codes from SCI tables
        (
            end_sentence_status_code,
            end_parole_status_code,
            end_movement_code,
            end_is_admin_edge,
        ) = raw_text.split("-")

        ## Catches errors before it catches anything else
        if end_sentence_status_code == "RE":
            return (
                StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION
            )

        if end_is_admin_edge == "TRUE":
            return StateIncarcerationPeriodReleaseReason.STATUS_CHANGE

        if end_movement_code in (
            "DA",
            "DIT",
        ):  # Delete Administrative, Delete In Transit
            return StateIncarcerationPeriodReleaseReason.TRANSFER

        is_sentence_complete = end_sentence_status_code == "SC"
        is_serve_previous = end_sentence_status_code == "SP"
        was_released_to_parole = end_parole_status_code in (
            "RP",
            "SP",
        )  # Re-Parole, State Parole
        is_generic_release = end_movement_code == "D"  # Delete

        if was_released_to_parole:
            # In case of a release to parole, the ending parole status code is the most informative
            return _retrieve_release_reason_mapping(end_parole_status_code)

        if is_sentence_complete:
            # This is set manually because there are conflicting SC meanings between sentence status code
            # and movement code (Sentence Completed versus Status Change, respectively)
            return StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED

        if is_serve_previous:
            # This is set manually because there are conflicting SP meanings between sentence status code
            # and movement code (Serve Previous (?) versus State Parole, respectively)
            return StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN

        if is_generic_release:
            # In case of a generic release reason, the ending sentence status code is the most informative
            if end_sentence_status_code == "NONE":
                return StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN

            return _retrieve_release_reason_mapping(end_sentence_status_code)

        # If none of the above are true, base this on the movement code itself
        if end_movement_code == "NONE":
            return StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN
            # If none of the above are true, base this on the movement code itself

    if end_movement_code == "NONE":
        return StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN

    return _retrieve_release_reason_mapping(end_movement_code)


def incarceration_period_admission_reason_mapper(
    raw_text: str,
) -> StateIncarcerationPeriodAdmissionReason:
    """Maps key incarceration period start codes to a formal admission reason.

    If start_is_admin_edge indicates that this new period was started only for
    administrative reasons, then we return STATUS_CHANGE. If start_is_new_revocation is
    true, then we return REVOCATION. Otherwise we map based on the
    start_movement_code.
    """
    # start_is_new_revocation comes in lower case
    raw_text = raw_text.upper()
    if raw_text.startswith("CCIS"):
        (_, start_is_new_revocation, start_movement_code) = raw_text.split("-")
        end_sentence_status_code = "NONE"
        start_is_admin_edge = "FALSE"
        parole_status_code = "NONE"
    else:
        (
            end_sentence_status_code,
            parole_status_code,
            start_is_new_revocation,
            start_movement_code,
            start_is_admin_edge,
        ) = raw_text.split("-")

    if end_sentence_status_code == "RE":
        # TODO(#18047): Adding as a placeholder for removing rows that have been administratively deleted.
        # These individuals were not actually admitted anywhere - entries with this
        # end sentence status code are all deleted administratively after the fact.
        # These are data entry errors, not admission errors.
        return StateIncarcerationPeriodAdmissionReason.ADMITTED_IN_ERROR

    if start_is_new_revocation == "TRUE":
        # Note: These are not always legal revocations. We are currently using the
        # REVOCATION admission_reason for admissions from parole for treatment
        # and shock incarceration as well as for legal revocations in ingest.
        # Treatment and shock incarceration admissions are handled as sanction
        # admissions in IP pre-processing.
        return StateIncarcerationPeriodAdmissionReason.REVOCATION

    if start_is_admin_edge == "TRUE":
        return StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE

    if parole_status_code == "PVP":
        return StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY

    admission_reason = (
        MOVEMENT_CODE_TO_INCARCERATION_PERIOD_ADMISSION_REASON_MAPPINGS.get(
            start_movement_code, None
        )
    )

    if not admission_reason:
        raise ValueError(
            f"No mapping for incarceration period admission reason from movement code {start_movement_code}"
        )
    return admission_reason


def supervision_period_supervision_type_mapper(
    raw_text: str,
) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Maps a list of supervision type codes to a supervision type. If both probation and parole types are present, this
    will return StateSupervisionPeriodSupervisionType.DUAL."""
    if not raw_text:
        return None

    supervision_type_strs = raw_text.split(",")

    supervision_types = set()
    for supervision_type_str in supervision_type_strs:
        supervision_type = STR_TO_SUPERVISION_PERIOD_SUPERVISION_TYPE_MAPPINGS.get(
            supervision_type_str, None
        )
        if not supervision_type:
            raise ValueError(
                f"No mapping for supervision period supervision type {supervision_type}"
            )

        supervision_types.add(supervision_type)

    return get_most_relevant_supervision_type(supervision_types)


def supervision_period_custodial_authority_mapper_based_on_supervision_county(
    raw_text: str,
) -> Optional[StateCustodialAuthority]:
    """Maps a list of supervision county codes to a custodial authority enum."""
    if not raw_text:
        return None

    custodial_authority = STR_TO_SUPERVISION_PERIOD_CUSTODIAL_AUTHORITY_MAPPINGS.get(
        raw_text, None
    )
    if not custodial_authority:
        raise ValueError(
            f"No mapping for supervision period custodial authority {custodial_authority}"
        )

    return custodial_authority


def supervision_period_custodial_authority_mapper_based_on_supervision_type(
    raw_text: str,
) -> Optional[StateCustodialAuthority]:
    """Maps a list of supervision type codes to a custodial authority enum."""
    if not raw_text:
        return None

    custodial_authority_strs = raw_text.split(",")

    custodial_authorities = set()
    for custodial_authority_str in custodial_authority_strs:
        custodial_authority = (
            STR_TO_SUPERVISION_PERIOD_CUSTODIAL_AUTHORITY_MAPPINGS.get(
                custodial_authority_str, None
            )
        )
        if not custodial_authority:
            raise ValueError(
                f"No mapping for supervision period custodial authority {custodial_authority_str}"
            )

        custodial_authorities.add(custodial_authority)

    if StateCustodialAuthority.SUPERVISION_AUTHORITY in custodial_authorities:
        return StateCustodialAuthority.SUPERVISION_AUTHORITY

    if StateCustodialAuthority.OTHER_STATE in custodial_authorities:
        return StateCustodialAuthority.OTHER_STATE

    if StateCustodialAuthority.INTERNAL_UNKNOWN in custodial_authorities:
        return StateCustodialAuthority.INTERNAL_UNKNOWN

    raise ValueError(
        f"Unexpected custodial authority in provided custodial_authorities set: [{custodial_authorities}]"
    )


def supervision_period_admission_reason_mapper(
    raw_text: str,
) -> Optional[StateSupervisionPeriodAdmissionReason]:
    """Maps a list of supervision period admission reason codes to an admisison reason enum."""
    if not raw_text:
        return None

    admission_reason_strs = raw_text.split(",")

    admission_reasons = set()
    for admission_reason_str in admission_reason_strs:
        admission_reason = STR_TO_SUPERVISION_PERIOD_ADMISSION_REASON_MAPPINGS.get(
            admission_reason_str, None
        )
        if not admission_reason:
            raise ValueError(
                f"No mapping for supervision period admission reason {admission_reason_str}"
            )

        admission_reasons.add(admission_reason)

    if (
        StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION
        in admission_reasons
    ):
        return StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION

    if StateSupervisionPeriodAdmissionReason.COURT_SENTENCE in admission_reasons:
        return StateSupervisionPeriodAdmissionReason.COURT_SENTENCE

    if (
        StateSupervisionPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION
        in admission_reasons
    ):
        return StateSupervisionPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION

    if StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN in admission_reasons:
        return StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN

    raise ValueError(
        f"Unexpected admission reason in provided admission_reasons set: [{admission_reasons}]"
    )


def role_subtype_mapper(raw_text: str) -> Optional[StateStaffRoleSubtype]:
    """Maps position titles to role subtypes for supervision employees.
    All facilities-side titles are currently mapped to INTERNAL_UNKNOWN."""
    if raw_text:
        if raw_text in (
            "SUPERVISION_OFFICER",
            "PRL AGT 2",
            "PAROLE AGT 2",
            "PAROLE AGT 1",
            "PRL AGT 1",
            "PRL MGR 1",
            "PRL MGR 2",
        ):
            return StateStaffRoleSubtype.SUPERVISION_OFFICER
        if raw_text in ("SUPERVISION_OFFICER_SUPERVISOR", "PRL SUPV", "PAROLE SPVR"):
            return StateStaffRoleSubtype.SUPERVISION_OFFICER_SUPERVISOR
        if raw_text in ("DISTRICT DIRECTOR", "DEPUTY DISTRICT DIRECTOR"):
            return StateStaffRoleSubtype.SUPERVISION_DISTRICT_MANAGER
        if raw_text in (
            "REGIONAL DIRECTOR",
            "RGNL PBTN PRL DIR",
            "PBTN PRL DEP DSTR DIR",
            "PBTN PRL DSTR DIR 1",
            "PBTN PRL DSTR DIR 2",
        ):
            return StateStaffRoleSubtype.SUPERVISION_REGIONAL_MANAGER
        if raw_text in (
            "DEPUTY SECRETARY",
            "EXECUTIVE ASSISTANT",
            "DEP SEC REENTRY",
            "CMY CORR CTR DIR 1",
            "CMY CORR CTR DIR 2",
        ):
            return StateStaffRoleSubtype.SUPERVISION_STATE_LEADERSHIP
        return StateStaffRoleSubtype.INTERNAL_UNKNOWN
    return StateStaffRoleSubtype.INTERNAL_UNKNOWN
