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

"""US_PA specific enum helper methods."""
from typing import Dict, List, Optional

from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodReleaseReason, \
    StateSpecializedPurposeForIncarceration, StateIncarcerationPeriodAdmissionReason
from recidiviz.common.constants.state.state_supervision_violation_response import \
    StateSupervisionViolationResponseRevocationType
from recidiviz.ingest.direct.direct_ingest_controller_utils import invert_enum_to_str_mappings


INCARCERATION_PERIOD_RELEASE_REASON_TO_MOVEMENT_CODE_MAPPINGS: \
    Dict[StateIncarcerationPeriodAdmissionReason, List[str]] = {
        StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN: [
            'AOTH',  # Other - Use Sparingly
        ],
        StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN: [
            'RTN'  # (Not in PA data dictionary, no instances after 1996)
        ],
        StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION: [
            'AB',  # Bail
            'AC',  # Court Commitment
            'ACT',  # County Transfer (transferred from a county jail, newly in DOC custody)
            'ADET',  # Detentioner
            'AFED',  # Federal Commitment
        ],
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE: [
            'AE',  # Escape
        ],
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION: [
            # TODO(#3312): Ask what the difference between these two is - APV is much more common
            'APD',  # Parole Detainee

            # TODO(#3312): What to do when the parole status code is TPV (i.e. they're already convicted)? I don't think
            #  this always corresponds to a revocation! For example CN=300233, who it looks like was transferred to a
            #  county jail (sentence status TC), then transferred back as an APV months later. However, CN=314993 seems
            #  to have just entered as a violation. TL;DR - going to need to change this logic to a mapper.
            # When a person comes in on a parole board hold they can have this status - they have not technically been
            # revoked yet if their parole status is 'PVP'.
            'APV',  # Parole Violator
        ],
        StateIncarcerationPeriodAdmissionReason.TRANSFER: [
            # This is an 'Add Administrative' which will generally follow a 'Delete Administrative' ('DA') directly
            # and is used to do some sort of record-keeping change without meaning this person went anywhere.
            'AA',  # Add - Administrative

            # Old, similar usage to 'AA'
            'AIT',  # Add - In Transit

            # TODO(#2002): This status represents that this person was returning from a long stay in a state hospital,
            # it generally follows a 'D' movement code with sentence status code 'SH'. Ideally we'd specify that this
            # was a transfer from a hospital
            'ASH',  # Add - State Hospital
            'ATT',  # Add - [Unlisted Transfer]

            # It was unclear from talking to PA what this means, but since it hasn't shown up in years, we're leaving
            # it here.
            'AW',  # Add - WRIT/ATA (Writ of Habeas Corpus Ad Prosequendum)

            'PLC',  # Permanent Location Change
            'RTT',  # Return Temporary Transfer
            'STT',  # Send Temporary Transfer
            'TFM',  # From Medical Facility
            'TRN',  # To Other Institution Or CCC
            'TTM',  # To Medical Facility
            'XPT',  # Transfer Point
            # In this context, SC is being used as a transfer from one type of
            # incarceration to another, either between facilities or within the same facility
            'SC',  # Status Change
        ],
        StateIncarcerationPeriodAdmissionReason.TRANSFERRED_FROM_OUT_OF_STATE: [
            # TODO(#3312): I think this is a person from another state who PA is holding for some short-ish
            #  period of time until they are sent back to that state - need to confirm. They are not being tried by the
            #  PA Parole Board and will have a 'NA' (Not Applicable) parole_stat_cd (parole status code). (ASK PA)
            'AOPV',  # Out Of State Probation/Parole Violator
        ],
    }

INCARCERATION_PERIOD_RELEASE_REASON_TO_STR_MAPPINGS: Dict[StateIncarcerationPeriodReleaseReason, List[str]] = {
    StateIncarcerationPeriodReleaseReason.COMMUTED: [
        'RD',  # Release Detentioner
    ],
    StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE: [
        'RP',  # Re-Parole (Paroled for the non-first time in this sentence group)
        'SP',  # State Parole (Paroled for the first time in this sentence group)
        'P',  # Paroled (relatively rare)
    ],
    StateIncarcerationPeriodReleaseReason.DEATH: [
        'DA',  # Deceased - Assault
        'DN',  # Deceased - Natural
        'DS',  # Deceased - Suicide
        'DX',  # Deceased - Accident
        'DZ',  # Deceased - Non DOC Location
    ],
    StateIncarcerationPeriodReleaseReason.EXECUTION: [
        'EX',  # Executed
    ],
    StateIncarcerationPeriodReleaseReason.EXTERNAL_UNKNOWN: [
        'AOTH',  # Other - Use Sparingly
        'X',  # Unknown
    ],
    StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN: [
        'AA',    # Administrative
        'AB',    # Bail
        'AC',    # Court Commitment
        'ADET',  # Detentioner
        'AFED',  # Federal Commitment
        'APD',  # Parole Detainee
        'AS',    # Actively Serving
        'CS',    # Change Other Sentence
        'RTN',  # (Not in PA data dictionary, no instances after 1996)
    ],
    StateIncarcerationPeriodReleaseReason.ESCAPE: [
        'AE',  # Escape
        'EC',  # Escape CSC
        'EI',  # Escape Institution
    ],
    StateIncarcerationPeriodReleaseReason.PARDONED: [
        'PD',  # Pardoned
    ],
    StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY: [
        'APV',  # Parole Violator
    ],
    StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION: [
        'RE',  # Received In Error
    ],
    StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED: [
        'B',   # Bailed
        'FR',  # Federal Release
        'NC',  # Non-return CSC
        'NF',  # Non-return Furlough
        'NR',  # [Unlisted]
    ],
    StateIncarcerationPeriodReleaseReason.TRANSFER: [
        'ACT',  # County Transfer
        'AIT',  # In Transit
        'ASH',  # State Hospital
        'ATT',  # [Unlisted Transfer]
        'AW',   # WRIT/ATA (Writ of Habeas Corpus Ad Prosequendum)
        'PLC',  # Permanent Location Change
        'RTT',  # Return Temporary Transfer
        'STT',  # Send Temporary Transfer
        'TFM',  # From Medical Facility
        'TRN',  # To Other Institution Or CCC
        'TTM',  # To Medical Facility
        'XPT',  # Transfer Point
        # In this context, SC is being used as a transfer from one type of
        # incarceration to another, either between facilities or within the same facility
        'SC',   # Status Change

        # From Sentence Status Codes
        'F',    # Furloughed
        'SH',   # State Hospital
        'TC',   # Transfer to County
        'WT',   # WRIT/ATA
    ],
    StateIncarcerationPeriodReleaseReason.TRANSFERRED_OUT_OF_STATE: [
        'IC',  # In Custody Elsewhere
        'TS',  # Transfer to Other State
    ],
    StateIncarcerationPeriodReleaseReason.VACATED: [
        'VC',  # Vacated Conviction
        'VS',  # Vacated Sentence
    ],
}
MOVEMENT_CODE_TO_INCARCERATION_PERIOD_RELEASE_REASON_MAPPINGS: Dict[str, StateIncarcerationPeriodAdmissionReason] = \
    invert_enum_to_str_mappings(INCARCERATION_PERIOD_RELEASE_REASON_TO_MOVEMENT_CODE_MAPPINGS)

REVOCATION_TYPE_TO_STR_MAPPINGS: Dict[StateSupervisionViolationResponseRevocationType, List[str]] = {
    StateSupervisionViolationResponseRevocationType.REINCARCERATION: [
        'ARR2',  # Incarceration
        'VCCF',  # Placement in PV Center
        'VCCP',  # Placement in Violation Center County Prison
    ],
    StateSupervisionViolationResponseRevocationType.RETURN_TO_SUPERVISION: [
        'CPCB',  # Placement in CCC Half Way Back
        'CPCO',  # Community Parole Corrections Half Way Out
    ],
    StateSupervisionViolationResponseRevocationType.TREATMENT_IN_PRISON: [
        'IDOX',  # Placement in D&A Detox Facility
        'IPMH',  # Placement in Mental Health Facility
    ],
}


STR_TO_INCARCERATION_PERIOD_RELEASE_REASON_MAPPINGS: Dict[str, StateIncarcerationPeriodReleaseReason] = \
    invert_enum_to_str_mappings(INCARCERATION_PERIOD_RELEASE_REASON_TO_STR_MAPPINGS)


def incarceration_period_admission_reason_mapper(concatenated_codes: str) -> StateIncarcerationPeriodAdmissionReason:
    _start_parole_status_code, start_is_new_revocation, start_movement_code = concatenated_codes.split(' ')

    if start_is_new_revocation == 'TRUE':
        return StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION

    admission_reason = MOVEMENT_CODE_TO_INCARCERATION_PERIOD_RELEASE_REASON_MAPPINGS.get(start_movement_code, None)
    if not admission_reason:
        raise ValueError(
            f'No mapping for incarceration period admission reason from movement code {start_movement_code}')
    return admission_reason


STR_TO_REVOCATION_TYPE_MAPPINGS: Dict[str, StateSupervisionViolationResponseRevocationType] = \
    invert_enum_to_str_mappings(REVOCATION_TYPE_TO_STR_MAPPINGS)


def incarceration_period_release_reason_mapper(concatenated_codes: str) -> StateIncarcerationPeriodReleaseReason:
    """Maps three key incarceration period end codes to a formal release reason.

    The three codes are end_sentence_status_code, end_parole_status_code, and end_movement_code. They are concatenated
    together in that order, separated by whitespace, in us_pa_controller. Here, we split them up and select a release
    reason based on the following logic:

    1. If the end_parole_status_code indicates the person was released to parole, we choose a static mapping based on
    that code specifically
    2. If the end_sentence_status_code indicates the sentence was just completed,
    we return SENTENCED_SERVED
    3. If the end_sentence_status_code indicates a conflict of meaning with the movement code,
    we return INTERNAL_UNKNOWN
    4. If the end_movement_code is a generic release reason, we choose a static mapping based on the
    end_sentence_status_code, which will be more informative
    5. If none of the above are true, we choose a static mapping based on end_movement_code
    """
    end_sentence_status_code, end_parole_status_code, end_movement_code = concatenated_codes.split(' ')

    if end_movement_code in ('DA', 'DIT'):  # Delete Administrative, Delete In Transit
        return StateIncarcerationPeriodReleaseReason.TRANSFER

    is_sentence_complete = end_sentence_status_code == 'SC'
    is_serve_previous = end_sentence_status_code == 'SP'
    was_released_to_parole = end_parole_status_code in ('RP', 'SP')  # Re-Parole, State Parole
    is_generic_release = end_movement_code == 'D'  # Delete

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
        if end_sentence_status_code == 'NONE':
            return StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN

        return _retrieve_release_reason_mapping(end_sentence_status_code)

    # If none of the above are true, base this on the movement code itself
    if end_movement_code == 'NONE':
        return StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN

    return _retrieve_release_reason_mapping(end_movement_code)


def incarceration_period_purpose_mapper(concatenated_codes: str) -> StateSpecializedPurposeForIncarceration:
    """Maps a combination of the incarceration period codes to a formal specialized purpose for incarceration.

    The two codes are start_parole_status_code and sentence_type. They are concatenated together in that order,
    separated by whitespace, in us_pa_controller. Here, we split them up and select a purpose for incarceration
    based on the following logic:

    1. If the start_parole_status_code indicates the person has a parole status pending, we choose a static mapping
    based on the start_parole_status_code
    2. If the sentence_type indicates the person is in some sort of treatment program, we choose a static mapping based
    on the sentence_type
    3. If none of the above are true, we return GENERAL
    """
    start_parole_status_code, sentence_type = concatenated_codes.split(' ')

    # TODO(#3312): There are 4 cases (ML0641, HJ9463, HM6768, JH9458) where there is a PVP parole status and a 'P'
    #  sentence type associated with that inmate number. What does it mean for a parole violator to be in on SIP
    #  Program? Is this just an error?
    is_parole_violation_pending = start_parole_status_code == 'PVP'

    if is_parole_violation_pending:
        return StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD

    is_treatment_program = sentence_type in (
        'E',  # SIP Evaluation
        'P'   # SIP Program
    )
    if is_treatment_program:
        return StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON

    return StateSpecializedPurposeForIncarceration.GENERAL


def revocation_type_mapper(sanction_code: Optional[str]) -> Optional[StateSupervisionViolationResponseRevocationType]:
    """Maps an incoming sanction code to the kind of revocation it represents, if applicable.

    Falls back to RETURN_TO_SUPERVISION if the sanction code does not represent any kind of revocation.
    """
    if not sanction_code:
        return None

    return STR_TO_REVOCATION_TYPE_MAPPINGS.get(sanction_code,
                                               StateSupervisionViolationResponseRevocationType.RETURN_TO_SUPERVISION)


def _retrieve_release_reason_mapping(code: str) -> StateIncarcerationPeriodReleaseReason:
    release_reason = STR_TO_INCARCERATION_PERIOD_RELEASE_REASON_MAPPINGS.get(code, None)
    if not release_reason:
        raise ValueError(f'No mapping for incarceration period release reason {code}')
    return release_reason


def concatenate_incarceration_period_start_codes(row: Dict[str, str]) -> str:
    start_parole_status_code = row['start_parole_status_code'] or 'None'
    start_is_new_revocation = row['start_is_new_revocation'] or 'None'
    start_movement_code = row['start_movement_code'] or 'None'

    return f"{start_parole_status_code}-{start_is_new_revocation}-{start_movement_code}"


def concatenate_incarceration_period_end_codes(row: Dict[str, str]) -> str:
    end_sentence_status_code = row['end_sentence_status_code'] or 'None'
    end_parole_status_code = row['end_parole_status_code'] or 'None'
    end_movement_code = row['end_movement_code'] or 'None'

    return f"{end_sentence_status_code}-{end_parole_status_code}-{end_movement_code}"


def concatenate_incarceration_period_purpose_codes(row: Dict[str, str]) -> str:
    start_parole_status_code = row['start_parole_status_code'] or 'None'
    sentence_type = row['sentence_type'] or 'None'

    return f"{start_parole_status_code}-{sentence_type}"
