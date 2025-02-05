# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Custom enum parsers functions for US_AR. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_ar_custom_enum_parsers.<function name>
"""

from typing import List, Optional

from recidiviz.common.constants.state.state_assessment import StateAssessmentLevel
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodHousingUnitCategory,
    StateIncarcerationPeriodHousingUnitType,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_person import StateEthnicity
from recidiviz.common.constants.state.state_sentence import StateSentenceType
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_staff_role_period import (
    StateStaffRoleSubtype,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodTerminationReason,
)

SOLITARY_BED_TYPES: List[str] = [
    "PC",  # Padded Cell
    "SC",  # Security Cell
    "SR",  # Single Room
]

GENERAL_BED_USES: List[str] = [
    "K",  # Maximum Custody
    "L",  # Medium Custody
    "M",  # Minimum Custody
]


def parse_incarceration_sentence_type_v2(
    raw_text: str,
) -> Optional[StateSentenceType]:
    # Incarceration sentences usually just have 1 sentence type; however, since they can
    # potentially have multiple, we prioritize them to pick the most relevant.
    sentence_types = raw_text.split("-")
    if "FS" in sentence_types:  # Federal Custody
        return StateSentenceType.FEDERAL_PRISON
    if "SP" in sentence_types:  # State Prison
        return StateSentenceType.STATE_PRISON
    if any(
        sentence_type
        in [
            "CJ",  # County Jail
            "JU",  # Judicial Transfer to CCC
            "PT",  # Pre-Trial
            "RP",  # Probation Plus (at CCC)
            "SA",  # Supervision Sanction Program (SSP)
        ]
        for sentence_type in sentence_types
    ):
        return StateSentenceType.COUNTY_JAIL
    return StateSentenceType.INTERNAL_UNKNOWN


def parse_arora_score(raw_text: str) -> Optional[StateAssessmentLevel]:
    """Custom parser retrieving assessment levels from ARORA scores. Scores of 0-4 are
    in the minimum category, 5-9 is low, 10-12 is moderate, and 13+ is high."""
    if not raw_text.isnumeric():
        return None
    asmt_score = int(raw_text)
    if asmt_score < 5:
        return StateAssessmentLevel.MINIMUM
    if asmt_score <= 9:
        return StateAssessmentLevel.LOW
    if asmt_score <= 12:
        return StateAssessmentLevel.MODERATE
    return StateAssessmentLevel.HIGH


def parse_housing_category(
    raw_text: str,
) -> Optional[StateIncarcerationPeriodHousingUnitCategory]:
    """Custom parser for housing category, taking into account both bed type and bed use."""
    bed_type, bed_use = raw_text.split("-")
    if bed_type in SOLITARY_BED_TYPES or bed_use in (
        "A",  # Adminis.Detention
        "B",  # Adminis.Segregation
        "F",  # Invol.Protective Custody
        "N",  # Protective Custody
    ):
        return StateIncarcerationPeriodHousingUnitCategory.SOLITARY_CONFINEMENT

    if bed_use in GENERAL_BED_USES:
        return StateIncarcerationPeriodHousingUnitCategory.GENERAL
    if bed_use == "U" and bed_type == "U":  # Unknown (conversion)
        return StateIncarcerationPeriodHousingUnitCategory.EXTERNAL_UNKNOWN
    return StateIncarcerationPeriodHousingUnitCategory.INTERNAL_UNKNOWN


def parse_housing_type(
    raw_text: str,
) -> Optional[StateIncarcerationPeriodHousingUnitType]:
    """Custom parser for housing type, taking into account both bed type and bed use."""
    bed_type, bed_use = raw_text.split("-")
    if bed_use in (
        "A",  # Adminis.Detention
        "B",  # Adminis.Segregation
    ):
        return (
            StateIncarcerationPeriodHousingUnitType.ADMINISTRATIVE_SOLITARY_CONFINEMENT
        )
    if bed_type in SOLITARY_BED_TYPES and bed_use == "P":  # Punitive
        return StateIncarcerationPeriodHousingUnitType.DISCIPLINARY_SOLITARY_CONFINEMENT
    if bed_use in (
        "F",  # Invol.Protective Custody
        "N",  # Protective Custody
    ):
        return StateIncarcerationPeriodHousingUnitType.PROTECTIVE_CUSTODY
    if bed_type in SOLITARY_BED_TYPES:
        return StateIncarcerationPeriodHousingUnitType.OTHER_SOLITARY_CONFINEMENT

    if bed_use in (
        "I",  # Infirmary
        "S",  # Medical
    ):
        return StateIncarcerationPeriodHousingUnitType.HOSPITAL

    if bed_use in GENERAL_BED_USES:
        return StateIncarcerationPeriodHousingUnitType.GENERAL

    if bed_use == "U" and bed_type == "U":  # Unknown (conversion)
        return StateIncarcerationPeriodHousingUnitType.EXTERNAL_UNKNOWN

    return StateIncarcerationPeriodHousingUnitType.INTERNAL_UNKNOWN


def parse_release_reason(
    raw_text: str,
) -> Optional[StateIncarcerationPeriodReleaseReason]:
    """Release reasons are mapped using the movement code and reason coinciding with
    the end of a period. The code and reason are split up, and then broken down into
    lists in case there are multiple; this should only happen if a custody classification
    and a bed assignment occur on the same day. This approach allows us to prioritize
    mappings correctly: for example, a movement code of 40 ('Discharged') maps to
    SENTENCE_SERVED, unless there's also a movement reason of 43 ('Sentence Vacated'), in
    which case the release reason is set to VACATED."""
    move_type, move_reason = raw_text.split("@@")
    move_type_list = move_type.split("-")
    move_reason_list = move_reason.split("-")
    if "INFERRED_RELEASE_TO_SUPERVISION" in move_type_list:
        # We can infer that these movements are releases to supervision, but we don't
        # actually want to make any assumptions about data with this movement code since
        # it's only used for movements with data entry errors.
        return StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN
    if "50" in move_type_list:  # Commutation
        return StateIncarcerationPeriodReleaseReason.COMMUTED
    if any(
        mt
        in [
            "60",  # Paroled - Regular
            "61",  # Paroled - Act 230
            "66",  # Paroled - Act 50
            "67",  # Act 309 Conditional Release
        ]
        for mt in move_type_list
    ):
        return StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE
    if "43" in move_reason_list:  # Sentence Vacated
        return StateIncarcerationPeriodReleaseReason.VACATED
    if any(
        mt
        in [
            "21",  # Returned from Court
            "52",  # Released by Court
            "81",  # Out To Court
        ]
        for mt in move_type_list
    ) or (
        any(
            mt
            in [
                "90",  # Transferred to Another Facility
                "30",  # Received from Another Facility
            ]
            for mt in move_type_list
        )
        and "31" in move_reason_list  # Court Appearance
    ):
        return StateIncarcerationPeriodReleaseReason.COURT_ORDER
    if "75" in move_type_list:  # Death
        return StateIncarcerationPeriodReleaseReason.DEATH
    if "77" in move_type_list:  # Execution
        return StateIncarcerationPeriodReleaseReason.EXECUTION
    if "80" in move_type_list:  # Escaped
        return StateIncarcerationPeriodReleaseReason.ESCAPE
    if "54" in move_type_list:  # Full Pardon
        return StateIncarcerationPeriodReleaseReason.PARDONED
    if (
        "40" in move_type_list and "76" in move_reason_list
    ):  # Discharged - Terminal Illness
        return StateIncarcerationPeriodReleaseReason.COMPASSIONATE
    if "42" in move_type_list and any(  # Released Permanently (Jail Detainee)
        mr
        in [
            "P1",  # County Jail (Parole Hold - In State)
            "P3",  # County Jail (Parole Hold - Out of State)
        ]
        for mr in move_reason_list
    ):
        return StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY
    if "37" in move_reason_list:  # Released In Error
        return StateIncarcerationPeriodReleaseReason.RELEASED_IN_ERROR
    if any(
        mt
        in [
            "41",  # ACC To Probation/SIS
            "68",  # ADC Released to Supervision
            "69",  # ACC Released to Supervision
            "78",  # Returned to Comm. Supervision
            "8A",  # Out to Field Supv
            "93",  # Transferred to Drug Court
        ]
        for mt in move_type_list
    ):
        return StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION
    if "20" in move_type_list:  # Returned from Escape
        return StateIncarcerationPeriodReleaseReason.RETURN_FROM_ESCAPE
    if any(
        mt
        in [
            "22",  # Returned from Leave
            "23",  # Returned from Hospital
            "26",  # Returned from Furlough
        ]
        for mt in move_type_list
    ):
        return StateIncarcerationPeriodReleaseReason.RETURN_FROM_TEMPORARY_RELEASE
    if any(
        mt
        in [
            "40",  # Discharged
            "42",  # Released Permanently (Jail Detainee)
        ]
        for mt in move_type_list
    ):
        return StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED
    if any(
        mt
        in [
            "83",  # Out to Hospital
            "82",  # Escorted Leave
            "81",  # Out To Court
            "84",  # Out on Bond
            "86",  # Out on Furlough
            "8J",  # Released Temporarily (Jail Detainee)
        ]
        for mt in move_type_list
    ):
        return StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE
    if any(
        mt
        in [
            "03",  # New Commitment- Juris. Tranfr.
            "27",  # Returned from County Jail
            "30",  # Received from Another Facility
            "31",  # Received at ACC from ADC,
            "32",  # Received at ADC from ACC
            "36",  # Transfer Jurisdiction
            "90",  # Transferred to Another Facility
            "91",  # Transferred to Another Facility
            "92",  # Transferred to ADC
            "97",  # Transferred to County Jail
            "BED_ASSIGNMENT",
        ]
        for mt in move_type_list
    ):
        return StateIncarcerationPeriodReleaseReason.TRANSFER
    if any(
        mt
        in [
            "74",  # Transferred Interstate
            "79",  # Released to Interstate Compact,
            "85",  # Transferred OOS (ISC)
            "89",  # Transferred OOS (not ISC)
        ]
        for mt in move_type_list
    ):
        return StateIncarcerationPeriodReleaseReason.TRANSFER_TO_OTHER_JURISDICTION
    if "CUSTODY_LVL_CHANGE" in move_type_list:
        return StateIncarcerationPeriodReleaseReason.STATUS_CHANGE

    return StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN


def parse_admission_reason(
    raw_text: str,
) -> Optional[StateIncarcerationPeriodAdmissionReason]:
    """Admission reasons are mapped using the movement code and reason coinciding with
    the start of a period. The code and reason are split up, and then broken down into
    lists in case there are multiple; this should only happen if a custody classification
    and a bed assignment occur on the same day."""
    move_type, move_reason = raw_text.split("@@")
    move_type_list = move_type.split("-")
    move_reason_list = move_reason.split("-")

    if "01" in move_type_list:  # New Commitment
        return StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION
    if "80" in move_type_list:  # Escaped
        return StateIncarcerationPeriodAdmissionReason.ESCAPE
    if "20" in move_type_list:  # Returned from Escape
        return StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE
    if (
        "02" in move_type_list and "37" in move_reason_list
    ):  # Recommitment - Released In Error
        return StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE
    if any(
        mt
        in [
            "22",  # Returned from Leave
            "23",  # Returned from Hospital
            "26",  # Returned from Furlough
        ]
        for mt in move_type_list
    ):
        return StateIncarcerationPeriodAdmissionReason.RETURN_FROM_TEMPORARY_RELEASE
    if any(
        mt
        in [
            "10",  # Returned from Parole
            "11",  # Returned from Act 230
            "14",  # Returned from Act 378
            "15",  # Returned from Act 814,
            "17",  # Act 309 Revoked
            "18",  # Returned from ADC Release
            "19",  # Returned from ACC Release
            "28",  # Returned from Abscond
            "2A",  # Returned from Field Supv
            "38",  # Supervision Violator (Arrested)
        ]
        for mt in move_type_list
    ):
        if any(
            mr
            in [
                "P1",  # County Jail (Parole Hold - In State)
                "P3",  # County Jail (Parole Hold - Out of State)
            ]
            for mr in move_reason_list
        ):
            return StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY
        if any(
            mr
            in [
                "55",  # Supervision Sanction
                "T1",  # Sanction Incarceration to SSP
                "T0",  # SSP County Jail Backup
                "89",  # Act 570 Jail Sanctions
                "B7",  # Act 423 Jail Sanctions
            ]
            for mr in move_reason_list
        ):
            return StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION
        if "04" in move_reason_list:  # County/City Jail Backup
            # If someone is returned from supervision with REASONFORMOVEMENT "04" (County/City Jail Backup),
            # then the admission is considered a revocation no matter what (since backup
            # facilities aren't used for sanction admissions or board holds). Admissions
            # with this REASONFORMOVEMENT can either be full revocations or 90-day revocations;
            # if the board hearing data indicates that it's a 90-day revocation, we still
            # treat it as a revocation admission, but set PFI to SHOCK_INCARCERATION.
            # (AR considers these to be revocations rather than sanctions, even though
            # people on 90-day revocations may be able to resume their supervision after
            # 90 days of good behavior).
            return StateIncarcerationPeriodAdmissionReason.REVOCATION
        return StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION
    if any(
        mt
        in [
            "83",  # Out to Hospital
            "82",  # Escorted Leave
            "81",  # Out To Court
            "84",  # Out on Bond
            "86",  # Out on Furlough
            "8J",  # Released Temporarily (Jail Detainee)
        ]
        for mt in move_type_list
    ):
        return StateIncarcerationPeriodAdmissionReason.TEMPORARY_RELEASE

    if any(
        mt
        in [
            "03",  # New Commitment- Juris. Tranfr.
            "27",  # Returned from County Jail
            "30",  # Received from Another Facility
            "31",  # Received at ACC from ADC,
            "32",  # Received at ADC from ACC
            "36",  # Transfer Jurisdiction
            "90",  # Transferred to Another Facility
            "91",  # Transferred to Another Facility
            "92",  # Transferred to ADC
            "97",  # Transferred to County Jail
            "BED_ASSIGNMENT",
        ]
        for mt in move_type_list
    ):
        return StateIncarcerationPeriodAdmissionReason.TRANSFER

    if any(
        mt
        in [
            "07",  # ISC Commitment
            "25",  # Returned from Interst. Compact
            "85",  # Transferred OOS (ISC)
            "89",  # Transferred OOS (not ISC)
        ]
        for mt in move_type_list
    ):
        return StateIncarcerationPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION
    if "CUSTODY_LVL_CHANGE" in move_type_list:
        return StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE

    return StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN


def parse_incarceration_type(
    raw_text: str,
) -> Optional[StateIncarcerationType]:
    """Custom parser for incarceration type, based on the ORGANIZATIONTYPE associated
    with someone's facility."""
    if raw_text in [
        "B6",  # County Jail Contract Condition
        "B7",  # County 309-In Jail
        "B8",  # County Jail Backup
        "E1",  # County Jail/Sheriff
    ]:
        return StateIncarcerationType.COUNTY_JAIL
    if raw_text == "U5":  # Federal Prison
        return StateIncarcerationType.FEDERAL_PRISON
    if raw_text and raw_text[0] == "I":  # All out-of-state incarceration sites
        return StateIncarcerationType.OUT_OF_STATE
    if raw_text == "B1":  # State Prison Unit
        return StateIncarcerationType.STATE_PRISON
    return StateIncarcerationType.INTERNAL_UNKNOWN if raw_text else None


def parse_custodial_authority(
    raw_text: str,
) -> Optional[StateCustodialAuthority]:
    """Custom parser for custodial authority, using the same logic as incarceration type
    but returning the comparable StateCustodialAuthority values."""
    # TODO(#26119): Fully classify all ORGANIZATIONTYPE codes to add detail to this enum
    # beyond the information already captured in StateIncarcerationType

    if raw_text in [
        "B6",  # County Jail Contract Condition
        "B7",  # County 309-In Jail
        "B8",  # County Jail Backup
        "E1",  # County Jail/Sheriff
    ]:
        return StateCustodialAuthority.COUNTY
    if raw_text == "U5":  # Federal Prison
        return StateCustodialAuthority.FEDERAL
    if raw_text and raw_text[0] == "I":  # All out-of-state incarceration sites
        return StateCustodialAuthority.OTHER_STATE
    if raw_text == "B1":  # State Prison Unit
        return StateCustodialAuthority.STATE_PRISON
    return StateCustodialAuthority.INTERNAL_UNKNOWN if raw_text else None


def parse_incarceration_sentence_type(
    raw_text: str,
) -> Optional[StateIncarcerationType]:
    # Incarceration sentences usually just have 1 sentence type; however, since they can
    # potentially have multiple, we prioritize them to pick the most relevant.
    sentence_types = raw_text.split("-")
    if "FS" in sentence_types:  # Federal Custody
        return StateIncarcerationType.FEDERAL_PRISON
    if "SP" in sentence_types:  # State Prison
        return StateIncarcerationType.STATE_PRISON
    if any(
        sentence_type
        in [
            "CJ",  # County Jail
            "JU",  # Judicial Transfer to CCC
            "PT",  # Pre-Trial
            "RP",  # Probation Plus (at CCC)
            "SA",  # Supervision Sanction Program (SSP)
        ]
        for sentence_type in sentence_types
    ):
        return StateIncarcerationType.COUNTY_JAIL
    return StateIncarcerationType.INTERNAL_UNKNOWN


def parse_role_subtype(
    raw_text: str,
) -> Optional[StateStaffRoleSubtype]:
    # TODO(#25552): Update SUPERVISION_OFFICER_SUPERVISOR mapping (along with others,
    # if necessary) to use a more actively used code value.
    if raw_text == "C22":  # Probation/Parole Officer
        return StateStaffRoleSubtype.SUPERVISION_OFFICER
    if raw_text == "C21":  # Probation/Parole Officer
        return StateStaffRoleSubtype.SUPERVISION_OFFICER_SUPERVISOR
    if raw_text in [
        "C11",  # Area Manager (ACC)
        "C12",  # Assistant Area Manager (ACC)
    ]:
        return StateStaffRoleSubtype.SUPERVISION_DISTRICT_MANAGER
    if raw_text == "C10":  # Region Supervisor (ACC)
        return StateStaffRoleSubtype.SUPERVISION_REGIONAL_MANAGER
    if raw_text in [
        "C01",  # Director (ACC)
        "C02",  # Division Director (ACC)
        "C0P",  # Deputy Dir Parole/Prob (ACC)
    ]:
        return StateStaffRoleSubtype.SUPERVISION_STATE_LEADERSHIP
    return StateStaffRoleSubtype.INTERNAL_UNKNOWN if raw_text else None


def parse_ethnic_group(
    raw_text: str,
) -> Optional[StateEthnicity]:
    if raw_text in [
        "03",  # Cuban
        "09",  # Hispanic or Latino
        "10",  # South American
        "11",  # Central America
        "23",  # Mexican American
        "24",  # Mexican National
        "27",  # Puerto Rican
        "33",  # Spain (note: by most definitions, Hispanic but not Latino)
        "35",  # Peru
        "36",  # Panama
        "37",  # Boliva
        "40",  # Mariel-Cuban
    ]:
        return StateEthnicity.HISPANIC

    if raw_text in ["98", "99"]:
        return StateEthnicity.EXTERNAL_UNKNOWN

    return StateEthnicity.NOT_HISPANIC if raw_text else None


def parse_non_parole_period_start_reason(
    raw_text: str,
) -> StateSupervisionPeriodAdmissionReason:
    """Custom parser for concatenated non-parole start reason codes."""
    # TODO(#21687): Separate parsers are currently needed to correctly interpret G01
    # (Intake new case) for parole and non-parole periods. This could be handled more cleanly
    # by pulling event reason into the supervision period view, since that data distinguishes
    # between court sentences/incarceration releases for G01 events.
    codes = raw_text.split("-")

    if "G01" in codes:
        return StateSupervisionPeriodAdmissionReason.COURT_SENTENCE
    if "S41" in codes:
        return StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION
    if "G05" in codes:
        return StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION
    if "L05" in codes:
        return StateSupervisionPeriodAdmissionReason.ABSCONSION
    if any(code in ["S48", "S31"] for code in codes):
        return StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION
    if "G03" in codes:
        return StateSupervisionPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION
    if any(
        code
        in [
            "G02",
            "G07",
            "G08",
            "S02",
            "S04",
            "S05",
            "S11",
            "S12",
            "S27",
            "S28",
            "S35",
            "S36",
            "S37",
            "S38",
            "S40",
        ]
        for code in codes
    ):
        return StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE

    return StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN


def parse_parole_period_start_reason(
    raw_text: str,
) -> StateSupervisionPeriodAdmissionReason:
    """Custom parser for concatenated parole start reason codes."""
    codes = raw_text.split("-")

    if "G01" in codes:
        return StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION

    return parse_non_parole_period_start_reason(raw_text)


def parse_supervision_period_end_reason(
    raw_text: str,
) -> StateSupervisionPeriodTerminationReason:
    """Custom parser for concatenated end reason codes."""
    codes = raw_text.split("-")
    if any(code in ["L05", "S25"] for code in codes):
        if "G05" in codes:
            # Same-day absconsions and returns are treated as unknown.
            return StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN
        return StateSupervisionPeriodTerminationReason.ABSCONSION
    if "G05" in codes:
        return StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION
    if any(code in ["L21", "S30", "S47"] for code in codes):
        return StateSupervisionPeriodTerminationReason.ADMITTED_TO_INCARCERATION
    if "L10" in codes:
        return StateSupervisionPeriodTerminationReason.DEATH
    if any(code in ["L12", "L13"] for code in codes):
        return StateSupervisionPeriodTerminationReason.DISCHARGE
    if "L11" in codes:
        return StateSupervisionPeriodTerminationReason.EXPIRATION
    if "L09" in codes:
        return StateSupervisionPeriodTerminationReason.PARDONED
    if any(code in ["L06", "L07", "L08"] for code in codes):
        return StateSupervisionPeriodTerminationReason.REVOCATION
    if "L03" in codes:
        return StateSupervisionPeriodTerminationReason.TRANSFER_TO_OTHER_JURISDICTION
    if any(
        code
        in [
            "G02",
            "G07",
            "G08",
            "S02",
            "S04",
            "S05",
            "S11",
            "S12",
            "S13",
            "S27",
            "S28",
            "S31",
            "S35",
            "S36",
            "S37",
            "S38",
            "S40",
            "S41",
        ]
        for code in codes
    ):
        return StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE
    if "S90" in codes:
        return StateSupervisionPeriodTerminationReason.VACATED
    return StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN
