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
"""Custom enum parsers functions for US_NE. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_ne_custom_enum_parsers.<function name>
"""
from typing import Optional

from recidiviz.common.constants.state.state_employment_period import (
    StateEmploymentPeriodEmploymentStatus,
    StateEmploymentPeriodEndReason,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentSeverity,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodHousingUnitCategory,
    StateIncarcerationPeriodHousingUnitType,
)
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentencingAuthority,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)


def parse_sentencing_authority(
    raw_text: str,
) -> Optional[StateSentencingAuthority]:
    """
    Determine sentencing authority from county
    """
    COUNTY = raw_text

    if COUNTY == "US MARSHAL/ATTORNEY":
        return StateSentencingAuthority.FEDERAL

    if COUNTY == "OUT OF STATE":
        return StateSentencingAuthority.OTHER_STATE

    return StateSentencingAuthority.COUNTY


def parse_custodial_authority(
    raw_text: str,
) -> Optional[StateCustodialAuthority]:
    """
    Determine custodial authority from in/out state indicator codes
    See https://drive.google.com/drive/folders/1C1GDoQFttK_gJtOR0dcfJ2mRd9o2PVuI
    for Nebraska usage of inOutStateIndicator1Code (code1),
    inOutStateIndicator2Code (location code), and inOutStateIndicator3Code (code2)
    """
    code1, location, code2 = raw_text.split("@@")

    if ((code1 == "2" and code2 == "1") or code1 == "4") and location != "NE":
        return StateCustodialAuthority.OTHER_STATE

    return StateCustodialAuthority.SUPERVISION_AUTHORITY


def parse_employment_status(
    raw_text: str,
) -> Optional[StateEmploymentPeriodEmploymentStatus]:
    """
    Determine employment status from employmentStatus field and employmentType
    """
    status, work_type = raw_text.split("@@")

    if status == "CURRENT EMPLOYER" and work_type == "PART-TIME":
        return StateEmploymentPeriodEmploymentStatus.EMPLOYED_PART_TIME

    if status == "CURRENT EMPLOYER" and work_type == "FULL-TIME":
        return StateEmploymentPeriodEmploymentStatus.EMPLOYED_FULL_TIME

    if status in ("CURRENT EMPLOYER", "SECOND JOB"):
        return StateEmploymentPeriodEmploymentStatus.EMPLOYED_UNKNOWN_AMOUNT

    if status in (
        "DISABLED, UNABLE TO WORK OR FINISH PROGRAM",
        "TEMP ILLNESS / INJURY",
    ):
        return StateEmploymentPeriodEmploymentStatus.UNABLE_TO_WORK

    if status in ("RETIRED", "SOCIAL SECURITY DISABILITY"):
        return StateEmploymentPeriodEmploymentStatus.ALTERNATE_INCOME_SOURCE

    return StateEmploymentPeriodEmploymentStatus.INTERNAL_UNKNOWN


def parse_employment_endReason(
    raw_text: str,
) -> Optional[StateEmploymentPeriodEndReason]:
    """
    Determine employment status from employmentStatus field and employmentType
    """

    if raw_text == "RETIRED":
        return StateEmploymentPeriodEndReason.RETIRED

    if raw_text == "REVOKED":
        return StateEmploymentPeriodEndReason.INCARCERATED

    if raw_text in (
        "TEMP ILLNESS / INJURY",
        "DISABLED, UNABLE TO WORK OR FINISH PROGRAM",
        "SOCIAL SECURITY DISABILITY",
    ):
        return StateEmploymentPeriodEndReason.MEDICAL

    if raw_text == "EMPLOYER-TERMINATED":
        return StateEmploymentPeriodEndReason.FIRED

    if raw_text == "SELF-TERMINATED":
        return StateEmploymentPeriodEndReason.QUIT

    if raw_text == "LAID OFF / BETWEEN TEMP ASSIGNMENTS":
        return StateEmploymentPeriodEndReason.LAID_OFF

    if raw_text in ("CHANGED JOBS WITHOUT PERMISSION", "CHANGED JOBS WITH PERMISSION"):
        return StateEmploymentPeriodEndReason.NEW_JOB

    return StateEmploymentPeriodEndReason.INTERNAL_UNKNOWN


def parse_sentence_status(
    raw_text: str,
) -> StateSentenceStatus:
    """
    Determine sentence status based on end date of last period.
    We expect a lot of end_reason to be null because they are only populated if the persons
    """
    end_date, end_reason = raw_text.split("@@")

    # Our schema considers ammended as still serving, so only mapping as ammended when
    # sentence is still ongoing
    if (end_date is None or end_date == "NONE") and end_reason == "SENTENCE AMENDED":
        return StateSentenceStatus.AMENDED

    if end_date is None or end_date == "NONE":
        return StateSentenceStatus.SERVING

    if end_reason in ("SENTENCE VACATED", "CASE DISMISSED"):
        return StateSentenceStatus.VACATED

    return StateSentenceStatus.COMPLETED


def parse_housing_unit_type(
    raw_text: str,
) -> Optional[StateIncarcerationPeriodHousingUnitType]:
    """
    Maps |housing_unit|, to its corresponding StateIncarcerationPeriodHousingUnitType.
    #TODO(#43055): Update solitary housing unit type once we have more information on
    Nebraska's solitary confinement housing units.

    """
    if raw_text in (
        "OCC SEGREGATION UNIT"
        "SEGREGATION B"
        "SPECIAL MANAGEMENT UNIT NCYF"
        "SPECIAL MANAGEMENT UNIT E  TSC"
        "NCW MAIN SEGREGATION UNIT"
        "NNC SEGREGATION UNIT #1"
        "SPECIAL MANAGEMENT UNIT B  TSC"
        "SPECIAL MANAGEMENT UNIT C  TSC"
        "SPECIAL MANAGEMENT UNIT A  TSC"
        "SPECIAL MANAGEMENT UNIT F  TSC"
        "SPECIAL MANAGEMENT UNIT D  TSC"
        "SEGREGATION, COMM CORRECTIONS"
        "HDC SEGREGATION UNIT"
    ):
        return StateIncarcerationPeriodHousingUnitType.OTHER_SOLITARY_CONFINEMENT

    if raw_text == "PROTECTIVE CUSTODY":
        return StateIncarcerationPeriodHousingUnitType.PROTECTIVE_CUSTODY

    if raw_text in (
        "NEBRASKA HEART HOSPITAL",
        "IMMANUEL MEDICAL CENTER",
        "HAYES KS MEDICDAL CENTER",
        "MIDLANDS HOSPITAL",
        "INSIDE HOSPITAL",
        "LINCOLN GENERAL HOSPITAL",
        "CREIGHTON UNIV MEDICAL CENTER",
        "BERGAN MERCY HOSPITAL",
        "ST. JOSEPH HOSPITAL - OMAHA",
        "METHODIST HOSPITAL - OMAHA",
        "MCCOOK COMMUNITY HOSPITAL",
        "BELLEVUE MEDICAL CENTER",
        "LINCOLN SURGICAL HOSPITAL",
        "RICHARD YOUNG HOSPITAL",
        "GREAT PLAINS MED CNTR-NTH PLTT",
        "REGIONAL WEST MEDICAL CENTER",
        "ST. ELIZABETH HOSPITAL",
        "UNIVERSITY MEDICAL CENTER",
        "OCC MEDICAL OBSERVATION",
        "CHI SELECT SPECIALTY HOSPITAL",
        "CLARKSON HOSPITAL",
        "BRYAN MEMORIAL HOSPITAL",
        "YORK GENERAL HOSPITAL",
        "JOHNSON COUNTY HOSPITAL",
        "WARREN MEMORIAL HOSP FRIEND",
        "VETERAN'S HOSPITAL",
    ):
        return StateIncarcerationPeriodHousingUnitType.HOSPITAL

    if not raw_text:
        return None
    return StateIncarcerationPeriodHousingUnitType.GENERAL


def parse_housing_unit_category(
    raw_text: str,
) -> Optional[StateIncarcerationPeriodHousingUnitCategory]:
    """
    Maps |housing_unit|, to its corresponding StateIncarcerationPeriodHousingUnitCategory,
    identifying which housing units are solitary confinement units and which are general.
    """

    if any(
        keyword in raw_text for keyword in ["SPECIAL MANAGEMENT UNIT", "SEGREGATION"]
    ):
        return StateIncarcerationPeriodHousingUnitCategory.SOLITARY_CONFINEMENT

    if not raw_text:
        return None

    return StateIncarcerationPeriodHousingUnitCategory.GENERAL


def parse_incarceration_custodial_authority(
    raw_text: str,
) -> Optional[StateCustodialAuthority]:
    """
    Uses LOCT_PRFX_DESC and ADMISSION_TYPE_DESC to determine custodial authority.
    """

    location, admission = raw_text.split("@@")

    if location == "COUNTY JAIL":
        return StateCustodialAuthority.COUNTY

    if admission == "OUT OF STATE TRANSFER":
        return StateCustodialAuthority.OTHER_STATE

    if admission == "TO FEDERAL CUSTODY":
        return StateCustodialAuthority.FEDERAL

    return StateCustodialAuthority.STATE_PRISON


def parse_incarceration_type(
    raw_text: str,
) -> Optional[StateIncarcerationType]:
    """
    Uses LOCT_PRFX_DESC and ADMISSION_TYPE to determine incarceration type.
    """
    location, admission = raw_text.split("@@")

    if location == "COUNTY JAIL":
        return StateIncarcerationType.COUNTY_JAIL

    if admission == "OUT OF STATE TRANSFER":
        return StateIncarcerationType.OUT_OF_STATE

    if admission == "TO FEDERAL CUSTODY":
        return StateIncarcerationType.FEDERAL_PRISON

    return StateIncarcerationType.STATE_PRISON


def parse_incident_severity(
    raw_text: str,
) -> Optional[StateIncarcerationIncidentSeverity]:
    """
    Uses ChargeCodeDesc to determine incident severity.
    """

    if raw_text.startswith("1"):
        return StateIncarcerationIncidentSeverity.HIGHEST

    if raw_text.startswith("2"):
        return StateIncarcerationIncidentSeverity.SECOND_HIGHEST

    if raw_text.startswith("3"):
        return StateIncarcerationIncidentSeverity.THIRD_HIGHEST

    return StateIncarcerationIncidentSeverity.INTERNAL_UNKNOWN


def parse_supervision_type(
    raw_text: str,
) -> Optional[StateSupervisionPeriodSupervisionType]:
    """
    Uses startReason to determine supervision type.
    All supervision data in Nebraska is parole, which is why we only change
    the type if the supervision period is an absconsion.
    """

    if raw_text == "ABSCOND":
        return StateSupervisionPeriodSupervisionType.ABSCONSION

    return StateSupervisionPeriodSupervisionType.PAROLE


def parse_clinical_participation(
    raw_text: str,
) -> Optional[StateProgramAssignmentParticipationStatus]:
    """
    Determine facility clinical program participation status
    """
    accept_refuse, outcome, complete_date = raw_text.split("@@")

    if accept_refuse == "Refused":
        return StateProgramAssignmentParticipationStatus.REFUSED

    if outcome in (
        "Adequate Progression",
        "Satisfactory Progression",
        "Met SO treatment goals",
    ):
        return StateProgramAssignmentParticipationStatus.DISCHARGED_SUCCESSFUL

    if outcome in (
        "Terminated from Program",
        "Withdrawn from Program",
        "Did not meet SO treatment goals",
    ):
        return StateProgramAssignmentParticipationStatus.DISCHARGED_UNSUCCESSFUL

    if accept_refuse is None and outcome is None and complete_date is not None:
        return StateProgramAssignmentParticipationStatus.DISCHARGED_UNKNOWN

    return StateProgramAssignmentParticipationStatus.INTERNAL_UNKNOWN
