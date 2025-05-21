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

from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_employment_period import (
    StateEmploymentPeriodEmploymentStatus,
    StateEmploymentPeriodEndReason,
)
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentencingAuthority,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority


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


def parse_supervision_case_type(
    raw_text: str,
) -> StateSupervisionCaseType:
    """
    Determine supervision case type from supervisedLevel field
    """
    if "SO:" in raw_text:
        return StateSupervisionCaseType.SEX_OFFENSE

    if "DV:" in raw_text:
        return StateSupervisionCaseType.DOMESTIC_VIOLENCE

    return StateSupervisionCaseType.GENERAL
