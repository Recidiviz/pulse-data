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
"""Custom enum parsers functions for US_UT. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_ut_custom_enum_parsers.<function name>
"""
from recidiviz.common.constants.state.state_charge import (
    StateChargeV2ClassificationType,
)
from recidiviz.common.constants.state.state_employment_period import (
    StateEmploymentPeriodEmploymentStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentencingAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)


def parse_employment_status(
    raw_text: str,
) -> StateEmploymentPeriodEmploymentStatus:
    """Determines the status of an employment period. If a person's job title does not
    signify that they are unemployed, we assume their appearance in the emplymt raw data
    table means they are employed in some capacity."""
    hours_per_week, job_title, comment = raw_text.split("@@")
    if (
        "UNEMPL" in job_title.upper()
        or job_title.upper() == "NONE"
        or "UNEMPL" in comment
    ):
        return StateEmploymentPeriodEmploymentStatus.UNEMPLOYED
    if hours_per_week != "NONE":
        if int(float(hours_per_week)) >= 40:
            return StateEmploymentPeriodEmploymentStatus.EMPLOYED_FULL_TIME
        return StateEmploymentPeriodEmploymentStatus.EMPLOYED_PART_TIME
    return StateEmploymentPeriodEmploymentStatus.EMPLOYED_UNKNOWN_AMOUNT


def parse_charge_classification_type(raw_text: str) -> StateChargeV2ClassificationType:
    if raw_text:
        if "MISDEMEANOR" in raw_text:
            return StateChargeV2ClassificationType.MISDEMEANOR
        if "FELONY" in raw_text:
            return StateChargeV2ClassificationType.FELONY
        return StateChargeV2ClassificationType.INTERNAL_UNKNOWN
    return StateChargeV2ClassificationType.INTERNAL_UNKNOWN


def parse_sentencing_authority(raw_text: str) -> StateSentencingAuthority:
    if raw_text:
        # Justice Courts are established by counties and municipalities and have the
        # authority to deal with class B and C misdemeanors, violations of ordinances,
        # small claims, and infractions committed within their territorial jurisdiction.
        if "JUSTICE" in raw_text:
            return StateSentencingAuthority.COUNTY
        if "FEDERAL" in raw_text or raw_text == "US SUPREME COURT":
            return StateSentencingAuthority.FEDERAL
        if "COMPACT" in raw_text:
            return StateSentencingAuthority.OTHER_STATE
        # The catch-all STATE sentencing authority here includes:
        # District Courts - the state trial court of general jurisdiction.
        # Juvenile Courts - of equal status with the District Court.
        # Circuit Courts - can be at a state or federal level, but are all associated
        # with a region in Utah in this data, so we assume they are state circuits.
        return StateSentencingAuthority.STATE
    return StateSentencingAuthority.INTERNAL_UNKNOWN


def parse_supervision_type(raw_text: str) -> StateSupervisionPeriodSupervisionType:
    if raw_text:
        if "PROBATION" in raw_text:
            return StateSupervisionPeriodSupervisionType.PROBATION
        if "PAROLE" in raw_text:
            return StateSupervisionPeriodSupervisionType.PAROLE
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
    return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
