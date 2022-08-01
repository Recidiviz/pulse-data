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
"""Dimension subclasses used for Jail system metrics."""


import enum

from recidiviz.justice_counts.dimensions.base import DimensionBase


class ReadmissionType(DimensionBase, enum.Enum):
    NEW_ADMISSION = "New Admission"
    VIOLATION_OF_CONDITIONS = "Violation of Conditions"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/reported_crime/type"


class JailPopulationType(DimensionBase, enum.Enum):
    PRETRIAL = "Pretrial"
    SENTENCED = "Sentenced"
    TRANSFER_OR_HOLD = "Transfer or Hold"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/population/type"


class PrisonPopulationType(DimensionBase, enum.Enum):
    NEW_SENTENCE = "New Sentence"
    TRANSFER_OR_HOLD = "Transfer or Hold"
    SUPERVISION_VIOLATION_OR_REVOCATION = "Supervision Violation or Revocation"
    OTHER = "Other"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/prison/population/type"


class JailReleaseType(DimensionBase, enum.Enum):
    SENTENCE_COMPLETION = "Sentence Completion"
    PRETRIAL_RELEASE = "Pretrial Release"
    TRANSFER = "Transfer"
    UNAPPROVED_ABSENCE = "Unapproved Absence"
    COMPASSIONATE = "Compassionate"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/release/type"


class PrisonReleaseTypes(DimensionBase, enum.Enum):
    SENTENCE_COMPLETION = "Sentence Completion"
    TO_SUPERVISION = "To Supervision"
    TRANSFER = "Transfer"
    UNAPPROVED_ABSENCE = "Unapproved Absence"
    COMPASSIONATE_RELEASE = "Compassionate Release"
    UNKNOWN = "Unknown"
    OTHER = "Other"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/prisons/release/type"


class CorrectionalFacilityForceType(DimensionBase, enum.Enum):
    PHYSICAL = "Physical"
    RESTRAINT = "Restraint"
    VERBAL = "Verbal"
    WEAPON_INVOLVED = "Weapon"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/correctional_facility/force/type"


class CorrectionalFacilityStaffType(DimensionBase, enum.Enum):
    SECURITY = "Security"
    SUPPORT = "Support"
    OTHER = "Other"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/staff/correctional_facility/type"
