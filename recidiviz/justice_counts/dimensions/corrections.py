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
"""Dimension subclasses used for Corrections system metrics."""
from enum import Enum

from recidiviz.justice_counts.dimensions.base import DimensionBase


class PopulationType(DimensionBase, Enum):
    """
    Dimension that represents the type of populations
    """

    RESIDENTS = "RESIDENTS"
    PRISON = "PRISON"
    SUPERVISION = "SUPERVISION"
    JAIL = "JAIL"
    OTHER = "OTHER"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/population/type"

    @property
    def dimension_value(self) -> str:
        return self.value


class ReleaseType(DimensionBase, Enum):
    """
    Dimension that represents the type of incarceration release
    """

    # Release from prison to supervision
    TO_SUPERVISION = "TO_SUPERVISION"

    # Release that has been fully served
    COMPLETED = "COMPLETED"

    # Releases that are not covered above
    OTHER = "OTHER"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/release/type"

    @property
    def dimension_value(self) -> str:
        return self.value


class AdmissionType(DimensionBase, Enum):
    """Dimension that represents the type of incarceration admission"""

    # Admissions due to a new sentence from the community.
    NEW_COMMITMENT = "NEW_COMMITMENT"

    # Admissions of persons from supervision (e.g. revocation, dunk, etc.)
    FROM_SUPERVISION = "FROM_SUPERVISION"

    # Any other admissions (e.g. in CT this is used for pre-trial admissions)
    OTHER = "OTHER"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/admission/type"

    @property
    def dimension_value(self) -> str:
        return self.value


class SupervisionViolationType(DimensionBase, Enum):
    """
    Dimension that represents the type of supervision violation
    """

    NEW_CRIME = "NEW_CRIME"
    TECHNICAL = "TECHNICAL"
    OTHER = "OTHER"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/supervision_violation/type"

    @property
    def dimension_value(self) -> str:
        return self.value


class SupervisionType(DimensionBase, Enum):
    """Dimension that represents the type of supervision."""

    PAROLE = "PAROLE"
    PROBATION = "PROBATION"

    # Some jurisdictions have other types of supervision (e.g. DUI home confinement)
    OTHER = "OTHER"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/supervision/type"

    @property
    def dimension_value(self) -> str:
        return self.value
