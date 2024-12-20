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
"""Dimension subclasses used for global person characteristic filters."""

import enum

import attr

from recidiviz.justice_counts.dimensions.base import DimensionBase


@attr.s(frozen=True)
class Race(DimensionBase):
    """
    Dimension that represents the type race
    """

    value: str = attr.ib()

    @classmethod
    def dimension_identifier(cls) -> str:
        return "global/race"

    @property
    def dimension_value(self) -> str:
        return self.value


class RaceAndEthnicity(DimensionBase, enum.Enum):
    """Class for Justice Counts Race and Ethnicity breakdowns"""

    # Note: if we change Race/Ethnicity values, we have to update the frontend too
    # as the frontend contains conditionals that are dependent on these values

    # Hispanic or Latino
    HISPANIC_AMERICAN_INDIAN_ALASKAN_NATIVE = (
        "American Indian or Alaska Native / Hispanic or Latino"
    )
    HISPANIC_ASIAN = "Asian / Hispanic or Latino"
    HISPANIC_BLACK = "Black / Hispanic or Latino"
    HISPANIC_MORE_THAN_ONE_RACE = "More than one race / Hispanic or Latino"
    HISPANIC_NATIVE_HAWAIIAN_PACIFIC_ISLANDER = (
        "Native Hawaiian or Pacific Islander / Hispanic or Latino"
    )
    HISPANIC_WHITE = "White / Hispanic or Latino"
    HISPANIC_OTHER = "Other / Hispanic or Latino"
    HISPANIC_UNKNOWN = "Unknown / Hispanic or Latino"

    # Not Hispanic or Latino
    NOT_HISPANIC_AMERICAN_INDIAN_ALASKAN_NATIVE = (
        "American Indian or Alaska Native / Not Hispanic or Latino"
    )
    NOT_HISPANIC_ASIAN = "Asian / Not Hispanic or Latino"
    NOT_HISPANIC_BLACK = "Black / Not Hispanic or Latino"
    NOT_HISPANIC_MORE_THAN_ONE_RACE = "More than one race / Not Hispanic or Latino"
    NOT_HISPANIC_NATIVE_HAWAIIAN_PACIFIC_ISLANDER = (
        "Native Hawaiian or Pacific Islander / Not Hispanic or Latino"
    )
    NOT_HISPANIC_WHITE = "White / Not Hispanic or Latino"
    NOT_HISPANIC_OTHER = "Other / Not Hispanic or Latino"
    NOT_HISPANIC_UNKNOWN = "Unknown / Not Hispanic or Latino"

    # Unknown Ethnicity
    UNKNOWN_ETHNICITY_AMERICAN_INDIAN_ALASKAN_NATIVE = (
        "American Indian or Alaska Native / Unknown Ethnicity"
    )
    UNKNOWN_ETHNICITY_ASIAN = "Asian / Unknown Ethnicity"
    UNKNOWN_ETHNICITY_BLACK = "Black / Unknown Ethnicity"
    UNKNOWN_ETHNICITY_MORE_THAN_ONE_RACE = "More than one race / Unknown Ethnicity"
    UNKNOWN_ETHNICITY_NATIVE_HAWAIIAN_PACIFIC_ISLANDER = (
        "Native Hawaiian or Pacific Islander / Unknown Ethnicity"
    )
    UNKNOWN_ETHNICITY_WHITE = "White / Unknown Ethnicity"
    UNKNOWN_ETHNICITY_OTHER = "Other / Unknown Ethnicity"
    UNKNOWN_ETHNICITY_UNKNOWN = "Unknown / Unknown Ethnicity"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "global/race_and_ethnicity"

    @classmethod
    def human_readable_name(cls) -> str:
        return "Race / Ethnicity"

    @classmethod
    def display_name(cls) -> str:
        return "Race / Ethnicities"

    @property
    def race(self) -> str:
        if "AMERICAN_INDIAN_ALASKAN_NATIVE" in self.name:
            return "American Indian or Alaska Native"
        if "ASIAN" in self.name:
            return "Asian"
        if "BLACK" in self.name:
            return "Black"
        if "MORE_THAN_ONE_RACE" in self.name:
            return "More than one race"
        if "NATIVE_HAWAIIAN_PACIFIC_ISLANDER" in self.name:
            return "Native Hawaiian or Pacific Islander"
        if "OTHER" in self.name:
            return "Other"
        if "WHITE" in self.name:
            return "White"
        return "Unknown"

    @property
    def ethnicity(self) -> str:
        if "NOT_HISPANIC" in self.name:
            return "Not Hispanic or Latino"
        if "HISPANIC" in self.name:
            return "Hispanic or Latino"
        return "Unknown Ethnicity"


class CensusRace(DimensionBase, enum.Enum):
    AMERICAN_INDIAN_ALASKAN_NATIVE = "American Indian or Alaska Native"
    ASIAN = "Asian"
    BLACK = "Black"
    MORE_THAN_ONE_RACE = "More than one race"
    NATIVE_HAWAIIAN_PACIFIC_ISLANDER = "Native Hawaiian or Pacific Islander"
    OTHER = "Other"
    WHITE = "White"
    UNKNOWN = "Unknown"
    HISPANIC_OR_LATINO = "Hispanic or Latino"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "global/race/census"

    @property
    def dimension_value(self) -> str:
        return self.value


class BiologicalSex(DimensionBase, enum.Enum):
    MALE = "Male Biological Sex"
    FEMALE = "Female Biological Sex"
    UNKNOWN = "Unknown Biological Sex"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "global/biological_sex"

    @classmethod
    def display_name(cls) -> str:
        return "Biological Sex"


class GenderRestricted(DimensionBase, enum.Enum):
    MALE = "Male"
    FEMALE = "Female"
    OTHER = "Other"
    NON_BINARY = "Non-Binary"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "global/gender/restricted"

    @classmethod
    def human_readable_name(cls) -> str:
        return "Gender"

    @classmethod
    def display_name(cls) -> str:
        return "Gender"


@attr.s(frozen=True)
class Gender(DimensionBase):
    """
    Dimension that represents the type of gender
    """

    value: str = attr.ib()

    @classmethod
    def dimension_identifier(cls) -> str:
        return "global/gender"

    @property
    def dimension_value(self) -> str:
        return self.value
