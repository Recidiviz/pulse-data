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
from typing import Dict, List, Optional, Type

import attr

from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.state import state_person
from recidiviz.justice_counts.dimensions.base import Dimension, DimensionBase
from recidiviz.justice_counts.dimensions.helpers import (
    assert_no_overrides,
    build_entity_overrides,
    parse_entity_enum,
    raw_for_dimension_cls,
)


@attr.s(frozen=True)
class Race(Dimension):
    """
    Dimension that represents the type race
    """

    value: str = attr.ib()

    @classmethod
    def get(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> "Race":
        parsed_enum = parse_entity_enum(
            state_person.StateRace, dimension_cell_value, enum_overrides
        )
        return cls(parsed_enum.value)

    @classmethod
    def build_overrides(cls, mapping_overrides: Dict[str, str]) -> EnumOverrides:
        return build_entity_overrides(state_person.StateRace, mapping_overrides)

    @classmethod
    def is_normalized(cls) -> bool:
        return True

    @classmethod
    def dimension_identifier(cls) -> str:
        return "global/race"

    @classmethod
    def get_generated_dimension_classes(cls) -> List[Type[Dimension]]:
        return [raw_for_dimension_cls(cls)]

    @classmethod
    def generate_dimension_classes(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> List[Dimension]:
        return [raw_for_dimension_cls(cls).get(dimension_cell_value)]

    @property
    def dimension_value(self) -> str:
        return self.value


@attr.s(frozen=True)
class Ethnicity(Dimension):
    """
    Dimension that represents the type of ethnicity
    """

    value: str = attr.ib()

    @classmethod
    def get(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> "Ethnicity":
        parsed_enum = parse_entity_enum(
            state_person.StateEthnicity, dimension_cell_value, enum_overrides
        )
        return cls(parsed_enum.value)

    @classmethod
    def build_overrides(cls, mapping_overrides: Dict[str, str]) -> EnumOverrides:
        return build_entity_overrides(state_person.StateEthnicity, mapping_overrides)

    @classmethod
    def is_normalized(cls) -> bool:
        return True

    @classmethod
    def dimension_identifier(cls) -> str:
        return "global/ethnicity"

    @classmethod
    def get_generated_dimension_classes(cls) -> List[Type[Dimension]]:
        return [raw_for_dimension_cls(cls)]

    @classmethod
    def generate_dimension_classes(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> List[Dimension]:
        return [raw_for_dimension_cls(cls).get(dimension_cell_value)]

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
class Gender(Dimension):
    """
    Dimension that represents the type of gender
    """

    value: str = attr.ib()

    @classmethod
    def get(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> "Gender":
        parsed_enum = parse_entity_enum(
            state_person.StateGender, dimension_cell_value, enum_overrides
        )
        return cls(parsed_enum.value)

    @classmethod
    def build_overrides(cls, mapping_overrides: Dict[str, str]) -> EnumOverrides:
        return build_entity_overrides(state_person.StateGender, mapping_overrides)

    @classmethod
    def is_normalized(cls) -> bool:
        return True

    @classmethod
    def dimension_identifier(cls) -> str:
        return "global/gender"

    @classmethod
    def get_generated_dimension_classes(cls) -> List[Type[Dimension]]:
        return [raw_for_dimension_cls(cls)]

    @classmethod
    def generate_dimension_classes(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> List[Dimension]:
        return [raw_for_dimension_cls(cls).get(dimension_cell_value)]

    @property
    def dimension_value(self) -> str:
        return self.value


@attr.s(frozen=True)
class Age(Dimension):
    """
    Dimension that represents the age
    """

    value: str = attr.ib()

    @classmethod
    def get(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> "Age":
        assert_no_overrides(cls, enum_overrides)
        return cls(dimension_cell_value)

    @classmethod
    def build_overrides(cls, mapping_overrides: Dict[str, str]) -> EnumOverrides:
        raise ValueError("Can't create overrides for this class")

    @classmethod
    def is_normalized(cls) -> bool:
        return False

    @classmethod
    def dimension_identifier(cls) -> str:
        return "global/age/raw"

    @classmethod
    def get_generated_dimension_classes(cls) -> List[Type[Dimension]]:
        return []

    @classmethod
    def generate_dimension_classes(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> List[Dimension]:
        return []

    @property
    def dimension_value(self) -> str:
        return self.value
