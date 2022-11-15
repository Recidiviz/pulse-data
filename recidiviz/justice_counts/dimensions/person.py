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

from recidiviz.common.constants.county import person_characteristics
from recidiviz.common.constants.enum_overrides import EnumOverrides
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
            person_characteristics.Race, dimension_cell_value, enum_overrides
        )
        return cls(parsed_enum.value)

    @classmethod
    def build_overrides(cls, mapping_overrides: Dict[str, str]) -> EnumOverrides:
        return build_entity_overrides(person_characteristics.Race, mapping_overrides)

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
            person_characteristics.Ethnicity, dimension_cell_value, enum_overrides
        )
        return cls(parsed_enum.value)

    @classmethod
    def build_overrides(cls, mapping_overrides: Dict[str, str]) -> EnumOverrides:
        return build_entity_overrides(
            person_characteristics.Ethnicity, mapping_overrides
        )

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

    # Hispanic
    HISPANIC_AMERICAN_INDIAN_ALASKAN_NATIVE = (
        "American Indian / Alaskan Native / Hispanic"
    )
    HISPANIC_ASIAN = "Asian / Hispanic"
    HISPANIC_BLACK = "Black / Hispanic"
    HISPANIC_MORE_THAN_ONE_RACE = "More than one race / Hispanic"
    HISPANIC_NATIVE_HAWAIIAN_PACIFIC_ISLANDER = (
        "Native Hawaiian / Pacific Islander / Hispanic"
    )
    HISPANIC_WHITE = "White / Hispanic"
    HISPANIC_OTHER = "Other / Hispanic"
    HISPANIC_UNKNOWN = "Unknown / Hispanic"

    # Not Hispanic
    NOT_HISPANIC_AMERICAN_INDIAN_ALASKAN_NATIVE = (
        "American Indian / Alaskan Native / Not Hispanic"
    )
    NOT_HISPANIC_ASIAN = "Asian / Not Hispanic"
    NOT_HISPANIC_BLACK = "Black / Not Hispanic"
    NOT_HISPANIC_MORE_THAN_ONE_RACE = "More than one race / Not Hispanic"
    NOT_HISPANIC_NATIVE_HAWAIIAN_PACIFIC_ISLANDER = (
        "Native Hawaiian / Pacific Islander / Not Hispanic"
    )
    NOT_HISPANIC_WHITE = "White / Not Hispanic"
    NOT_HISPANIC_OTHER = "Other / Not Hispanic"
    NOT_HISPANIC_UNKNOWN = "Unknown / Not Hispanic"

    # Unknown Ethnicity
    UNKNOWN_ETHNICITY_AMERICAN_INDIAN_ALASKAN_NATIVE = (
        "American Indian / Alaskan Native / Unknown Ethnicity"
    )
    UNKNOWN_ETHNICITY_ASIAN = "Asian / Unknown Ethnicity"
    UNKNOWN_ETHNICITY_BLACK = "Black / Unknown Ethnicity"
    UNKNOWN_ETHNICITY_MORE_THAN_ONE_RACE = "More than one race / Unknown Ethnicity"
    UNKNOWN_ETHNICITY_NATIVE_HAWAIIAN_PACIFIC_ISLANDER = (
        "Native Hawaiian / Pacific Islander / Unknown Ethnicity"
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
            return "American Indian / Alaskan Native"
        if "ASIAN" in self.name:
            return "Asian"
        if "BLACK" in self.name:
            return "Black"
        if "MORE_THAN_ONE_RACE" in self.name:
            return "More than one race"
        if "NATIVE_HAWAIIAN_PACIFIC_ISLANDER" in self.name:
            return "Native Hawaiian / Pacific Islander"
        if "OTHER" in self.name:
            return "Other"
        if "WHITE" in self.name:
            return "White"
        return "Unknown"

    @property
    def ethnicity(self) -> str:
        if "NOT_HISPANIC" in self.name:
            return "Not Hispanic"
        if "HISPANIC" in self.name:
            return "Hispanic"
        return "Unknown Ethnicity"


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
            person_characteristics.Gender, dimension_cell_value, enum_overrides
        )
        return cls(parsed_enum.value)

    @classmethod
    def build_overrides(cls, mapping_overrides: Dict[str, str]) -> EnumOverrides:
        return build_entity_overrides(person_characteristics.Gender, mapping_overrides)

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
