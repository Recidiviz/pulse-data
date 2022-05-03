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
"""Dimension subclasses used for global location filters."""


from typing import Dict, List, Optional, Type, Union

import attr

from recidiviz.common import fips
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.states import StateCode
from recidiviz.justice_counts.dimensions.base import Dimension
from recidiviz.justice_counts.dimensions.helpers import (
    assert_no_overrides,
    build_entity_overrides,
    parse_entity_enum,
    raw_for_dimension_cls,
)


class Country(Dimension, EntityEnum, metaclass=EntityEnumMeta):
    """
    Dimension that represents the country
    """

    US = "US"

    @classmethod
    def get(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> "Country":
        return parse_entity_enum(cls, dimension_cell_value, enum_overrides)

    @classmethod
    def build_overrides(cls, mapping_overrides: Dict[str, str]) -> EnumOverrides:
        return build_entity_overrides(cls, mapping_overrides)

    @classmethod
    def is_normalized(cls) -> bool:
        return True

    @classmethod
    def dimension_identifier(cls) -> str:
        return "global/location/country"

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

    @classmethod
    def _get_default_map(cls) -> Dict[str, "Country"]:
        return {}


@attr.s(frozen=True)
class State(Dimension):
    """
    Dimension that represents the state. Takes state code as input. example: 'US_WI'
    """

    state_code: StateCode = attr.ib(converter=StateCode)

    @classmethod
    def get(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> "State":
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
        return "global/location/state"

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
        return self.state_code.value


@attr.s(frozen=True)
class County(Dimension):
    """
    Dimension that represents the county. Takes county code as input. example: 'US_WI_MILWAUKEE'
    """

    county_code: str = attr.ib()

    @classmethod
    def get(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> "County":
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
        return "global/location/county"

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
        return self.county_code

    @classmethod
    def _get_default_map(cls) -> Dict[str, "County"]:
        return {}

    @county_code.validator
    def _value_is_valid(self, _attribute: attr.Attribute, county_code: str) -> None:
        if not county_code.isupper():
            raise ValueError(
                f"Invalid county code '{county_code}' must be uppercase. "
                f"ex: 'US_NY_NEW_YORK'"
            )
        fips.validate_county_code(county_code.lower())


def _fips_zero_pad(fips_raw: str) -> str:
    return fips_raw.zfill(5)


@attr.s(frozen=True)
class CountyFIPS(Dimension):
    """
    Dimension that represents the county fips. Can also generate State and County dimensions from fips value.
    """

    value: str = attr.ib(converter=_fips_zero_pad)

    @classmethod
    def get(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> "CountyFIPS":
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
        return "global/location/county-fips"

    @classmethod
    def get_generated_dimension_classes(cls) -> List[Type[Dimension]]:
        return [State, County]

    @classmethod
    def generate_dimension_classes(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> List[Dimension]:
        [state, county] = fips.get_state_and_county_for_fips(
            _fips_zero_pad(dimension_cell_value)
        )
        return [State.get(state), County.get(f"{state}_{county}")]

    @property
    def dimension_value(self) -> str:
        return self.value

    @classmethod
    def _get_default_map(cls) -> Dict[str, "CountyFIPS"]:
        return {}


Location = Union[Country, State, County]

# TODO(#4473): Make this per jurisdiction


@attr.s(frozen=True)
class Facility(Dimension):
    """
    Dimension that represents the facility
    """

    name: str = attr.ib()

    @classmethod
    def get(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> "Facility":
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
        return "global/facility/raw"

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
        return self.name


@attr.s(frozen=True)
class Agency(Dimension):
    """
    Dimension that represents the reporting agency
    """

    name: str = attr.ib()

    @classmethod
    def get(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> "Agency":
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
        return "global/agency/raw"

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
        return self.name
