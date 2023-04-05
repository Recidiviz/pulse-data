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
from enum import Enum
from typing import Union

import attr

from recidiviz.common import fips
from recidiviz.common.constants.states import StateCode
from recidiviz.justice_counts.dimensions.base import DimensionBase


class Country(DimensionBase, Enum):
    """
    Dimension that represents the country
    """

    US = "US"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "global/location/country"

    @property
    def dimension_value(self) -> str:
        return self.value


@attr.s(frozen=True)
class State(DimensionBase):
    """
    Dimension that represents the state. Takes state code as input. example: 'US_WI'
    """

    state_code: StateCode = attr.ib(converter=StateCode)

    @classmethod
    def dimension_identifier(cls) -> str:
        return "global/location/state"

    @property
    def dimension_value(self) -> str:
        return self.state_code.value


@attr.s(frozen=True)
class County(DimensionBase):
    """
    Dimension that represents the county. Takes county code as input. example: 'US_WI_MILWAUKEE'
    """

    county_code: str = attr.ib()

    @classmethod
    def dimension_identifier(cls) -> str:
        return "global/location/county"

    @property
    def dimension_value(self) -> str:
        return self.county_code

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
class CountyFIPS(DimensionBase):
    """
    Dimension that represents the county fips. Can also generate State and County dimensions from fips value.
    """

    value: str = attr.ib(converter=_fips_zero_pad)

    @classmethod
    def dimension_identifier(cls) -> str:
        return "global/location/county-fips"

    @property
    def dimension_value(self) -> str:
        return self.value


Location = Union[Country, State, County]

# TODO(#4473): Make this per jurisdiction


@attr.s(frozen=True)
class Facility(DimensionBase):
    """
    Dimension that represents the facility
    """

    name: str = attr.ib()

    @classmethod
    def dimension_identifier(cls) -> str:
        return "global/facility/raw"

    @property
    def dimension_value(self) -> str:
        return self.name


@attr.s(frozen=True)
class Agency(DimensionBase):
    """
    Dimension that represents the reporting agency
    """

    name: str = attr.ib()

    @classmethod
    def dimension_identifier(cls) -> str:
        return "global/agency/raw"

    @property
    def dimension_value(self) -> str:
        return self.name
