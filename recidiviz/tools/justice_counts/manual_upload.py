# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Utilities for ingesting a report (as CSVs) into the Justice Counts database.
Will use service file if you have environment variable GOOGLE_APPLICATION_CREDENTIALS set,
allowing for automatic ingest instead of opening a web browser and having the user verify that the ingest was successful

To setup GOOGLE_APPLICATION_CREDENTIALS environment variable, got to IAM & Admin in Google Cloud,
and under Service Accounts go to the action manage keys for "App Engine default service account".
There you can create a new key to save in a secure location.
Point the environment variable "GOOGLE_APPLICATION_CREDENTIALS" to the file path of the credentials.
You can then use the script as normal, and it will automatically ingest instead of opening the browser.

Example usage:
python -m recidiviz.tools.justice_counts.manual_upload \
    --manifest-file recidiviz/tests/tools/justice_counts/reports/report1/manifest.yaml \
    --project-id recidiviz-staging
python -m recidiviz.tools.justice_counts.manual_upload \
    --manifest-file recidiviz/tests/tools/justice_counts/reports/report1/manifest.yaml \
    --project-id recidiviz-staging \
    --app-url http://127.0.0.1:5000
"""
import argparse
import datetime
import decimal
import enum
import logging
import os
import typing
import webbrowser
from abc import ABCMeta, abstractmethod
from collections import Counter, defaultdict
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from urllib import parse

import attr
import pandas
from more_itertools import peekable
from sqlalchemy import cast
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.schema import UniqueConstraint

from recidiviz.cloud_functions.cloud_function_utils import (
    IAP_CLIENT_ID,
    make_iap_request,
)
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common import fips
from recidiviz.common.constants import states
from recidiviz.common.constants.entity_enum import (
    EntityEnum,
    EntityEnumMeta,
    EntityEnumT,
    EnumParsingError,
)
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.shared_enums import person_characteristics
from recidiviz.common.date import (
    DateRange,
    NonNegativeDateRange,
    first_day_of_month,
    last_day_of_month,
)
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.common.str_field_utils import to_snake_case
from recidiviz.persistence.database.base_schema import JusticeCountsBase
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.utils import metadata
from recidiviz.utils.yaml_dict import YAMLDict

METRIC_VALUE_UPPER_BOUND = (
    400_000  # If metric value is greater than this value, an exception is raised
)
DimensionT = TypeVar("DimensionT", bound="Dimension")

# Dimensions

# TODO(#4472) Refactor all dimensions out to a common justice counts directory.


class Dimension:
    """Each dimension is represented as a class that is used to hold the values for that dimension and perform any
    necessary validation. All dimensions are categorical. Those with a pre-defined set of values are implemented as
    enums. Others are classes with a single text field to hold any value, and are potentially normalized to a
    pre-defined set of values as a separate dimension.
    """

    @classmethod
    @abstractmethod
    def get(
        cls: Type[DimensionT],
        dimension_cell_value: str,
        enum_overrides: Optional[EnumOverrides] = None,
    ) -> DimensionT:
        """Create an instance of the dimension based on the given value.

        Raises an error if it is unable to create an instance of a dimension. Only returns None if the value is
        explicitly ignored in `enum_overrides`.
        """

    @classmethod
    @abstractmethod
    def build_overrides(
        cls: Type[DimensionT], mapping_overrides: Dict[str, str]
    ) -> EnumOverrides:
        """
        Builds EnumOverrides for this Dimension, based on the provided mapping_overrides.
        Should raise an error if this Dimension is not normalized or if overrides are not supported.
        """

    @classmethod
    @abstractmethod
    def is_normalized(cls) -> bool:
        """
        Returns whether the dimensions cls is normalized
        """

    @classmethod
    @abstractmethod
    def dimension_identifier(cls) -> str:
        """The globally unique dimension_identifier of this dimension, used when storing it in the database.

        E.g. 'metric/population/type' or 'global/facility/raw'.
        """

    @classmethod
    @abstractmethod
    def get_generated_dimension_classes(cls) -> List[Type["Dimension"]]:
        """Returns a list of Dimensions that the current dimension will generate"""

    @classmethod
    @abstractmethod
    def generate_dimension_classes(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> List["Dimension"]:
        """Generates Dimensions based on the dimension cell value provided"""

    @property
    @abstractmethod
    def dimension_value(self) -> str:
        """The value of this dimension instance.

        E.g. 'FEMALE' is a potential value for an instance of the 'global/raw/gender' dimension.
        """


@attr.s(frozen=True)
class RawDimension(Dimension, metaclass=ABCMeta):
    """Base class to use to create a raw version of a normalized dimension.

    Child classes are typically created by passing a normalized dimension class to `raw_type_for_dimension`, which will
    create a raw, or not normalized, copy version of the dimension.
    """

    value: str = attr.ib(converter=str)

    @classmethod
    def get(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> "RawDimension":
        if enum_overrides is not None:
            raise ValueError(
                f"Unexpected enum_overrides when building raw dimension value: {enum_overrides}"
            )
        return cls(dimension_cell_value)

    @classmethod
    def build_overrides(cls, mapping_overrides: Dict[str, str]) -> EnumOverrides:
        raise ValueError("Can't raise override for RawDimension class")

    @classmethod
    def is_normalized(cls) -> bool:
        return False

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


def raw_for_dimension_cls(dimension_cls: Type[Dimension]) -> Type[Dimension]:
    return type(
        f"{dimension_cls.__name__}Raw",
        (RawDimension,),
        {
            "dimension_identifier": classmethod(
                lambda cls: "/".join([dimension_cls.dimension_identifier(), "raw"])
            )
        },
    )


def title_to_snake_case(title: str) -> str:
    return title.replace(" ", "_").lower()


def get_synthetic_dimension(column_name: str, source: str) -> Type[Dimension]:
    column_no_space = column_name.replace(" ", "")
    synthetic_dimension = type(
        f"{column_no_space}Raw",
        (RawDimension,),
        {
            "dimension_identifier": classmethod(
                lambda cls: "/".join(
                    [
                        "source",
                        title_to_snake_case(source),
                        title_to_snake_case(column_name),
                        "raw",
                    ]
                )
            )
        },
    )
    return synthetic_dimension


def parse_entity_enum(
    enum_cls: Type[EntityEnumT],
    dimension_cell_value: str,
    enum_overrides: Optional[EnumOverrides],
) -> EntityEnumT:
    entity_enum = enum_cls.parse(
        dimension_cell_value, enum_overrides or EnumOverrides.empty()
    )
    if entity_enum is None or not isinstance(entity_enum, enum_cls):
        raise ValueError(
            f"Attempting to parse '{dimension_cell_value}' as {enum_cls} returned unexpected "
            f"entity: {entity_enum}"
        )
    return entity_enum


def build_entity_overrides(
    enum_cls: Type[EntityEnumT], mapping_overrides: Dict[str, str]
) -> EnumOverrides:
    overrides_builder = EnumOverrides.Builder()
    for value, mapping in mapping_overrides.items():
        mapped = enum_cls(mapping)
        if mapped is None:
            raise ValueError(
                f"Unable to parse override value '{mapping}' as {enum_cls}"
            )
        overrides_builder.add(value, mapped)
    overrides = overrides_builder.build()
    return overrides


class PopulationType(Dimension, EntityEnum, metaclass=EntityEnumMeta):
    """
    Dimension that represents the type of populations
    """

    PRISON = "PRISON"
    SUPERVISION = "SUPERVISION"
    JAIL = "JAIL"
    OTHER = "OTHER"

    @classmethod
    def get(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> "PopulationType":
        return parse_entity_enum(cls, dimension_cell_value, enum_overrides)

    @classmethod
    def build_overrides(cls, mapping_overrides: Dict[str, str]) -> EnumOverrides:
        return build_entity_overrides(cls, mapping_overrides)

    @classmethod
    def is_normalized(cls) -> bool:
        return True

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/population/type"

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
    def _get_default_map(cls) -> Dict[str, "PopulationType"]:
        return {"PRISON": cls.PRISON, "SUPERVISION": cls.SUPERVISION, "JAIL": cls.JAIL}


class ReleaseType(Dimension, EntityEnum, metaclass=EntityEnumMeta):
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
    def get(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> "ReleaseType":
        return parse_entity_enum(cls, dimension_cell_value, enum_overrides)

    @classmethod
    def build_overrides(cls, mapping_overrides: Dict[str, str]) -> EnumOverrides:
        return build_entity_overrides(cls, mapping_overrides)

    @classmethod
    def is_normalized(cls) -> bool:
        return True

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/release/type"

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
    def _get_default_map(cls) -> Dict[str, "ReleaseType"]:
        return {
            "TO SUPERVISION": cls.TO_SUPERVISION,
            "COMPLETED": cls.COMPLETED,
            "OTHER": cls.OTHER,
        }


class AdmissionType(Dimension, EntityEnum, metaclass=EntityEnumMeta):
    """Dimension that represents the type of incarceration admission"""

    # Admissions due to a new sentence from the community.
    NEW_COMMITMENT = "NEW_COMMITMENT"

    # Admissions of persons from supervision (e.g. revocation, dunk, etc.)
    FROM_SUPERVISION = "FROM_SUPERVISION"

    # Any other admissions (e.g. in CT this is used for pre-trial admissions)
    OTHER = "OTHER"

    @classmethod
    def get(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> "AdmissionType":
        return parse_entity_enum(cls, dimension_cell_value, enum_overrides)

    @classmethod
    def build_overrides(cls, mapping_overrides: Dict[str, str]) -> EnumOverrides:
        return build_entity_overrides(cls, mapping_overrides)

    @classmethod
    def is_normalized(cls) -> bool:
        return True

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/admission/type"

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
    def _get_default_map(cls) -> Dict[str, "AdmissionType"]:
        return {
            "NEW COMMITMENT": cls.NEW_COMMITMENT,
            "FROM SUPERVISION": cls.FROM_SUPERVISION,
        }


class SupervisionViolationType(Dimension, EntityEnum, metaclass=EntityEnumMeta):
    """
    Dimension that represents the type of supervision violation
    """

    NEW_CRIME = "NEW_CRIME"
    TECHNICAL = "TECHNICAL"
    OTHER = "OTHER"

    @classmethod
    def get(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> "SupervisionViolationType":
        return parse_entity_enum(cls, dimension_cell_value, enum_overrides)

    @classmethod
    def build_overrides(cls, mapping_overrides: Dict[str, str]) -> EnumOverrides:
        return build_entity_overrides(cls, mapping_overrides)

    @classmethod
    def is_normalized(cls) -> bool:
        return True

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/supervision_violation/type"

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
    def _get_default_map(cls) -> Dict[str, "SupervisionViolationType"]:
        return {
            "NEW CRIME": cls.NEW_CRIME,
            "TECHNICAL": cls.TECHNICAL,
        }


class SupervisionType(Dimension, EntityEnum, metaclass=EntityEnumMeta):
    """Dimension that represents the type of supervision."""

    PAROLE = "PAROLE"
    PROBATION = "PROBATION"

    # Some jurisdictions have other types of supervision (e.g. DUI home confinement)
    OTHER = "OTHER"

    @classmethod
    def get(
        cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None
    ) -> "SupervisionType":
        return parse_entity_enum(cls, dimension_cell_value, enum_overrides)

    @classmethod
    def build_overrides(cls, mapping_overrides: Dict[str, str]) -> EnumOverrides:
        return build_entity_overrides(cls, mapping_overrides)

    @classmethod
    def is_normalized(cls) -> bool:
        return True

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/supervision/type"

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
    def _get_default_map(cls) -> Dict[str, "SupervisionType"]:
        return {
            "PAROLE": cls.PAROLE,
            "PROBATION": cls.PROBATION,
        }


def assert_no_overrides(
    dimension_cls: Type[Dimension], enum_overrides: Optional[EnumOverrides]
) -> None:
    if enum_overrides is not None:
        raise ValueError(
            f"Overrides not supported for {dimension_cls} but received {enum_overrides}"
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

    state_code: states.StateCode = attr.ib(converter=states.StateCode)

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


# TODO(#4473): Raise an error if there are conflicting dimension names
def parse_dimension_name(dimension_name: str) -> Type[Dimension]:
    """Parses a dimension name to its corresponding Dimension class."""
    for dimension in Dimension.__subclasses__():
        if not issubclass(dimension, Dimension):
            raise ValueError(f"Non-dimension subclass returned: {dimension}")
        if dimension_name == to_snake_case(dimension.__name__).upper():
            return dimension
    raise KeyError(f"No dimension exists for name: {dimension_name}")


# Ingest Models
# TODO(#4472): Pull these out into the ingest directory, alongside existing ingest_info.

# Properties are used within the models to handle the conversion specific to each object. This is advantageous so that
# if and when, for instance, a new metric needs to be added, it is clear what conversion steps must be implemented. Any
# conversion that is not specific to a particular object, but instead a class of objects, e.g. all Metrics, is instead
# implemented within the persistence code itself.
class DateFormatType(enum.Enum):
    DATE = "DATE"
    MONTH = "MONTH"
    YEAR = "YEAR"


DateFormatParserType = Callable[[str], datetime.date]


DATE_FORMAT_PARSERS: Dict[DateFormatType, DateFormatParserType] = {
    DateFormatType.DATE: datetime.date.fromisoformat,
    DateFormatType.MONTH: lambda text: datetime.datetime.strptime(text, "%Y-%m").date(),
    DateFormatType.YEAR: lambda text: datetime.datetime.strptime(text, "%Y").date(),
}


class MeasurementWindowType(enum.Enum):
    RANGE = "RANGE"
    SNAPSHOT = "SNAPSHOT"


# Currently we only expect one or two columns to be used to construct date ranges, but this can be expanded in the
# future if needed.
DateRangeConverterType = Union[
    Callable[[datetime.date], DateRange],
    Callable[[datetime.date, datetime.date], DateRange],
]


class RangeType(enum.Enum):
    @classmethod
    def get_or_default(cls, text: Optional[str]) -> "RangeType":
        if text is None:
            return RangeType.CUSTOM
        return cls(text)

    CUSTOM = "CUSTOM"
    MONTH = "MONTH"
    YEAR = "YEAR"


RANGE_CONVERTERS: Dict[RangeType, DateRangeConverterType] = {
    RangeType.CUSTOM: NonNegativeDateRange,
    RangeType.MONTH: NonNegativeDateRange.for_month_of_date,
    RangeType.YEAR: NonNegativeDateRange.for_year_of_date,
}

RANGE_CONVERTER_FORMAT_TYPES: Dict[RangeType, DateFormatType] = {
    RangeType.CUSTOM: DateFormatType.DATE,
    RangeType.MONTH: DateFormatType.MONTH,
    RangeType.YEAR: DateFormatType.YEAR,
}


class SnapshotType(enum.Enum):
    @classmethod
    def get_or_default(cls, text: Optional[str]) -> "SnapshotType":
        if text is None:
            return SnapshotType.DAY
        return cls(text)

    DAY = "DAY"
    FIRST_DAY_OF_MONTH = "FIRST_DAY_OF_MONTH"
    LAST_DAY_OF_MONTH = "LAST_DAY_OF_MONTH"


SNAPSHOT_CONVERTERS: Dict[SnapshotType, DateRangeConverterType] = {
    SnapshotType.DAY: NonNegativeDateRange.for_day,
    SnapshotType.FIRST_DAY_OF_MONTH: lambda date: NonNegativeDateRange.for_day(
        first_day_of_month(date)
    ),
    SnapshotType.LAST_DAY_OF_MONTH: lambda date: NonNegativeDateRange.for_day(
        last_day_of_month(date)
    ),
}

SNAPSHOT_CONVERTER_FORMAT_TYPES: Dict[SnapshotType, DateFormatType] = {
    SnapshotType.DAY: DateFormatType.DATE,
    SnapshotType.FIRST_DAY_OF_MONTH: DateFormatType.MONTH,
    SnapshotType.LAST_DAY_OF_MONTH: DateFormatType.MONTH,
}


class Metric:
    @property
    @abstractmethod
    def filters(self) -> List[Dimension]:
        """Any dimensions where the data only represents a subset of values for that dimension.

        For instance, a table for the population metric may only cover data for the prison population, not those on
        parole or probation. In that case filters would contain PopulationType.PRISON.
        """

    @property
    @abstractmethod
    def required_aggregated_dimensions(self) -> List[Type[Dimension]]:
        """
        Dimension types that are required to be aggregated to make this a valid metric
        """

    @abstractmethod
    def get_measurement_type(self) -> schema.MeasurementType:
        """How the metric over a given time window was reduced to a single point."""

    @classmethod
    @abstractmethod
    def get_metric_type(cls) -> schema.MetricType:
        """The metric type that this corresponds to in the schema."""


def _convert_optional(
    dimension_type: Type[EntityEnumT], value: Optional[str]
) -> Optional[EntityEnumT]:
    return None if value is None else dimension_type(value)


# `attr` requires converters to be named functions, so we create one for each type.


def _convert_optional_population_type(value: Optional[str]) -> Optional[PopulationType]:
    return _convert_optional(PopulationType, value)


def _convert_optional_release_type(value: Optional[str]) -> Optional[ReleaseType]:
    return _convert_optional(ReleaseType, value)


def _convert_optional_admission_type(value: Optional[str]) -> Optional[AdmissionType]:
    return _convert_optional(AdmissionType, value)


def _convert_optional_supervision_type(
    value: Optional[str],
) -> Optional[SupervisionType]:
    return _convert_optional(SupervisionType, value)


def _convert_optional_supervision_violation_type(
    value: Optional[str],
) -> Optional[SupervisionViolationType]:
    return _convert_optional(SupervisionViolationType, value)


@attr.s(frozen=True)
class Population(Metric):
    """Metric for recording populations.

    Currently this is only used for prison and supervision populations."""

    measurement_type: schema.MeasurementType = attr.ib(converter=schema.MeasurementType)

    population_type: Optional[PopulationType] = attr.ib(
        converter=_convert_optional_population_type, default=None
    )
    supervision_type: Optional[SupervisionType] = attr.ib(
        converter=_convert_optional_supervision_type, default=None
    )

    def __attrs_post_init__(self) -> None:
        if self.population_type != PopulationType.SUPERVISION:
            if self.supervision_type is not None:
                raise ValueError(
                    "'supervision_type' may only be set on population if 'population_type' is 'SUPERVISION'"
                )

    @property
    def filters(self) -> List[Dimension]:
        filters: List[Dimension] = []
        if self.population_type is not None:
            filters.append(self.population_type)
        if self.supervision_type is not None:
            filters.append(self.supervision_type)
        return filters

    @property
    def required_aggregated_dimensions(self) -> List[Type[Dimension]]:
        return [PopulationType] if self.population_type is None else []

    def get_measurement_type(self) -> schema.MeasurementType:
        return self.measurement_type

    @classmethod
    def get_metric_type(cls) -> schema.MetricType:
        return schema.MetricType.POPULATION


@attr.s(frozen=True)
class Releases(Metric):
    """
    Metric for recording releases.
    """

    measurement_type: schema.MeasurementType = attr.ib(converter=schema.MeasurementType)
    release_type: Optional[ReleaseType] = attr.ib(
        converter=_convert_optional_release_type, default=None
    )

    @property
    def filters(self) -> List[Dimension]:
        return [self.release_type] if self.release_type is not None else []

    @property
    def required_aggregated_dimensions(self) -> List[Type[Dimension]]:
        return []

    def get_measurement_type(self) -> schema.MeasurementType:
        return self.measurement_type

    @classmethod
    def get_metric_type(cls) -> schema.MetricType:
        return schema.MetricType.RELEASES


@attr.s(frozen=True)
class Admissions(Metric):
    """Metric for recording admissions.

    Currently this is intended to only be used for *incarceration* admissions. If needed in the future, it could be
    expanded to be used for other admissions as well.
    """

    measurement_type: schema.MeasurementType = attr.ib(converter=schema.MeasurementType)

    admission_type: Optional[AdmissionType] = attr.ib(
        converter=_convert_optional_admission_type, default=None
    )
    supervision_type: Optional[SupervisionType] = attr.ib(
        converter=_convert_optional_supervision_type, default=None
    )
    supervision_violation_type: Optional[SupervisionViolationType] = attr.ib(
        converter=_convert_optional_supervision_violation_type, default=None
    )

    def __attrs_post_init__(self) -> None:
        if self.admission_type != AdmissionType.FROM_SUPERVISION:
            if self.supervision_type is not None:
                raise ValueError(
                    "'supervision_type' may only be set on admissions if 'admission_type' is 'FROM_SUPERVISION'"
                )
            if self.supervision_violation_type is not None:
                raise ValueError(
                    "'supervision_violation_type' may only be set on admissions if 'admission_type' is "
                    "'FROM_SUPERVISION'"
                )

    def get_measurement_type(self) -> schema.MeasurementType:
        return self.measurement_type

    @classmethod
    def get_metric_type(cls) -> schema.MetricType:
        return schema.MetricType.ADMISSIONS

    @property
    def filters(self) -> List[Dimension]:
        filters: List[Dimension] = []
        if self.admission_type is not None:
            filters.append(self.admission_type)
        if self.supervision_type is not None:
            filters.append(self.supervision_type)
        if self.supervision_violation_type is not None:
            filters.append(self.supervision_violation_type)
        return filters

    @property
    def required_aggregated_dimensions(self) -> List[Type[Dimension]]:
        return []


# TODO(#7775): This can be merged with the Admissions metric if we allow for a dimension
# to be used multiple times on a single metric, so we can use SupervisionType for both
# the type of Supervision admitted to for supervsion starts, and type of supervsion
# admitted from for prison admissions.
@attr.s(frozen=True)
class SupervisionStarts(Metric):
    """Metric for recording supervision starts."""

    measurement_type: schema.MeasurementType = attr.ib(converter=schema.MeasurementType)

    supervision_type: Optional[SupervisionType] = attr.ib(
        converter=_convert_optional_supervision_type, default=None
    )

    def get_measurement_type(self) -> schema.MeasurementType:
        return self.measurement_type

    @classmethod
    def get_metric_type(cls) -> schema.MetricType:
        return schema.MetricType.SUPERVISION_STARTS

    @property
    def filters(self) -> List[Dimension]:
        filters: List[Dimension] = []
        if self.supervision_type is not None:
            filters.append(self.supervision_type)
        return filters

    @property
    def required_aggregated_dimensions(self) -> List[Type[Dimension]]:
        return [SupervisionType] if self.supervision_type is None else []


class DateRangeProducer:
    """Produces DateRanges for a given table, splitting the table as needed."""

    @abstractmethod
    def split_dataframe(
        self, df: pandas.DataFrame
    ) -> List[Tuple[DateRange, pandas.DataFrame]]:
        pass


@attr.s(frozen=True, kw_only=True)
class FixedDateRangeProducer(DateRangeProducer):
    """Used when data in the table is for a single date range, configured outside of the table."""

    # The date range for the table
    fixed_range: DateRange = attr.ib()

    def split_dataframe(
        self, df: pandas.DataFrame
    ) -> List[Tuple[DateRange, pandas.DataFrame]]:
        return [(self.fixed_range, df)]


@attr.s(frozen=True, kw_only=True)
class DynamicDateRangeProducer(DateRangeProducer):
    """Used when data in the table is for multiple date ranges, represented by the
    values of a particular set of columns in the table.
    """

    # The columns that contain the date ranges and how to parse the values in that column.
    # The parsed values are passed to `converter` in the same order in which the columns are specified in the dict.
    columns: Dict[str, DateFormatParserType] = attr.ib()
    # The function to use to convert the column values into date ranges
    converter: DateRangeConverterType = attr.ib()

    @property
    def column_names(self) -> List[str]:
        return list(self.columns.keys())

    @property
    def column_parsers(self) -> List[DateFormatParserType]:
        return list(self.columns.values())

    def split_dataframe(
        self, df: pandas.DataFrame
    ) -> List[Tuple[DateRange, pandas.DataFrame]]:
        # - Groups the df by the specified column, getting a separate df per date range
        # - Converts the column values to a `DateRange`, using the provided converter
        # - Drops the columns from the split dfs, as they are no longer needed
        return [
            (self._convert(date_args), split.drop(self.column_names, axis=1))
            for date_args, split in df.groupby(self.column_names)
        ]

    def _convert(self, args: Union[int, str, List[str]]) -> DateRange:
        unified_args: List[str] = [str(args)] if isinstance(args, (str, int)) else args
        parsed_args: List[datetime.date] = [
            parser(arg) for arg, parser in zip(unified_args, self.column_parsers)
        ]

        # pylint: disable=not-callable
        return self.converter(*parsed_args)


@attr.s(frozen=True)
class ColumnDimensionMapping:
    """Denotes that a particular dimension column can generate dimensions of a given type, with information about how
    to map values in that column to dimensions of this type.
    """

    # The class of the Dimension this column can generate.
    dimension_cls: Type[Dimension] = attr.ib()

    # Any enum overrides to use when converting to the dimension.
    overrides: Optional[EnumOverrides] = attr.ib()

    # If true, enum parsing will throw if a value in this column is not covered by enum overrides.
    strict: bool = attr.ib(default=True)

    @classmethod
    def from_input(
        cls,
        dimension_cls: Type[Dimension],
        mapping_overrides: Optional[Dict[str, str]] = None,
        strict: Optional[bool] = None,
    ) -> "ColumnDimensionMapping":
        overrides = None
        if mapping_overrides is not None:
            if not dimension_cls.is_normalized():
                raise ValueError(
                    f"Overrides can only be specified for normalized dimensions, not {dimension_cls}"
                )
            overrides = dimension_cls.build_overrides(mapping_overrides)
        return cls(dimension_cls, overrides, strict if strict is not None else True)

    @classmethod
    def for_synthetic(cls, column_name: str, source: str) -> "ColumnDimensionMapping":
        column_dimension = get_synthetic_dimension(column_name, source)
        return cls(column_dimension, overrides=None)

    def get_generated_dimension_classes(self) -> List[Type[Dimension]]:
        return self.dimension_cls.get_generated_dimension_classes()


def _validate_no_dimension_duplicates(dimensions: List["DimensionClassT"]) -> None:
    cleaned_generated_dimension = {
        dimension.dimension_identifier() for dimension in dimensions
    }
    if len(cleaned_generated_dimension) != len(dimensions):
        raise ValueError(f"There may be one or more duplicate dimensions: {dimensions}")


def _validate_generated_dimensions(
    dimension_cls: Type[Dimension], generated_dimensions: List[Dimension]
) -> None:
    _validate_no_dimension_duplicates(generated_dimensions)
    dimension_identifiers = [
        dimension.dimension_identifier() for dimension in generated_dimensions
    ]

    raw_dimension_name = "/".join([dimension_cls.dimension_identifier(), "raw"])
    if (
        dimension_cls.is_normalized()
        and raw_dimension_name not in dimension_identifiers
    ):
        raise ValueError(
            f"Synthetic dimension {dimension_cls} missing generated RawDimension"
        )


@attr.s(frozen=True)
class DimensionGenerator:
    """Generates dimensions and dimension values for a single column"""

    # The column name in the input file
    column_name: str = attr.ib()
    dimension_mappings: List[ColumnDimensionMapping] = attr.ib()

    def possible_dimensions_for_column(self) -> List[Type[Dimension]]:
        """Generates a list of all dimension classes that may be associated with a column."""
        output = []
        for mapping in self.dimension_mappings:
            output.append(mapping.dimension_cls)
            output.extend(mapping.get_generated_dimension_classes())
        return output

    def dimension_values_for_cell(self, dimension_cell_value: str) -> List[Dimension]:
        """Converts a single value in a dimension column to a list of dimension values."""
        dimension_values = []
        for mapping in self.dimension_mappings:

            try:
                base_dimension_value: Optional[Dimension] = mapping.dimension_cls.get(
                    dimension_cell_value, mapping.overrides
                )
            except EnumParsingError as e:
                if mapping.strict:
                    raise e
                base_dimension_value = None

            if base_dimension_value is not None:
                dimension_values.append(base_dimension_value)

            generated_dimension_classes = mapping.dimension_cls.generate_dimension_classes(  # type: ignore[arg-type]
                dimension_cell_value, mapping.overrides
            )
            _validate_generated_dimensions(
                mapping.dimension_cls, generated_dimension_classes
            )
            dimension_values.extend(generated_dimension_classes)

            if not dimension_values:
                raise ValueError(
                    f"Unable to parse '{dimension_cell_value}' as {mapping.dimension_cls}, but no raw "
                    f"dimension exists.'"
                )
        return dimension_values


# A single data point that has been annotated with a set of dimensions that it represents.
DimensionalDataPoint = Tuple[Tuple[Dimension, ...], decimal.Decimal]


def _dimension_generators_by_name(
    dimension_generators: List[DimensionGenerator],
) -> Dict[str, DimensionGenerator]:
    return {
        dimension_generator.column_name: dimension_generator
        for dimension_generator in dimension_generators
    }


DimensionClassT = TypeVar("DimensionClassT", Dimension, Type[Dimension])


@attr.s(frozen=True)
class TableConverter:
    """Maps all dimension column values to Dimensions in a table into dimensionally-annotated data points."""

    # For each dimension column, an object that can produce the list of possible dimension types in that column and
    # convert dimension cell values to those types.
    dimension_generators: Dict[str, DimensionGenerator] = attr.ib(
        converter=_dimension_generators_by_name
    )
    value_column: str = attr.ib()
    filters: Set[Dimension] = attr.ib()

    def _remove_filtered_dimensions_from_dimension_types_generated(
        self, generated_dimensions: List["DimensionClassT"]
    ) -> List["DimensionClassT"]:
        return [
            dimension
            for dimension in generated_dimensions
            if dimension.dimension_identifier()
            not in {
                filtered_dimension.dimension_identifier()
                for filtered_dimension in self.filters
            }
        ]

    def dimension_classes_for_columns(
        self, columns: List[str]
    ) -> List[Type[Dimension]]:
        """Returns a list of all possible dimensions that a value in a table could have (superset of possible dimensions
        from individual columns)."""
        dimensions: List[Type[Dimension]] = []

        generators_to_use = dict(self.dimension_generators)
        for column in columns:
            if column in generators_to_use:
                possible_dimensions = generators_to_use.pop(
                    column
                ).possible_dimensions_for_column()
                dimensions.extend(
                    self._remove_filtered_dimensions_from_dimension_types_generated(
                        possible_dimensions
                    )
                )
            elif column != self.value_column:
                if column in self.dimension_generators:
                    raise ValueError(
                        f"Column '{column}' appeared multiple times in the data."
                    )
                raise ValueError(f"Column '{column}' was not mapped.")

        if generators_to_use:
            raise ValueError(
                f"Columns [{', '.join(generators_to_use.keys())}] are mapped but do not appear in the data."
            )
        return dimensions

    def table_to_data_points(self, df: pandas.DataFrame) -> List[DimensionalDataPoint]:
        data_points = []
        for row_idx in df.index:
            row = df.loc[row_idx]

            dimension_values_list: List[Dimension] = []
            for column_name in row.index:
                if column_name in self.dimension_generators:
                    dimension_values_list.extend(
                        self._dimension_values_for_dimension_cell(
                            column_name, str(row[column_name])
                        )
                    )
            dimension_values = tuple(dimension_values_list)

            # Pandas might've inferred our data as strings, floats, or ints. This handles converting all of them to
            # Decimal. We could instead do this up front, and tell `read_csv` which dtypes to use for each column.
            raw_value = row[self.value_column]
            if isinstance(raw_value, str):
                raw_value = raw_value.replace(",", "")
            else:
                raw_value = raw_value.item()

            value = decimal.Decimal(raw_value)
            data_points.append((dimension_values, value))
        return data_points

    def _dimension_values_for_dimension_cell(
        self, dimension_column_name: str, dimension_value: str
    ) -> Iterable[Dimension]:
        return self._remove_filtered_dimensions_from_dimension_types_generated(
            self.dimension_generators[dimension_column_name].dimension_values_for_cell(
                dimension_value
            )
        )


HasIdentifierT = TypeVar("HasIdentifierT", Dimension, Type[Dimension])


def _sort_dimensions(dimensions: Iterable[HasIdentifierT]) -> List[HasIdentifierT]:
    return sorted(dimensions, key=lambda dimension: dimension.dimension_identifier())


def _is_raw_dimension(dimension: HasIdentifierT) -> bool:
    return dimension.dimension_identifier().endswith("/raw")


@attr.s(frozen=True)
class Table:
    """Ingest model that represents a table in a report"""

    date_range: DateRange = attr.ib()
    metric: Metric = attr.ib()
    system: schema.System = attr.ib(converter=schema.System)
    label: Optional[str] = attr.ib()
    filename: Optional[str] = attr.ib()
    methodology: str = attr.ib()

    # These are dimensions that apply to all data points in this table
    location: Optional[Location] = attr.ib()
    table_filters: List[Dimension] = attr.ib()

    # The superset of all possible dimension classes that may be associated with a row in this table.
    dimensions: List[Type[Dimension]] = attr.ib()

    # Each row in `data_points` may contain a subset of the dimensions in `dimensions`.
    data_points: List[DimensionalDataPoint] = attr.ib()

    @data_points.validator
    def _values_are_valid(
        self, _attribute: attr.Attribute, data_points: List[DimensionalDataPoint]
    ) -> None:
        for dimension_values, value in data_points:
            if not value.is_finite():
                raise ValueError(
                    f"Invalid value '{value}' for row with dimensions: {dimension_values}"
                )
            if value < 0:
                raise ValueError(
                    f"Negative value '{value}' for row with dimensions: {dimension_values}"
                )
            if value > METRIC_VALUE_UPPER_BOUND:
                raise ValueError(
                    f"Invalid value '{value}' for row with dimensions: {dimension_values} is too large."
                )

    @data_points.validator
    def _rows_dimension_combinations_are_unique(
        self, _attribute: attr.Attribute, data_points: List[DimensionalDataPoint]
    ) -> None:
        row_dimension_values = set()
        for dimension_values, _value in data_points:
            if dimension_values in row_dimension_values:
                raise ValueError(
                    f"Multiple rows in table with identical dimensions: {dimension_values}"
                )
            row_dimension_values.add(dimension_values)

    def _validate_metric(self) -> None:
        for dimension_metric in self.metric.required_aggregated_dimensions:
            if dimension_metric not in self.dimensions:
                raise AttributeError(
                    f"metric and dimension column not specified for {dimension_metric}, "
                    "make sure you have one or the other."
                )
        for dimension in self.metric.filters:
            if type(dimension) in self.dimensions:
                raise AttributeError(
                    f"metric and dimension column specified for {type(dimension)},  "
                    "make sure you have one or the other."
                )

    def _validate_unique_dimensions(self) -> None:
        """
        Validate that unique dimensions have more than one value.
        Does not validate raw dimensions since it is okay for raw dimensions to only have one value.
        """
        unique_dimension_values_per_identifier: Dict[str, typing.Counter[str]] = {
            dimension.dimension_identifier(): Counter()
            for dimension in self.dimensions
            if not _is_raw_dimension(dimension)
        }

        for dimensions, _value in self.data_points:
            for dimension in dimensions:
                if not _is_raw_dimension(dimension):
                    unique_dimension_values_per_identifier[
                        dimension.dimension_identifier()
                    ].update([dimension.dimension_value])
        for (
            _key,
            unique_dimension_values,
        ) in unique_dimension_values_per_identifier.items():
            if len(unique_dimension_values) == 1:
                [unique_dimension, occurrences] = unique_dimension_values.popitem()
                if occurrences == len(self.data_points):
                    raise AttributeError(
                        f"Attribute '{unique_dimension}' only has one set value, "
                        f"change it to a filtered dimension."
                    )

    def _validate_dimension_either_filter_or_aggregate(self) -> None:
        filter_identifiers = [
            dimension.dimension_identifier() for dimension in self.filters
        ]
        dimension_identifiers = [
            dimension.dimension_identifier() for dimension in self.dimensions
        ]

        duplicate_dimensions = set(filter_identifiers) & set(dimension_identifiers)

        if duplicate_dimensions:
            raise ValueError(
                f"These dimensions are both in the dimension column and the filtered dimension: "
                f"{duplicate_dimensions}"
            )
        if (
            State.dimension_identifier() not in dimension_identifiers
            and State.dimension_identifier() not in filter_identifiers
        ):
            raise ValueError(
                "Location not specified and state dimension not in either filtered dimension or "
                "dimension columns"
            )

    def __attrs_post_init__(self) -> None:
        # Validate consistency between `dimensions` and `data`.
        self._validate_metric()
        identifiers = [
            dimension.dimension_identifier() for dimension in self.dimensions
        ]
        duplicates = [item for item, count in Counter(identifiers).items() if count > 1]
        if duplicates:
            raise ValueError(f"Duplicate dimensions in table: {duplicates}")
        self._validate_unique_dimensions()
        _validate_no_dimension_duplicates(self.filters)
        self._validate_dimension_either_filter_or_aggregate()
        for dimensions, _value in self.data_points:
            row_dimension_identifiers = {
                dimension_value.dimension_identifier() for dimension_value in dimensions
            }
            if len(row_dimension_identifiers) != len(dimensions):
                raise ValueError(f"Duplicate dimensions in row: {dimensions}")
            if not set(identifiers).issuperset(row_dimension_identifiers):
                raise ValueError(
                    f"Row has dimensions not defined for table. Row dimensions: "
                    f"'{row_dimension_identifiers}', table dimensions: '{identifiers}'"
                )

    @classmethod
    def from_table(
        cls,
        date_range: DateRange,
        table_converter: TableConverter,
        label: Optional[str],
        filename: Optional[str],
        metric: Metric,
        system: str,
        methodology: str,
        location: Optional[Location],
        additional_filters: List[Dimension],
        df: pandas.DataFrame,
    ) -> "Table":
        return cls(
            date_range=date_range,
            metric=metric,
            system=system,
            label=label,
            filename=filename,
            methodology=methodology,
            dimensions=table_converter.dimension_classes_for_columns(df.columns.values),
            data_points=table_converter.table_to_data_points(df),
            location=location,
            table_filters=additional_filters,
        )

    @classmethod
    def list_from_dataframe(
        cls,
        date_range_producer: DateRangeProducer,
        table_converter: TableConverter,
        metric: Metric,
        system: str,
        label: Optional[str],
        filename: Optional[str],
        methodology: str,
        location: Optional[Location],
        additional_filters: List[Dimension],
        df: pandas.DataFrame,
    ) -> List["Table"]:
        """
        Returns list of tables split across date_range dataframes
        """
        tables = []
        for date_range, df_date in date_range_producer.split_dataframe(df):
            tables.append(
                cls.from_table(
                    date_range=date_range,
                    table_converter=table_converter,
                    metric=metric,
                    system=system,
                    label=label,
                    filename=filename,
                    methodology=methodology,
                    location=location,
                    additional_filters=additional_filters,
                    df=df_date,
                )
            )
        return tables

    @property
    def filters(self) -> List[Dimension]:
        filters = self.metric.filters + self.table_filters
        if self.location is not None:
            filters.append(self.location)
        return _sort_dimensions(filters)

    @property
    def filtered_dimension_names(self) -> List[str]:
        return [filter.dimension_identifier() for filter in self.filters]

    @property
    def filtered_dimension_values(self) -> List[str]:
        return [filter.dimension_value for filter in self.filters]

    @property
    def aggregated_dimensions(self) -> List[Type[Dimension]]:
        return _sort_dimensions(self.dimensions)

    @property
    def aggregated_dimension_names(self) -> List[str]:
        return [
            dimension.dimension_identifier() for dimension in self.aggregated_dimensions
        ]

    @property
    def cells(self) -> List[Tuple[List[Optional[str]], decimal.Decimal]]:
        """Returns all of the cells in this table."""
        table_dimensions = self.aggregated_dimensions

        results = []
        for row in self.data_points:
            cell_dimension_values: List[Optional[str]] = []

            # Align the row dimension values with the table dimensions, filling in with None for any dimension that the
            # row does not have.
            row_dimension_values = _sort_dimensions(row[0])
            row_dimension_iter = peekable(row_dimension_values)
            for table_dimension in table_dimensions:
                if (
                    table_dimension.dimension_identifier()
                    == row_dimension_iter.peek().dimension_identifier()
                ):
                    cell_dimension_values.append(
                        next(row_dimension_iter).dimension_value
                    )
                else:
                    cell_dimension_values.append(None)

            try:
                next(row_dimension_iter)
                raise ValueError(
                    f"Dimensions for cell not aligned with table. Table dimensions: "
                    f"'{self.aggregated_dimensions}', row dimensions: '{row_dimension_values}'"
                )
            except StopIteration:
                pass

            results.append((cell_dimension_values, row[1]))

        return results


def _convert_publish_date(value: Optional[str]) -> datetime.date:
    return datetime.date.fromisoformat(value) if value else datetime.date.today()


@attr.s(frozen=True)
class Report:
    """Ingest model that represents a report"""

    # Name of the website or organization that published the report, e.g. 'Mississippi Department of Corrections'
    source_name: str = attr.ib()
    # Distinguishes between the many types of reports that a single source may produce, e.g. 'Daily Status Report' or
    # 'Monthly Fact Sheet'
    report_type: str = attr.ib()
    # Identifies a specific instance of a report type, and should be unique within report type and source, e.g. 'August
    # 2020' for the August Monthly Fact Sheet.
    report_instance: str = attr.ib()

    tables: List[Table] = attr.ib()

    # The date the report was published, used to identify updated reports.
    publish_date: datetime.date = attr.ib(converter=_convert_publish_date)

    # The URL for the report on the source's website
    url: parse.ParseResult = attr.ib(converter=parse.urlparse)

    # Parsing Layer
    # TODO(#4480): Pull this out to somewhere within ingest

    @tables.validator
    def _sums_across_tables_match(
        self, _attribute: attr.Attribute, tables: List[Table]
    ) -> None:
        """Raises error if sums across dimesions do not match among tables."""
        # sums dictionary is {table attributes (metric, dimension, date, filters) --> (table names, expected total)}
        # sums keeps track of a single table name that is the source of the expected total, any table names that have
        # sums that don't match are added to the value tuple,
        sums: Dict[
            Tuple[
                Metric,
                Type[Dimension],
                datetime.date,
                datetime.date,
                str,
            ],
            Tuple[List[str], decimal.Decimal],
        ] = {}
        for table in tables:
            table_sum = decimal.Decimal(
                sum([data_point[1] for data_point in table.data_points])
            )
            for dimension in table.dimensions:
                table_filters_hashable = [
                    (f.dimension_identifier(), f.dimension_value) for f in table.filters
                ]
                key = (
                    table.metric,
                    dimension,
                    table.date_range.lower_bound_inclusive_date,
                    table.date_range.upper_bound_exclusive_date,
                    str(table_filters_hashable),
                )
                table_name_key = table.filename or "<Table name not found>"
                if key in sums and table_sum != sums[key][1]:
                    # If sum of the current table does not match another table with the same date,
                    # one of the tables has an incorrect value
                    sums[key][0].append(table_name_key)
                if key not in sums:
                    sums[key] = (
                        [table_name_key],
                        table_sum,
                    )

        invalid_table_groups = [
            table_names for table_names, sum in sums.values() if len(table_names) > 1
        ]
        if len(invalid_table_groups) > 0:
            raise ValueError(
                f"Sums across dimensions do not match for the following table group(s) {str(invalid_table_groups)}."
            )


def _parse_location(location_input: Optional[YAMLDict]) -> Optional[Location]:
    """Expects a dict with a single entry, e.g. `{'state': 'US_XX'}`"""
    if location_input is None:
        return None

    if len(location_input) == 1:
        [location_type] = location_input.get().keys()
        location_name = location_input.pop(location_type, str)
        if location_type == "country":
            return Country(location_name)
        if location_type == "state":
            return State(location_name)
        if location_type == "county":
            return County(location_name)
    raise ValueError(
        f"Invalid location, expected a dictionary with a single key that is one of ('country', 'state', "
        f"'county') but received: {repr(location_input)}"
    )


def _get_converter(
    range_type_input: str, range_converter_input: Optional[str] = None
) -> DateRangeConverterType:
    range_type = MeasurementWindowType(range_type_input)
    if range_type is MeasurementWindowType.SNAPSHOT:
        return SNAPSHOT_CONVERTERS[SnapshotType.get_or_default(range_converter_input)]
    if range_type is MeasurementWindowType.RANGE:
        return RANGE_CONVERTERS[RangeType.get_or_default(range_converter_input)]
    raise ValueError(f"Enum case not handled for {range_type} when building converter.")


def _get_date_formatter(
    range_type_input: str, range_converter_input: Optional[str] = None
) -> DateFormatParserType:
    measurement_window_type = MeasurementWindowType(range_type_input)
    if measurement_window_type is MeasurementWindowType.SNAPSHOT:
        snapshot_type = SnapshotType.get_or_default(range_converter_input)
        format_type = SNAPSHOT_CONVERTER_FORMAT_TYPES[snapshot_type]
        return DATE_FORMAT_PARSERS[format_type]
    if measurement_window_type is MeasurementWindowType.RANGE:
        range_type = RangeType.get_or_default(range_converter_input)
        format_type = RANGE_CONVERTER_FORMAT_TYPES[range_type]
        return DATE_FORMAT_PARSERS[format_type]
    raise ValueError(
        f"Enum case not handled for {measurement_window_type} when getting date formatter."
    )


# TODO(#4480): Generalize these parsing methods, instead of creating one for each class. If value is a dict, pop it,
# find all implementing classes of `key`, find matching class, pass inner dict as parameters to matching class.
def _parse_date_range(range_input: YAMLDict) -> DateRange:
    """
    Expects a dict with a type, an input, and an optional range_converter.
    e.g. `{'type':'snapshot', 'input': ['2020-11-01'], 'range_converter': 'DATE'}`
    """

    range_args = range_input.pop("input", list)
    range_type = range_input.pop("type", str)
    range_converter = range_input.pop_optional("converter", str)
    date_formatter = _get_date_formatter(range_type.upper(), range_converter)

    if len(range_input) > 0:
        raise ValueError(
            f"Received unexpected parameters for date_range: {range_input}"
        )
    if len(range_args) > 2:
        raise ValueError(
            f"Have a maximum of 2 dates for input. Currently have: {range_args}"
        )

    converter = _get_converter(range_type.upper(), range_converter)
    parsed_args = [date_formatter(value) for value in range_args]

    return converter(*parsed_args)  # type: ignore[call-arg]


def _parse_dynamic_date_range_producer(
    range_input: YAMLDict,
) -> DynamicDateRangeProducer:
    """Expects a dict with type (str), columns (dict) and converter (str, optional) entries.

    E.g. `{'type': 'SNAPSHOT' {'columns': 'Date': 'DATE'}}`
    """
    range_type = range_input.pop("type", str)
    range_converter = range_input.pop_optional("converter", str)
    column_names = range_input.pop("columns", dict)
    columns = {
        key: DATE_FORMAT_PARSERS[DateFormatType(value)]
        for key, value in column_names.items()
    }

    if len(range_input) > 0:
        raise ValueError(
            f"Received unexpected parameters for date range: {repr(range_input)}"
        )

    return DynamicDateRangeProducer(
        converter=_get_converter(range_type, range_converter), columns=columns
    )


def _parse_date_range_producer(range_producer_input: YAMLDict) -> DateRangeProducer:
    """Expects a dict with a single entry that is the arguments for the producer, e.g. `{'fixed': ...}`"""
    if len(range_producer_input) == 1:
        [range_producer_type] = range_producer_input.get().keys()
        range_producer_args = range_producer_input.pop_dict(range_producer_type)
        if range_producer_type == "fixed":
            return FixedDateRangeProducer(
                fixed_range=_parse_date_range(range_producer_args)
            )
        if range_producer_type == "dynamic":
            return _parse_dynamic_date_range_producer(range_producer_args)
    raise ValueError(
        f"Invalid date range, expected a dictionary with a single key that is one of ('fixed', 'dynamic'"
        f") but received: {repr(range_producer_input)}"
    )


def _parse_dimensions_from_additional_filters(
    additional_filters_input: Optional[YAMLDict],
) -> List[Dimension]:
    if additional_filters_input is None:
        return []
    dimensions = []
    for dimension_name in list(additional_filters_input.get().keys()):
        dimension_cls = parse_dimension_name(dimension_name)
        value = additional_filters_input.pop(dimension_name, str)
        dimension_value = dimension_cls.get(value)
        if dimension_value is None:
            raise ValueError(
                f"Unable to parse filter value '{value}' as {dimension_cls}"
            )
        dimensions.append(dimension_value)
    return dimensions


def _parse_table_converter(
    source_name: str,
    value_column_input: YAMLDict,
    dimension_columns_input: Optional[List[YAMLDict]],
    location: Optional[Location],
    additional_filters: List[Dimension],
) -> TableConverter:
    """Expects a dict with the value column name and, optionally, a list of dicts describing the dimension columns.

    E.g. `value_column_input={'column_name': 'Population'}}
          dimension_columns_input=[{'column_name': 'Race', 'dimension_name': 'Race', 'mapping_overrides': {...}}, ...]`
    """
    column_dimension_mappings: Dict[str, List[ColumnDimensionMapping]] = defaultdict(
        list
    )
    if dimension_columns_input is not None:
        for dimension_column_input in dimension_columns_input:
            column_name = dimension_column_input.pop("column_name", str)

            synthetic_column = dimension_column_input.pop_optional("synthetic", bool)
            if synthetic_column is True:
                column_dimension_mappings[column_name].append(
                    ColumnDimensionMapping.for_synthetic(
                        column_name=column_name, source=source_name
                    )
                )
                continue

            dimension_cls = parse_dimension_name(
                dimension_column_input.pop("dimension_name", str)
            )

            overrides_input = dimension_column_input.pop_dict_optional(
                "mapping_overrides"
            )
            overrides = None
            if overrides_input is not None:
                overrides = {
                    key: overrides_input.pop(key, str)
                    for key in list(overrides_input.get().keys())
                }

            strict = dimension_column_input.pop_optional("strict", bool)

            if len(dimension_column_input) > 0:
                raise ValueError(
                    f"Received unexpected input for dimension column: {repr(dimension_column_input)}"
                )
            column_dimension_mappings[column_name].append(
                ColumnDimensionMapping.from_input(
                    dimension_cls=dimension_cls,
                    mapping_overrides=overrides,
                    strict=strict,
                )
            )

    dimension_generators = []
    for column_name, mappings in column_dimension_mappings.items():
        dimension_generators.append(
            DimensionGenerator(column_name=column_name, dimension_mappings=mappings)
        )
    value_column = value_column_input.pop("column_name", str)

    filters: Set[Dimension] = set(additional_filters)
    if location is not None:
        filters.add(location)

    if len(value_column_input) > 0:
        raise ValueError(
            f"Received unexpected parameters for value column: {repr(value_column_input)}"
        )

    return TableConverter(
        dimension_generators=dimension_generators,
        value_column=value_column,
        filters=filters,
    )


def _parse_metric(metric_input: YAMLDict) -> Metric:
    """Expects a dict with a single entry that is the arguments for the metric, e.g. `{'population': ...}`"""
    if len(metric_input) == 1:
        [metric_type] = metric_input.get().keys()
        metric_args = metric_input.pop(metric_type, dict)
        if metric_type == "population":
            return Population(**metric_args)
        if metric_type == "admissions":
            return Admissions(**metric_args)
        if metric_type == "releases":
            return Releases(**metric_args)
        if metric_type == "supervision_starts":
            return SupervisionStarts(**metric_args)
    raise ValueError(
        f"Invalid metric, expected a dictionary with a single key that is one of ('admissions', "
        f"'population', 'releases', 'supervision_starts') but received: {repr(metric_input)}"
    )


def _normalize(name: str) -> str:
    return name.replace("/", "_")


def csv_filename(sheet_name: str, worksheet_name: str) -> str:
    return f"{_normalize(sheet_name)} - {_normalize(worksheet_name)}.csv"


def _get_table_filename(
    spreadsheet_name: str, name: Optional[str], file: Optional[str]
) -> str:
    if name is not None:
        return csv_filename(spreadsheet_name, name)
    if file is not None:
        return file
    raise ValueError("Did not receive name parameter for table")


# Only three layers of dictionary nesting is currently supported by the table parsing logic but we use the recursive
# dictionary type for convenience.


def _parse_tables(
    gcs: GCSFileSystem,
    manifest_path: GcsfsFilePath,
    source_name: str,
    tables_input: List[YAMLDict],
) -> List[Table]:
    """Parses the YAML list of dictionaries describing tables into Table objects"""
    directory_path = GcsfsDirectoryPath.from_file_path(manifest_path)

    # We are assuming that the spreadsheet and the yaml file have the same name
    spreadsheet_name = manifest_path.file_name.replace(".yaml", "")
    tables = []
    for table_input in tables_input:
        # Parse nested objects separately
        date_range_producer = _parse_date_range_producer(
            table_input.pop_dict("date_range")
        )
        location_dimension: Optional[Location] = _parse_location(
            table_input.pop_dict_optional("location")
        )
        filter_dimensions = _parse_dimensions_from_additional_filters(
            table_input.pop_dict_optional("additional_filters")
        )
        table_converter = _parse_table_converter(
            source_name,
            table_input.pop_dict("value_column"),
            table_input.pop_dicts_optional("dimension_columns"),
            location=location_dimension,
            additional_filters=filter_dimensions,
        )
        metric = _parse_metric(table_input.pop_dict("metric"))

        table_name = table_input.pop_optional("name", str)
        table_filename = table_input.pop_optional("file", str)
        try:
            table_handle = open_table_file(
                gcs, directory_path, spreadsheet_name, table_name, table_filename
            )
        except ValueError as e:
            # Much of the manually collected data uses a single spreadsheet with all of the data for a state, even if
            # it is from multiple sources. To support that we first look for a table named with the data source, e.g.
            # 'AL_A', but otherwise we fall back to the generic version, e.g. 'AL_Data'.
            try:
                spreadsheet_name = f"{spreadsheet_name.split('_')[0]}_Data"
                table_handle = open_table_file(
                    gcs,
                    directory_path,
                    spreadsheet_name,
                    table_name,
                    table_filename,
                )
            except BaseException:
                # Raise the original error.
                raise e from e
        with table_handle.open() as table_file:
            name = _get_table_filename(
                spreadsheet_name, name=table_name, file=table_filename
            )
            file_extension = os.path.splitext(name)[1]
            if file_extension == ".csv":
                df = pandas.read_csv(table_file)
            elif file_extension == ".tsv":
                df = pandas.read_table(table_file)
            else:
                raise ValueError(f"Received unexpected file extension: {name}")

        tables.extend(
            Table.list_from_dataframe(
                date_range_producer=date_range_producer,
                table_converter=table_converter,
                metric=metric,
                system=table_input.pop("system", str),
                label=table_input.pop_optional("label", str),
                filename=name,
                methodology=table_input.pop("methodology", str),
                location=location_dimension,
                additional_filters=filter_dimensions,
                df=df,
            )
        )

        if len(table_input) > 0:
            raise ValueError(f"Received unexpected parameters for table: {table_input}")

    return tables


def open_table_file(
    gcs: GCSFileSystem,
    directory_path: GcsfsDirectoryPath,
    spreadsheet_name: str,
    table_name: Optional[str],
    table_file: Optional[str],
) -> LocalFileContentsHandle:
    table_path = GcsfsFilePath.from_directory_and_file_name(
        directory_path,
        _get_table_filename(spreadsheet_name, name=table_name, file=table_file),
    )
    table_handle = gcs.download_to_temp_file(table_path)
    if table_handle is None:
        raise ValueError(f"Unable to download table from path: {table_path}")
    logging.info("Reading table: %s", table_path)
    return table_handle


def _get_report_and_acquirer(
    gcs: GCSFileSystem, manifest_path: GcsfsFilePath
) -> Tuple[Report, str]:
    logging.info("Reading report manifest: %s", manifest_path)
    manifest_handle = gcs.download_to_temp_file(manifest_path)
    if manifest_handle is None:
        raise ValueError(f"Unable to download manifest from path: {manifest_path}")

    manifest = YAMLDict.from_path(manifest_handle.local_file_path)
    source_name = manifest.pop("source", str)
    # Parse tables separately
    # TODO(#4479): Also allow for location to be a column in the csv, as is done for dates.
    tables = _parse_tables(
        gcs, manifest_path, source_name, manifest.pop_dicts("tables")
    )

    report = Report(
        source_name=source_name,
        report_type=manifest.pop("report_type", str),
        report_instance=manifest.pop("report_instance", str),
        publish_date=manifest.pop("publish_date", str),
        url=manifest.pop("url", str),
        tables=tables,
    )
    acquirer = manifest.pop("assignee", str)

    if len(manifest) > 0:
        raise ValueError(f"Received unexpected parameters in manifest: {manifest}")

    return report, acquirer


# Persistence Layer
# TODO(#4478): Refactor this into the persistence layer (including splitting out conversion, validation)


@attr.s(frozen=True)
class Metadata:
    acquisition_method: schema.AcquisitionMethod = attr.ib()
    acquired_by: str = attr.ib()


def _get_existing_entity(
    ingested_entity: schema.JusticeCountsDatabaseEntity, session: Session
) -> Optional[JusticeCountsBase]:
    table = ingested_entity.__table__
    [unique_constraint] = [
        constraint
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    ]
    query = session.query(table)
    for column in unique_constraint:
        # TODO(#4477): Instead of making an assumption about how the property name is formed from the column name, use
        # an Entity method here to follow the foreign key relationship.
        if column.name.endswith("_id"):
            value = getattr(ingested_entity, column.name[: -len("_id")]).id
        else:
            value = getattr(ingested_entity, column.name)
        # Cast to the type because array types aren't deduced properly.
        query = query.filter(column == cast(value, column.type))
    table_entity: Optional[JusticeCountsBase] = query.first()
    return table_entity


def _update_existing_or_create(
    ingested_entity: schema.JusticeCountsDatabaseEntity, session: Session
) -> schema.JusticeCountsDatabaseEntity:
    # Note: Using on_conflict_do_update to resolve whether there is an existing entity could be more efficient as it
    # wouldn't incur multiple roundtrips. However for some entities we need to know whether there is an existing entity
    # (e.g. table instance) so we can clear child entities, so we probably wouldn't win much if anything.
    table_entity = _get_existing_entity(ingested_entity, session)
    if table_entity is not None:
        # TODO(#4477): Instead of assuming the primary key field is named `id`, use an Entity method.
        ingested_entity.id = table_entity.id
        # TODO(#4477): Merging here doesn't seem perfect, although it should work so long as the given entity always has
        # all the properties set explicitly. To avoid the merge, the method could instead take in the entity class as
        # one parameter and the parameters to construct it separately and then query based on those parameters. However
        # this would likely make mypy less useful.
        merged_entity = session.merge(ingested_entity)
        return merged_entity
    session.add(ingested_entity)
    return ingested_entity


def _delete_existing_and_create(
    session: Session,
    ingested_entity: schema.JusticeCountsDatabaseEntity,
    entity_cls: Type[schema.JusticeCountsDatabaseEntity],
) -> schema.JusticeCountsDatabaseEntity:
    table_entity = _get_existing_entity(ingested_entity, session)
    if table_entity is not None:
        table = ingested_entity.__table__
        # TODO(#4477): need to have a better way to identify id since below method doesn't guarantee id is a valid attr
        delete_q = table.delete().where(entity_cls.id == table_entity.id)  # type: ignore[attr-defined]
        session.execute(delete_q)
    session.add(ingested_entity)
    return ingested_entity


def _convert_entities(
    session: Session, ingested_report: Report, report_metadata: Metadata
) -> None:
    """Convert the ingested report into SQLAlchemy models"""
    report = schema.Report(
        source=_update_existing_or_create(
            schema.Source(name=ingested_report.source_name), session
        ),
        type=ingested_report.report_type,
        instance=ingested_report.report_instance,
        publish_date=ingested_report.publish_date,
        url=ingested_report.url.geturl(),
        acquisition_method=report_metadata.acquisition_method,
        acquired_by=report_metadata.acquired_by,
        project=schema.Project.JUSTICE_COUNTS_DATA_SCAN,
    )
    # Does not delete associated report_table_definitions of report_table_instances,
    # which may in certain cases leave orphaned report_table_definition_rows
    _delete_existing_and_create(session, report, schema.Report)

    for table in ingested_report.tables:
        table_definition = _update_existing_or_create(
            schema.ReportTableDefinition(
                system=table.system,
                metric_type=table.metric.get_metric_type(),
                measurement_type=table.metric.get_measurement_type(),
                filtered_dimensions=table.filtered_dimension_names,
                filtered_dimension_values=table.filtered_dimension_values,
                aggregated_dimensions=table.aggregated_dimension_names,
                label=table.label or "",
            ),
            session,
        )

        table_instance = schema.ReportTableInstance(
            report=report,
            report_table_definition=table_definition,
            time_window_start=table.date_range.lower_bound_inclusive_date,
            time_window_end=table.date_range.upper_bound_exclusive_date,
            methodology=table.methodology,
        )

        table_instance.cells = [
            schema.Cell(
                report_table_instance=table_instance,
                aggregated_dimension_values=dimensions,
                value=value,
            )
            for dimensions, value in table.cells
        ]

        session.add(table_instance)


def _persist_report(report: Report, report_metadata: Metadata) -> None:
    with SessionFactory.using_database(
        SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS)
    ) as session:
        _convert_entities(session, report, report_metadata)
        # TODO(#4475): Add sanity check validation of the data provided, either here or as part of objects above. E.g.:
        # - If there is only one value for a dimension in a table it should be a filter not an aggregated dimension
        # - Ensure the measurement type is valid with the window type
        # - Sanity check custom date ranges
        # Validation of dimension values should already be enforced by enums above.


def ingest(gcs: GCSFileSystem, manifest_filepath: GcsfsFilePath) -> Set[str]:
    """
    ingests manifest file locally and returns the table names that were ingested
    """
    logging.info("Fetching report for ingest...")
    report, acquirer = _get_report_and_acquirer(gcs, manifest_filepath)
    logging.info("Ingesting report...")
    _persist_report(
        report,
        Metadata(
            acquisition_method=schema.AcquisitionMethod.MANUALLY_ENTERED,
            acquired_by=acquirer,
        ),
    )
    logging.info("Report ingested.")

    ingested_file_names: Set[str] = set()
    for table in report.tables:
        if table.filename:
            ingested_file_names.add(table.filename)

    return ingested_file_names


# TODO(#4127): Everything above should be refactored out of the tools directory so only the script below is left.


def _create_parser() -> argparse.ArgumentParser:
    """Creates the CLI argument parser."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--manifest-file",
        required=True,
        type=str,
        help="The yaml describing how to ingest the data",
    )
    parser.add_argument(
        "--project-id",
        required=True,
        type=str,
        help="The GCP project to ingest the data into",
    )
    parser.add_argument(
        "--app-url", required=False, type=str, help="Override the url of the app."
    )
    parser.add_argument(
        "--log",
        required=False,
        default="INFO",
        type=logging.getLevelName,
        help="Set the logging level",
    )
    return parser


def upload(gcs: GCSFileSystem, manifest_path: str) -> GcsfsFilePath:
    """Uploads the manifest and any referenced tables to GCS."""
    directory, manifest_filename = os.path.split(manifest_path)
    manifest = YAMLDict.from_path(manifest_path)

    gcs_directory = GcsfsDirectoryPath.from_absolute_path(
        os.path.join(
            f"gs://{metadata.project_id()}-justice-counts-ingest",
            manifest.pop("source", str),
            manifest.pop("report_type", str),
            manifest.pop("report_instance", str),
        )
    )

    for table in manifest.pop_dicts("tables"):
        spreadsheet_name = manifest_filename[: -len(".yaml")]
        table_name = table.pop_optional("name", str)
        table_filename = table.pop_optional("file", str)
        try:
            upload_table(
                gcs,
                directory,
                gcs_directory,
                spreadsheet_name,
                table_name,
                table_filename,
            )
        except FileNotFoundError as e:
            # Much of the manually collected data uses a single spreadsheet with all of the data for a state, even if
            # it is from multiple sources. To support that we first look for a table named with the data source, e.g.
            # 'AL_A', but otherwise we fall back to the generic version, e.g. 'AL_Data'.
            try:
                spreadsheet_prefix = spreadsheet_name.split("_")[0]
                upload_table(
                    gcs,
                    directory,
                    gcs_directory,
                    f"{spreadsheet_prefix}_Data",
                    table_name,
                    table_filename,
                )
            except BaseException:
                # Raise the original error.
                raise e from e

    manifest_gcs_path = GcsfsFilePath.from_directory_and_file_name(
        gcs_directory, os.path.basename(manifest_path)
    )
    gcs.upload_from_contents_handle_stream(
        path=manifest_gcs_path,
        contents_handle=LocalFileContentsHandle(manifest_path, cleanup_file=False),
        content_type="text/yaml",
    )
    return manifest_gcs_path


def upload_table(
    gcs: GCSFileSystem,
    directory: str,
    gcs_directory: GcsfsDirectoryPath,
    spreadsheet_name: str,
    table_name: Optional[str],
    table_file: Optional[str],
) -> None:
    table_filename = _get_table_filename(
        spreadsheet_name, name=table_name, file=table_file
    )
    gcs.upload_from_contents_handle_stream(
        path=GcsfsFilePath.from_directory_and_file_name(gcs_directory, table_filename),
        contents_handle=LocalFileContentsHandle(
            os.path.join(directory, table_filename), cleanup_file=False
        ),
        content_type="text/csv",
    )


def trigger_ingest(gcs_path: GcsfsFilePath, app_url: Optional[str]) -> None:
    """
    Triggers ingest by checking to see if service account credentials environment variable is set and using it,
    or by triggering ingest via a web browser.
    """
    app_url = app_url or f"https://{metadata.project_id()}.appspot.com"
    url = f"{app_url}/justice_counts/ingest?{parse.urlencode({'manifest_path': gcs_path.uri()})}"

    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        client_id = IAP_CLIENT_ID[metadata.project_id()]
        response = make_iap_request(url, client_id)

        # May return 200 status code with an error in the response
        if response.text != "":
            raise ValueError(response.text)
    else:
        logging.info("Opening browser to trigger ingest...")
        webbrowser.open(url=url)
        # Ask the user if the browser request was successful or displayed an error.
        i = input("Was the ingest successful? [Y/n]: ")
        if i and i.strip().lower() != "y":
            raise ValueError("Ingest failed")


def main(manifest_path: str, app_url: Optional[str]) -> None:
    logging.info("Uploading report for ingest...")
    gcs_path = upload(GcsfsFactory.build(), manifest_path)

    # We can't hit the endpoint on the app directly from the python script as we don't have IAP credentials. Instead we
    # launch the browser to hit the app and allow the user to auth in browser.

    trigger_ingest(gcs_path, app_url)

    logging.info("Report ingested.")


def _configure_logging(level: str) -> None:
    root = logging.getLogger()
    root.setLevel(level)


if __name__ == "__main__":
    arg_parser = _create_parser()
    arguments = arg_parser.parse_args()

    _configure_logging(arguments.log)

    with metadata.local_project_id_override(arguments.project_id):
        main(arguments.manifest_file, arguments.app_url)
