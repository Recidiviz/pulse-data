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

from typing import Dict, List, Optional, Type

from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.justice_counts.dimensions.base import Dimension
from recidiviz.justice_counts.dimensions.helpers import (
    build_entity_overrides,
    parse_entity_enum,
    raw_for_dimension_cls,
)


class PopulationType(Dimension, EntityEnum, metaclass=EntityEnumMeta):
    """
    Dimension that represents the type of populations
    """

    RESIDENTS = "RESIDENTS"
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
