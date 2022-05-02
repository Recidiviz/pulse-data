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
"""Contains helper utilities for working with Dimension classes."""


import inspect
from typing import Dict, Optional, Type

from recidiviz.common.constants.entity_enum import EntityEnumT
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.common.str_field_utils import to_snake_case
from recidiviz.justice_counts import dimensions
from recidiviz.justice_counts.dimensions.base import Dimension, RawDimension


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


def assert_no_overrides(
    dimension_cls: Type[Dimension], enum_overrides: Optional[EnumOverrides]
) -> None:
    if enum_overrides is not None:
        raise ValueError(
            f"Overrides not supported for {dimension_cls} but received {enum_overrides}"
        )


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
                        _title_to_snake_case(source),
                        _title_to_snake_case(column_name),
                        "raw",
                    ]
                )
            )
        },
    )
    return synthetic_dimension


class DimensionParser(ModuleCollectorMixin):
    def __init__(self) -> None:
        self.names_to_dimensions: Dict[str, Type[Dimension]] = {}

        dimension_modules = ModuleCollectorMixin.get_submodules(
            base_module=dimensions, submodule_name_prefix_filter=None
        )
        dimension_classes = set()
        for module in dimension_modules:
            for attribute_name in dir(module):
                attribute = getattr(module, attribute_name)
                if inspect.isclass(attribute) and issubclass(attribute, Dimension):
                    dimension_classes.add(attribute)

        for dimension in dimension_classes:
            name = to_snake_case(dimension.__name__).upper()
            if name in self.names_to_dimensions:
                raise ValueError(
                    f"Multiple dimensions with name '{name}': {self.names_to_dimensions[name]}, {dimension}"
                )
            self.names_to_dimensions[name] = dimension

    def get_dimension_for_name(self, dimension_name: str) -> Type[Dimension]:
        """Gets the corresponding Dimension class for `dimension_name`."""
        dimension = self.names_to_dimensions.get(dimension_name)
        if dimension is None:
            raise KeyError(f"No dimension exists for name: {dimension_name}")
        return dimension


def _title_to_snake_case(title: str) -> str:
    return title.replace(" ", "_").lower()
