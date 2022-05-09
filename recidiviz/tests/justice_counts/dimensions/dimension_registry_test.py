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
"""Tests for registry containing all official Justice Counts dimensions."""

import unittest
from inspect import getmembers, isclass

from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.justice_counts import dimensions
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.dimensions.dimension_registry import (
    DIMENSIONS as registered_dimensions,
)


class TestJusticeCountsDimensionRegistry(unittest.TestCase):
    def test_all_dimensions_in_dimension_registry(self) -> None:
        dimension_modules = ModuleCollectorMixin.get_submodules(
            dimensions, submodule_name_prefix_filter=None
        )

        dimension_dict = {}
        for module in dimension_modules:
            for type_str, obj in getmembers(module):
                if isclass(obj) and issubclass(obj, DimensionBase):
                    if type_str not in {"Dimension", "DimensionBase", "RawDimension"}:
                        dimension_dict[obj.dimension_identifier()] = obj

        registered_dimensions_dict = {
            obj.dimension_identifier(): obj for obj in registered_dimensions
        }
        self.assertEqual(dimension_dict, registered_dimensions_dict)
