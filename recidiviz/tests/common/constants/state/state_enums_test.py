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
"""General tests for state schema enums."""
import importlib
import inspect
import os
import pkgutil
import sys
import unittest
from typing import List, Type

import recidiviz.common.constants.state as state_constants
from recidiviz.common.constants.entity_enum import EntityEnum
from recidiviz.common.str_field_utils import normalize

STATE_CONSTANTS_MODULE_DIR = os.path.dirname(state_constants.__file__)


class StateEnumsTest(unittest.TestCase):
    """General tests for state schema enums."""

    @staticmethod
    def _get_all_state_enum_classes() -> List[Type[EntityEnum]]:
        enum_classes = []
        packages = pkgutil.walk_packages([STATE_CONSTANTS_MODULE_DIR])
        for package in packages:
            if package.name.startswith("state"):

                module_name = f"{state_constants.__name__}.{package.name}"

                importlib.import_module(module_name)
                all_members_in_current_module = inspect.getmembers(
                    sys.modules[module_name], inspect.isclass
                )
                for _, member in all_members_in_current_module:
                    if member.__module__ == module_name and issubclass(
                        member, EntityEnum
                    ):
                        enum_classes.append(member)

        return enum_classes

    def test_state_enum_default_overrides_have_correct_types(self):
        enum_classes = self._get_all_state_enum_classes()
        self.assertTrue(len(enum_classes) > 0)
        for entity_enum_cls in enum_classes:
            # pylint: disable=protected-access
            default_enum_mappings = entity_enum_cls._get_default_map()

            for enum_val in default_enum_mappings.values():
                if enum_val is not None:
                    self.assertIsInstance(enum_val, entity_enum_cls)

    def test_all_enum_values_covered_in_default_overrides(self):
        enum_classes = self._get_all_state_enum_classes()
        self.assertTrue(len(enum_classes) > 0)
        for entity_enum_cls in enum_classes:
            # pylint: disable=protected-access
            default_enum_mappings = entity_enum_cls._get_default_map()

            for entity_enum in entity_enum_cls:
                normalized_value = normalize(entity_enum.value, remove_punctuation=True)
                self.assertIn(
                    normalized_value,
                    default_enum_mappings,
                    f"[{normalized_value}] not found in "
                    f"{entity_enum_cls} default mappings.",
                )

                self.assertEqual(default_enum_mappings[normalized_value], entity_enum)
