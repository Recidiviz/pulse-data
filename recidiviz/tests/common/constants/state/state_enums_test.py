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
import os
import unittest
from typing import List, Type

import recidiviz.common.constants.state as state_constants
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum
from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.common.str_field_utils import normalize
from recidiviz.persistence.entity.entity_utils import get_all_enum_classes_in_module

STATE_CONSTANTS_MODULE_DIR = os.path.dirname(state_constants.__file__)


class StateEnumsTest(unittest.TestCase):
    """General tests for state schema enums."""

    @staticmethod
    def _get_all_state_enum_classes() -> List[Type[StateEntityEnum]]:
        enum_file_modules = ModuleCollectorMixin.get_submodules(
            state_constants, submodule_name_prefix_filter=None
        )
        result = []
        for enum_file_module in enum_file_modules:
            enum_classes = get_all_enum_classes_in_module(enum_file_module)
            for enum_cls in enum_classes:
                if not issubclass(enum_cls, StateEntityEnum):
                    raise ValueError(f"Unexpected class type: {enum_cls}")
                result.append(enum_cls)
        return result

    def test_state_enum_default_overrides_have_correct_types(self) -> None:
        enum_classes = self._get_all_state_enum_classes()
        self.assertTrue(len(enum_classes) > 0)
        for entity_enum_cls in enum_classes:
            default_enum_mappings = getattr(entity_enum_cls, "_get_default_map")()

            for enum_val in default_enum_mappings.values():
                if enum_val is not None:
                    self.assertIsInstance(enum_val, entity_enum_cls)

    def test_all_enum_values_covered_in_default_overrides(self) -> None:
        enum_classes = self._get_all_state_enum_classes()
        self.assertTrue(len(enum_classes) > 0)
        for entity_enum_cls in enum_classes:
            default_enum_mappings = getattr(entity_enum_cls, "_get_default_map")()

            for entity_enum in entity_enum_cls:
                normalized_value = normalize(entity_enum.value, remove_punctuation=True)
                self.assertIn(
                    normalized_value,
                    default_enum_mappings,
                    f"[{normalized_value}] not found in "
                    f"{entity_enum_cls} default mappings.",
                )

                self.assertEqual(default_enum_mappings[normalized_value], entity_enum)
