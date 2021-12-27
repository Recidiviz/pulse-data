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
"""A mixin class that helps with traversing Python modules / directory trees."""

import importlib
import os
import pkgutil
from types import ModuleType
from typing import List, Optional


class ModuleCollectorMixin:
    """A mixin class that helps with traversing Python modules / directory trees."""

    @staticmethod
    def get_relative_module(
        base_module: ModuleType, sub_module_parts: List[str]
    ) -> ModuleType:
        module = base_module
        for part in sub_module_parts:
            module = importlib.import_module(f"{module.__name__}.{part}")
        return module

    @staticmethod
    def _get_submodule_names(
        base_module: ModuleType, submodule_name_prefix_filter: Optional[str]
    ) -> List[str]:
        if base_module.__file__ is None:
            raise ValueError(f"No file associated with {base_module}.")
        base_module_path = os.path.dirname(base_module.__file__)
        return [
            name
            for _, name, _ in pkgutil.iter_modules([base_module_path])
            if not submodule_name_prefix_filter
            or name.startswith(submodule_name_prefix_filter)
        ]

    @classmethod
    def get_submodules(
        cls, base_module: ModuleType, submodule_name_prefix_filter: Optional[str]
    ) -> List[ModuleType]:
        submodules = []
        for submodule_name in cls._get_submodule_names(
            base_module, submodule_name_prefix_filter
        ):
            submodules.append(cls.get_relative_module(base_module, [submodule_name]))

        return submodules
