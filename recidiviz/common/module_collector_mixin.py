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
"""A mixin class that helps with traversing Python modules / directory trees."""

import importlib
import os
import pkgutil
import re
from types import ModuleType
from typing import Callable, List, Optional, Set, Type

from recidiviz.utils.types import T, assert_type


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
            try:
                rel_module = cls.get_relative_module(base_module, [submodule_name])
                submodules.append(rel_module)
            except ModuleNotFoundError:
                continue

        return submodules

    @classmethod
    def is_module_package(cls, module: ModuleType) -> bool:
        file = assert_type(module.__file__, str)
        return file.endswith("__init__.py")

    @classmethod
    def collect_top_level_attributes_in_module(
        cls,
        *,
        attribute_type: Type[T],
        dir_module: ModuleType,
        attribute_name_regex: str,
        recurse: bool = False,
        file_prefix_filter: Optional[str] = None,
        validate_fn: Optional[Callable[[T, ModuleType], None]] = None,
        expect_match_in_all_files: bool = True,
    ) -> List[T]:
        """Collects and returns a list of all attributes with the correct type /
         specifications defined in files in a given directory. If the collection
         encounters a matching list attribute, it will look inside the list to collect
         the attributes of the correct type.

        Args:
            attribute_type: The type of attribute that we expect to find in this subdir
            dir_module: The module for the directory that contains all the files
                where attributes are defined.
            recurse: If true, look inside submodules of dir_module for files.
            file_prefix_filter: When set, collection filters returns only attributes
                defined in files whose names match the provided prefix.
            validate_fn: When set, this function will be called with each
                found attribute and the module that attribute was defined in. Implementations of
                this function should throw if the attribute does not meet validation
                conditions. This can be used to enforce agreement between
                filenames and the attribute characteristics (e.g. a name field).
            attribute_name_regex: Regex that matches the name of attribute
                top-level variables in the module. Must be an exact match.
            expect_match_in_all_files: If True, throws if a matching attribute does not
                exist in all discovered python files. Otherwise, just skips files with
                no matching attribute.
        """

        dir_modules = [dir_module]
        found_attributes: Set[T] = set()
        while dir_modules:
            dir_module = dir_modules.pop(0)
            child_modules = cls.get_submodules(
                dir_module, submodule_name_prefix_filter=None
            )
            for child_module in child_modules:
                if cls.is_module_package(child_module):
                    if recurse:
                        dir_modules.append(child_module)
                    continue

                file_name = os.path.basename(assert_type(child_module.__file__, str))
                if file_prefix_filter and not file_name.startswith(file_prefix_filter):
                    continue

                attribute_variable_names = [
                    attribute
                    for attribute in dir(child_module)
                    if re.fullmatch(attribute_name_regex, attribute)
                ]

                if expect_match_in_all_files and not attribute_variable_names:
                    raise ValueError(
                        f"File [{child_module.__file__}] has no top-level attribute matching "
                        f"[{attribute_name_regex}]"
                    )

                found_attributes_in_module: List[T] = []
                for attribute_name in attribute_variable_names:
                    attribute = getattr(child_module, attribute_name)
                    if isinstance(attribute, list):
                        if not attribute:
                            raise ValueError(
                                f"Unexpected empty list for attribute [{attribute_name}]"
                                f"in file [{child_module.__file__}]."
                            )
                        if not isinstance(attribute[0], attribute_type):
                            raise ValueError(
                                f"Unexpected type [List[{attribute[0].__class__.__name__}]] for attribute "
                                f"[{attribute_name}] in file [{child_module.__file__}]. Expected "
                                f"type [List[{attribute_type.__name__}]]."
                            )
                        found_attributes_in_module += attribute
                    else:
                        if not isinstance(attribute, attribute_type):
                            raise ValueError(
                                f"Unexpected type [{attribute.__class__.__name__}] for attribute "
                                f"[{attribute_name}] in file [{child_module.__file__}]. Expected "
                                f"type [{attribute_type.__name__}]."
                            )

                        found_attributes_in_module.append(attribute)

                for val in found_attributes_in_module:
                    if validate_fn:
                        validate_fn(val, child_module)
                    found_attributes.add(val)

        return list(found_attributes)
