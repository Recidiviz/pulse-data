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
from types import FunctionType, ModuleType
from typing import Any, Callable, List, Optional, Type

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
    def get_module_attribute_as_typed_list(
        cls,
        module: ModuleType,
        attribute_name: str,
        attribute: Any,
        expected_attribute_type: Type[T],
    ) -> List[T]:
        """Given an |attribute| and an |attribute_type|, this function will verify that
        the |attribute| is either of |attribute_type| or a list of |attribute_type|
        """
        if isinstance(attribute, list):
            if not attribute:
                raise ValueError(
                    f"Unexpected empty list for attribute [{attribute_name}] "
                    f"in file [{module.__file__}]."
                )
            if not all(
                isinstance(attribute_elem, expected_attribute_type)
                for attribute_elem in attribute
            ):
                raise ValueError(
                    f"An attribute in List [{attribute_name}] in file [{module.__file__}] "
                    f"did not match expected type [{expected_attribute_type.__name__}]."
                )
            return attribute

        if not isinstance(attribute, expected_attribute_type):
            raise ValueError(
                f"Unexpected type [{attribute.__class__.__name__}] for attribute "
                f"[{attribute_name}] in file [{module.__file__}]. Expected "
                f"type [{expected_attribute_type.__name__}]."
            )

        return [attribute]

    @classmethod
    def collect_from_callable(
        cls,
        module: ModuleType,
        attribute_name: str,
        expected_attribute_type: Type[T],
    ) -> List[T]:
        """Given a |callable_name|, invoke the callable and validate the returned
        objects as having the correct type.
        """
        attribute = getattr(module, attribute_name)

        if not isinstance(attribute, FunctionType):
            raise ValueError(
                f"Unexpected type [{type(attribute)}] for [{attribute_name}] in "
                f"[{module.__file__}]. Expected type [Callable]"
            )

        try:
            collected_objs = attribute()
        except TypeError as e:
            raise ValueError(
                f"Unexpected parameters to callable [{attribute_name}] in "
                f"[{module.__name__}]. Expected no paramaeters."
            ) from e

        return cls.get_module_attribute_as_typed_list(
            module, attribute_name, collected_objs, expected_attribute_type
        )

    @classmethod
    def collect_top_level_attributes_in_module(
        cls,
        *,
        attribute_type: Type[T],
        dir_module: ModuleType,
        attribute_name_regex: str,
        recurse: bool = False,
        collect_from_callables: bool = False,
        callable_name_regex: str = "",
        file_prefix_filter: Optional[str] = None,
        validate_fn: Optional[Callable[[T, ModuleType], None]] = None,
        expect_match_in_all_files: bool = True,
        deduplicate_found_attributes: bool = True,
    ) -> List[T]:
        """Collects and returns a list of all attributes with the correct type /
         specifications defined in files in a given directory. If the collection
         encounters a matching list attribute, it will look inside the list to collect
         the attributes of the correct type. If the collection encounters a matching
         callable without any parameters and a proper return type, it will envoke that
         callable and return

        Args:
            attribute_type: The type of attribute that we expect to find in this subdir
            dir_module: The module for the directory that contains all the files
                where attributes are defined.
            recurse: If true, look inside submodules of dir_module for files.
            collect_from_callables: If True, will also find callables that match the
                |callable_regex|, collect all return objects, validate them as
                valid |attribute_type|s and include them in the returned list.
            callable_regex: When |collect_from_callables| is True, this regex will be
                used to determine which module attributes are callables.
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
            deduplicate_found_attributes: If True, all found attributes will be
                deduplicated. This can only be True if the attribute_type is a hashable
                type. If false, found attributes are added to a list without checking
                for duplicates.
        Returns:
            List of |attribute_type|
        """
        if collect_from_callables and not callable_name_regex:
            raise ValueError(
                "You must specify a callable_regex if you are trying to collect callabes"
            )

        found_attributes: set[T] | list[T] = (
            set() if deduplicate_found_attributes else []
        )
        dir_modules = [dir_module]
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

                callable_var_names: List[str] = []

                if collect_from_callables:
                    callable_var_names = [
                        attribute
                        for attribute in dir(child_module)
                        if re.fullmatch(callable_name_regex, attribute)
                    ]

                if expect_match_in_all_files and not (
                    attribute_variable_names
                    or (collect_from_callables and callable_var_names)
                ):
                    raise ValueError(
                        f"File [{child_module.__file__}] has no top-level attribute matching "
                        f"[{attribute_name_regex}]"
                    )

                found_attributes_in_module: List[T] = []
                for attribute_name in attribute_variable_names:
                    attribute = getattr(child_module, attribute_name)
                    found_attributes_in_module.extend(
                        cls.get_module_attribute_as_typed_list(
                            child_module,
                            attribute_name,
                            attribute,
                            attribute_type,
                        )
                    )

                if collect_from_callables:
                    for attribute_name in callable_var_names:
                        found_attributes_in_module.extend(
                            cls.collect_from_callable(
                                child_module,
                                attribute_name,
                                attribute_type,
                            )
                        )

                for val in found_attributes_in_module:
                    if validate_fn:
                        validate_fn(val, child_module)

                    if isinstance(found_attributes, list):
                        found_attributes.append(val)
                    elif isinstance(found_attributes, set):
                        found_attributes.add(val)
                    else:
                        raise ValueError(
                            f"Unexpected type for found_attributes: "
                            f"[{type(found_attributes)}]"
                        )

        return list(found_attributes)
