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
"""A class that collects top-level BigQueryViewBuilder instances in a directory and uses them to build BigQueryViews."""
import abc
import os
import re
from types import ModuleType
from typing import Callable, Generic, List, Optional, Set, Type

import recidiviz
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    BigQueryViewBuilderType,
)
from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.utils.types import assert_type

VIEW_BUILDER_EXPECTED_NAME = "VIEW_BUILDER"


class BigQueryViewCollector(Generic[BigQueryViewBuilderType], ModuleCollectorMixin):
    """A class that collects top-level BigQueryViewBuilder instances in a directory.

    In order to be discovered, the view builder instances must have the name defined in VIEW_BUILDER_EXPECTED_NAME.
    """

    @abc.abstractmethod
    def collect_view_builders(self) -> List[BigQueryViewBuilderType]:
        """Returns a list of view builders of the appropriate type. Should be implemented by subclasses."""

    @classmethod
    def collect_view_builders_in_dir(
        cls,
        *,
        builder_type: Type[BigQueryViewBuilderType],
        relative_dir_path: str,
        recurse: bool = False,
        view_file_prefix_filter: Optional[str] = None,
        view_builder_attribute_name_regex: str = VIEW_BUILDER_EXPECTED_NAME,
        expect_builders_in_all_files: bool = True,
    ) -> List[BigQueryViewBuilderType]:
        """Collects all view builders in a directory relative to the recidiviz base directory and returns a list of all
        views that can be built from builders defined in files in that directory.

        Args:
            builder_type: The type of builder that we expect to find in this subdir
            relative_dir_path: The relative path to search in (e.g. 'calculator/query/state/views/admissions').
            recurse: If true, look inside subdirectories of relative_dir_path for view files.
            view_file_prefix_filter: When set, collection filters out any files whose name does not have this prefix.
            view_builder_attribute_name_regex: Regex that matches the name of view
                builder top-level variables in the module. Must be an exact match.
            expect_builders_in_all_files: If True, throws if a view builder does not
                exist in all discovered python files. Otherwise, just skips files with
                no matching builder.
        """
        sub_module_parts = os.path.normpath(relative_dir_path).split("/")
        view_dir_module = cls.get_relative_module(recidiviz, sub_module_parts)
        return cls.collect_view_builders_in_module(
            builder_type=builder_type,
            view_dir_module=view_dir_module,
            recurse=recurse,
            view_file_prefix_filter=view_file_prefix_filter,
            view_builder_attribute_name_regex=view_builder_attribute_name_regex,
            expect_builders_in_all_files=expect_builders_in_all_files,
        )

    @classmethod
    def collect_view_builders_in_module(
        cls,
        *,
        builder_type: Type[BigQueryViewBuilderType],
        view_dir_module: ModuleType,
        recurse: bool = False,
        view_file_prefix_filter: Optional[str] = None,
        view_builder_attribute_name_regex: str = VIEW_BUILDER_EXPECTED_NAME,
        validate_builder_fn: Optional[
            Callable[[BigQueryViewBuilderType, ModuleType], None]
        ] = None,
        expect_builders_in_all_files: bool = True,
    ) -> List[BigQueryViewBuilderType]:
        """Collects all view builders in a directory module and returns a list of all
        views that can be built from builders defined in files in that directory.

        Args:
            builder_type: The type of builder that we expect to find in this subdir
            view_dir_module: The module for the directory that contains all the view
                definition files.
            recurse: If true, look inside submodules of view_dir_module for view files.
            view_file_prefix_filter: When set, collection filters out any files whose
                name does not have this prefix.
            validate_builder_fn: When set, this function will be called with each
                builder and the module that builder was defined in. Implementations of
                this function should throw if the builder does not meet validation
                conditions. This can be used to enforce agreement between builder
                filenames and the builder view names, for example.
            view_builder_attribute_name_regex: Regex that matches the name of view
                builder top-level variables in the module. Must be an exact match.
            expect_builders_in_all_files: If True, throws if a view builder does not
                exist in all discovered python files. Otherwise, just skips files with
                no matching builder.
        """

        view_dir_modules = [view_dir_module]
        builders: Set[BigQueryViewBuilderType] = set()
        while view_dir_modules:
            view_dir_module = view_dir_modules.pop(0)
            child_modules = cls.get_submodules(
                view_dir_module, submodule_name_prefix_filter=None
            )
            for child_module in child_modules:
                if cls.is_module_package(child_module):
                    if recurse:
                        view_dir_modules.append(child_module)
                    continue

                view_file_name = os.path.basename(
                    assert_type(child_module.__file__, str)
                )
                if view_file_prefix_filter and not view_file_name.startswith(
                    view_file_prefix_filter
                ):
                    continue

                builder_variable_names = [
                    attribute
                    for attribute in dir(child_module)
                    if re.fullmatch(view_builder_attribute_name_regex, attribute)
                ]

                if expect_builders_in_all_files and not builder_variable_names:
                    raise ValueError(
                        f"File [{child_module.__file__}] has no top-level attribute matching "
                        f"[{view_builder_attribute_name_regex}]"
                    )

                for builder_name in builder_variable_names:
                    builder = getattr(child_module, builder_name)
                    if not isinstance(builder, builder_type):
                        raise ValueError(
                            f"Unexpected type [{builder.__class__.__name__}] for attribute "
                            f"[{builder_name}] in file [{child_module.__file__}]. Expected "
                            f"type [{builder_type.__name__}]."
                        )

                    if validate_builder_fn:
                        validate_builder_fn(builder, child_module)
                    builders.add(builder)

        def get_address(builder: BigQueryViewBuilderType) -> BigQueryAddress:
            return builder.address

        return sorted(builders, key=get_address)


def filename_matches_view_id_validator(
    builder: BigQueryViewBuilder,
    view_module: ModuleType,
) -> None:
    """A validator function that checks that the filename of the given view module
    matches the view_id of the view builder defined inside that file. Can be passed
    to the validate_builder_fn argument on
    BigQueryViewCollector.collect_view_builders_in_module when we want to enforce that
    view filename and view_ids match.
    """
    if not view_module.__file__:
        raise ValueError(f"Found no file for module [{view_module}]")
    filename = os.path.splitext(os.path.basename(view_module.__file__))[0]
    expected_filename = builder.address.table_id
    if filename != expected_filename:
        raise ValueError(
            f"Found view builder [{builder.address}] defined in file with "
            f"name that does not match the view_id: {filename}. Expected filename: "
            f"{expected_filename}."
        )
