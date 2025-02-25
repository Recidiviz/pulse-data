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
from types import ModuleType
from typing import Callable, Generic, List, Optional, Type

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    BigQueryViewBuilderType,
)
from recidiviz.common.module_collector_mixin import ModuleCollectorMixin

VIEW_BUILDER_EXPECTED_NAME = "VIEW_BUILDER"
VIEW_BUILDER_CALLABLE_NAME_REGEX = "collect_.*view_builder"


class BigQueryViewCollector(Generic[BigQueryViewBuilderType], ModuleCollectorMixin):
    """A class that collects top-level BigQueryViewBuilder instances in a directory.

    In order to be discovered, the view builder instances must have the name defined in VIEW_BUILDER_EXPECTED_NAME.
    """

    @abc.abstractmethod
    def collect_view_builders(self) -> List[BigQueryViewBuilderType]:
        """Returns a list of view builders of the appropriate type. Should be implemented by subclasses."""

    @classmethod
    def collect_view_builders_in_module(
        cls,
        *,
        builder_type: Type[BigQueryViewBuilderType],
        view_dir_module: ModuleType,
        recurse: bool = False,
        collect_builders_from_callables: bool = False,
        builder_callable_name_regex: str = VIEW_BUILDER_CALLABLE_NAME_REGEX,
        view_file_prefix_filter: Optional[str] = None,
        view_builder_attribute_name_regex: str = VIEW_BUILDER_EXPECTED_NAME,
        validate_builder_fn: Optional[
            Callable[[BigQueryViewBuilderType, ModuleType], None]
        ] = None,
        expect_builders_in_all_files: bool = True,
    ) -> List[BigQueryViewBuilderType]:
        """Collects all view builders defined as top-level variables in the provided
        module.

        Args:
            builder_type: The type of builder that we expect to find in this subdir
            view_dir_module: The module for the directory that contains all the view
                definition files.
            recurse: If true, look inside submodules of view_dir_module for view files.
            collect_builders_from_callables: If True, will also find callables that match
                the |builder_callable_regex|, collect all return objects, validate them as
                valid |attribute_type|s and include them in the returned list.
            builder_callable_regex: If |collect_builders_from_callables| is set, this
                regex will be used to identify module attributes to invoke and include
                the resulting objects in the returned list.
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

        unsorted_builders = cls.collect_top_level_attributes_in_module(
            attribute_type=builder_type,
            dir_module=view_dir_module,
            recurse=recurse,
            collect_from_callables=collect_builders_from_callables,
            callable_name_regex=builder_callable_name_regex,
            file_prefix_filter=view_file_prefix_filter,
            attribute_name_regex=view_builder_attribute_name_regex,
            validate_fn=validate_builder_fn,
            expect_match_in_all_files=expect_builders_in_all_files,
        )

        def get_address(builder: BigQueryViewBuilderType) -> BigQueryAddress:
            return builder.address

        return sorted(unsorted_builders, key=get_address)


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
