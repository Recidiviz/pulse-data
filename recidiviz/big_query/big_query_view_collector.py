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
from typing import Generic, List, Optional, Type

import recidiviz
from recidiviz.big_query.big_query_view import BigQueryViewBuilderType
from recidiviz.common.module_collector_mixin import ModuleCollectorMixin

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
        builder_type: Type[BigQueryViewBuilderType],
        relative_dir_path: str,
        view_file_prefix_filter: Optional[str] = None,
    ) -> List[BigQueryViewBuilderType]:
        """Collects all view builders in a directory relative to the recidiviz base directory and returns a list of all
        views that can be built from builders defined in files in that directory.

        Args:
            builder_type: The type of builder that we expect to find in this subdir
            relative_dir_path: The relative path to search in (e.g. 'calculator/query/state/views/admissions').
            view_file_prefix_filter: When set, collection filters out any files whose name does not have this prefix.
        """
        sub_module_parts = os.path.normpath(relative_dir_path).split("/")
        view_dir_module = cls.get_relative_module(recidiviz, sub_module_parts)
        return cls.collect_view_builders_in_module(
            builder_type=builder_type,
            view_dir_module=view_dir_module,
            view_file_prefix_filter=view_file_prefix_filter,
        )

    @classmethod
    def collect_view_builders_in_module(
        cls,
        builder_type: Type[BigQueryViewBuilderType],
        view_dir_module: ModuleType,
        view_file_prefix_filter: Optional[str] = None,
    ) -> List[BigQueryViewBuilderType]:
        """Collects all view builders in a directory module and returns a list of all
        views that can be built from builders defined in files in that directory.

        Args:
            builder_type: The type of builder that we expect to find in this subdir
            view_dir_module: The module for the directory that contains all the view definition files.
            view_file_prefix_filter: When set, collection filters out any files whose name does not have this prefix.
        """

        view_modules = cls.get_submodules(view_dir_module, view_file_prefix_filter)

        builders = []
        for view_module in view_modules:
            if not hasattr(view_module, VIEW_BUILDER_EXPECTED_NAME):
                raise ValueError(
                    f"File [{view_module.__file__}] has no top-level attribute called "
                    f"[{VIEW_BUILDER_EXPECTED_NAME}]"
                )

            builder = getattr(view_module, VIEW_BUILDER_EXPECTED_NAME)

            if not isinstance(builder, builder_type):
                raise ValueError(f"Unexpected type for builder [{type(builder)}]")

            builders.append(builder)

        return builders
