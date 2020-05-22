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
import importlib
import os
import pkgutil
from types import ModuleType
from typing import Generic, List, Optional, Type

import recidiviz
from recidiviz.big_query.big_query_view import BigQueryViewType, BigQueryViewBuilder

VIEW_BUILDER_EXPECTED_NAME = 'VIEW_BUILDER'


class BigQueryViewCollector(Generic[BigQueryViewType]):
    """A class that collects top-level BigQueryViewBuilder instances in a directory and uses them to build
    BigQueryViews.

    In order to be discovered, the view builder instances must have the name defined in VIEW_BUILDER_EXPECTED_NAME.
    """

    @abc.abstractmethod
    def collect_views(self) -> List[BigQueryViewType]:
        """Returns a list of views of the appropriate type. Should be implemented by subclasses."""

    @staticmethod
    def _get_relative_module(base_module: ModuleType,
                             sub_module_parts: List[str]) -> ModuleType:
        module = base_module
        for part in sub_module_parts:
            module = importlib.import_module(f'{module.__name__}.{part}')
        return module

    @staticmethod
    def _get_submodule_names(base_module: ModuleType,
                             submodule_name_prefix_filter: Optional[str]) -> List[str]:
        base_module_path = os.path.dirname(base_module.__file__)
        return [name for _, name, _ in pkgutil.iter_modules([base_module_path])
                if not submodule_name_prefix_filter or name.startswith(submodule_name_prefix_filter)]

    @classmethod
    def _get_submodules(cls, base_module: ModuleType, submodule_name_prefix_filter: Optional[str]) -> List[ModuleType]:
        submodules = []
        for submodule_name in cls._get_submodule_names(base_module, submodule_name_prefix_filter):
            submodules.append(cls._get_relative_module(base_module, [submodule_name]))

        return submodules

    @classmethod
    def collect_and_build_views_in_dir(cls,
                                       view_type: Type[BigQueryViewType],
                                       relative_dir_path: str,
                                       view_file_prefix_filter: Optional[str] = None) -> List[BigQueryViewType]:
        """Collects all view builders in a directory relative to the recidiviz base directory and returns a list of all
        views that can be built from builders defined in files in that directory.

        Args:
            view_type: The type of view that we expect to find in this subdir
            relative_dir_path: The relative path to search in (e.g. 'calculator/query/state/views/admissions').
            view_file_prefix_filter: When set, collection filters out any files whose name does not have this prefix.
        """
        sub_module_parts = os.path.normpath(relative_dir_path).split('/')
        view_dir_module = cls._get_relative_module(recidiviz, sub_module_parts)
        view_modules = cls._get_submodules(view_dir_module, view_file_prefix_filter)

        views = []
        for view_module in view_modules:
            if not hasattr(view_module, VIEW_BUILDER_EXPECTED_NAME):
                raise ValueError(f'File [{view_module.__file__}] has no top-level attribute called '
                                 f'[{VIEW_BUILDER_EXPECTED_NAME}]')

            builder = getattr(view_module, VIEW_BUILDER_EXPECTED_NAME)

            if not isinstance(builder, BigQueryViewBuilder):
                raise ValueError(f'Unexpected type for builder [{type(builder)}]')

            view = builder.build()
            if not isinstance(view, view_type):
                raise ValueError(f'Unexpected type for built view [{type(view)}]')

            views.append(view)

        return views
