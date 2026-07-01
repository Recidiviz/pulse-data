# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Utils for working with Pipelines"""
from __future__ import absolute_import

import inspect
from types import ModuleType
from typing import List, Type

from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.pipelines import metrics as metrics_pipeline_top_level
from recidiviz.pipelines import supplemental as supplemental_pipeline_top_level
from recidiviz.pipelines.base_pipeline import BasePipeline
from recidiviz.pipelines.ingest import activity as activity_ingest_pipeline_top_level


def collect_all_pipeline_names() -> List[str]:
    """Collects all of the pipeline names from all of the implementations of the Pipeline.
    A Pipeline must exist inside one of hte modules listed in the _TOP_LEVEL_PIPELINE_MODULES
    for it to be included."""
    pipelines = collect_all_pipeline_classes()

    return [pipeline.pipeline_name().lower() for pipeline in pipelines]


def collect_all_pipeline_classes() -> List[Type[BasePipeline]]:
    """Collects all of the versions of the BasePipeline."""
    pipeline_modules = collect_all_pipeline_modules()
    pipelines: List[Type[BasePipeline]] = []

    for pipeline_module in pipeline_modules:
        for attribute_name in dir(pipeline_module):
            attribute = getattr(pipeline_module, attribute_name)
            if inspect.isclass(attribute):
                if issubclass(attribute, BasePipeline) and not inspect.isabstract(
                    attribute
                ):
                    pipelines.append(attribute)

    return pipelines


def _get_pipeline_submodule(module: ModuleType) -> ModuleType | None:
    """Returns the ``pipeline`` submodule of |module|, or None."""
    pipeline_modules = [
        m
        for m in ModuleCollectorMixin.get_submodules(
            module, submodule_name_prefix_filter="pipeline"
        )
        if m.__name__.endswith(".pipeline")
    ]
    if len(pipeline_modules) > 1:
        raise ValueError(
            "More than one submodule named 'pipeline' in "
            f"module: {module}. Found: [{pipeline_modules}]."
        )
    return pipeline_modules[0] if pipeline_modules else None


def collect_all_pipeline_modules() -> List[ModuleType]:
    """Collects all of the modules storing BasePipeline implementations.

    For each entry in _TOP_LEVEL_PIPELINE_MODULES, looks for a ``pipeline``
    submodule. If found directly, uses it. Otherwise, walks sub-packages
    looking for ``pipeline`` inside each one.
    """
    pipeline_file_modules: List[ModuleType] = []

    for top_level_pipeline_module in _TOP_LEVEL_PIPELINE_MODULES:
        direct = _get_pipeline_submodule(top_level_pipeline_module)
        if direct:
            pipeline_file_modules.append(direct)
            continue

        for submodule in ModuleCollectorMixin.get_submodules(
            base_module=top_level_pipeline_module,
            submodule_name_prefix_filter=None,
        ):
            found = _get_pipeline_submodule(submodule)
            if found:
                pipeline_file_modules.append(found)

    return pipeline_file_modules


# TODO(#88681): Add identity ingest pipeline to this list.
_TOP_LEVEL_PIPELINE_MODULES: List[ModuleType] = [
    supplemental_pipeline_top_level,
    metrics_pipeline_top_level,
    activity_ingest_pipeline_top_level,
]
