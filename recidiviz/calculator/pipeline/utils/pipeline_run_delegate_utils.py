# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Utils for working with PipelineRunDelegates."""
from __future__ import absolute_import

import inspect
from types import ModuleType
from typing import List, Type

from recidiviz.calculator.pipeline import metrics as metrics_pipeline_top_level
from recidiviz.calculator.pipeline import (
    normalization as normalization_pipeline_top_level,
)
from recidiviz.calculator.pipeline import (
    supplemental as supplemental_pipeline_top_level,
)
from recidiviz.calculator.pipeline.base_pipeline import PipelineRunDelegate
from recidiviz.common.module_collector_mixin import ModuleCollectorMixin


def collect_all_pipeline_run_delegate_classes() -> List[Type[PipelineRunDelegate]]:
    """Collects all of the versions of the PipelineRunDelegate."""

    pipeline_modules = collect_all_pipeline_run_delegate_modules()
    run_delegates: List[Type[PipelineRunDelegate]] = []

    for pipeline_module in pipeline_modules:
        for attribute_name in dir(pipeline_module):
            attribute = getattr(pipeline_module, attribute_name)
            if inspect.isclass(attribute):
                if issubclass(
                    attribute, PipelineRunDelegate
                ) and not inspect.isabstract(attribute):
                    run_delegates.append(attribute)

    return run_delegates


def collect_all_pipeline_run_delegate_modules() -> List[ModuleType]:
    """Collects all of the modules storing PipelineRunDelegate implementations."""
    pipeline_submodules: List[ModuleType] = []

    for top_level_pipeline_module in TOP_LEVEL_PIPELINE_MODULES:
        pipeline_submodules.extend(
            ModuleCollectorMixin.get_submodules(
                base_module=top_level_pipeline_module, submodule_name_prefix_filter=None
            )
        )

    pipeline_file_modules: List[ModuleType] = []

    for module in pipeline_submodules:
        pipeline_modules = ModuleCollectorMixin.get_submodules(
            module, submodule_name_prefix_filter="pipeline"
        )

        if len(pipeline_modules) > 1:
            raise ValueError(
                "More than one submodule found named 'pipeline' in "
                f"module: {module}. Found: [{pipeline_modules}]."
            )
        if pipeline_modules:
            pipeline_file_modules.append(pipeline_modules[0])

    return pipeline_file_modules


TOP_LEVEL_PIPELINE_MODULES: List[ModuleType] = [
    metrics_pipeline_top_level,
    normalization_pipeline_top_level,
    supplemental_pipeline_top_level,
]
