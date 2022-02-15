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
"""Util for launching Dataflow pipelines.
"""
from __future__ import absolute_import

import importlib
import inspect
import pkgutil
from typing import List, Type

from recidiviz.calculator.pipeline import metrics as metrics_pipeline_top_level
from recidiviz.calculator.pipeline.base_pipeline import (
    BasePipeline,
    PipelineRunDelegate,
)
from recidiviz.common.module_collector_mixin import ModuleCollectorMixin


def load_all_pipelines() -> None:
    """Loads all subclasses of PipelineRunDelegate."""
    for _, name, _ in pkgutil.walk_packages(metrics_pipeline_top_level.__path__):  # type: ignore
        full_name = f"{metrics_pipeline_top_level.__name__}.{name}.pipeline"
        try:
            importlib.import_module(full_name)
        except ModuleNotFoundError:
            continue


def collect_all_pipeline_names() -> List[str]:
    """Collects all of the pipeline names from all of the implementations of the
    PipelineRunDelegate."""
    run_delegates = collect_all_pipeline_run_delegates()

    return [
        run_delegate.pipeline_config().pipeline_name.lower()
        for run_delegate in run_delegates
    ]


def collect_all_pipeline_run_delegates() -> List[Type[PipelineRunDelegate]]:
    metrics_submodules = ModuleCollectorMixin.get_submodules(
        base_module=metrics_pipeline_top_level, submodule_name_prefix_filter=None
    )

    run_delegates: List[Type[PipelineRunDelegate]] = []

    for module in metrics_submodules:
        pipeline_modules = ModuleCollectorMixin.get_submodules(
            module, submodule_name_prefix_filter="pipeline"
        )

        for pipeline_module in pipeline_modules:
            for attribute_name in dir(pipeline_module):
                attribute = getattr(pipeline_module, attribute_name)
                if inspect.isclass(attribute):
                    if issubclass(
                        attribute, PipelineRunDelegate
                    ) and not inspect.isabstract(attribute):
                        run_delegates.append(attribute)

    return run_delegates


def _delegate_cls_for_pipeline_name(pipeline_name: str) -> Type[PipelineRunDelegate]:
    """Finds the PipelineRunDelegate class corresponding to the pipeline with the
    given |pipeline_name|."""
    all_run_delegates = collect_all_pipeline_run_delegates()
    delegates_with_pipeline_name = [
        delegate
        for delegate in all_run_delegates
        if delegate.pipeline_config().pipeline_name.lower() == pipeline_name
    ]

    if len(delegates_with_pipeline_name) != 1:
        raise ValueError(
            "Expected exactly one PipelineRunDelegate with the "
            f"pipeline_name: {pipeline_name}. Found: {delegates_with_pipeline_name}."
        )

    return delegates_with_pipeline_name[0]


def run_pipeline(pipeline_name: str, argv: List[str]) -> None:
    """Runs the given pipeline_module with the arguments contained in argv."""
    delegate_cls = _delegate_cls_for_pipeline_name(pipeline_name)
    pipeline = BasePipeline(pipeline_run_delegate=delegate_cls.build_from_args(argv))
    pipeline.run()
