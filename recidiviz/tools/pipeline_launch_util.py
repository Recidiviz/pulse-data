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
import pkgutil
from typing import List

from recidiviz.calculator import pipeline as pipeline_top_level
from recidiviz.calculator.pipeline.base_pipeline import (
    BasePipeline,
    PipelineRunDelegate,
)


def load_all_pipelines() -> None:
    """Loads all subclasses of CalculationPipelineRunDelegate."""
    for _, name, _ in pkgutil.walk_packages(pipeline_top_level.__path__):  # type: ignore
        full_name = f"{pipeline_top_level.__name__}.{name}.pipeline"
        try:
            importlib.import_module(full_name)
        except ModuleNotFoundError:
            continue


def _delegate_cls_for_pipeline_name(pipeline_name: str) -> PipelineRunDelegate:
    pipeline_module = f"{pipeline_top_level.__name__}.{pipeline_name}.pipeline"
    delegate_name = f"{pipeline_name.capitalize()}PipelineRunDelegate"

    return getattr(importlib.import_module(pipeline_module), delegate_name)


def run_pipeline(pipeline_name: str, argv: List[str]) -> None:
    """Runs the given pipeline_module with the arguments contained in argv."""
    delegate_cls = _delegate_cls_for_pipeline_name(pipeline_name)
    pipeline = BasePipeline(pipeline_run_delegate=delegate_cls.build_from_args(argv))
    pipeline.run()
