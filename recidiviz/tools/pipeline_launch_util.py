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
from recidiviz.calculator.pipeline.base_pipeline import BasePipeline
from recidiviz.calculator.pipeline.utils.beam_utils.pipeline_args_utils import (
    get_apache_beam_pipeline_options_from_args,
)


def load_all_pipelines() -> None:
    """Loads all subclasses of BasePipeline."""
    for _, name, _ in pkgutil.walk_packages(pipeline_top_level.__path__):  # type: ignore
        full_name = f"{pipeline_top_level.__name__}.{name}.pipeline"
        try:
            importlib.import_module(full_name)
        except ModuleNotFoundError:
            continue


def get_pipeline(pipeline: str) -> BasePipeline:
    """Returns the calculation pipeline module corresponding to the given pipeline type."""
    for subclass in BasePipeline.__subclasses__():
        instance = subclass()  # type: ignore
        if instance.pipeline_config.pipeline_type.value.lower() == pipeline:
            return instance

    raise ValueError(f"Unexpected pipeline {pipeline}")


def run_pipeline(pipeline: BasePipeline, argv: List[str]) -> None:
    """Runs the given pipeline_module with the arguments contained in argv."""
    known_args, remaining_args = pipeline.get_arg_parser().parse_known_args(argv)
    apache_beam_pipeline_options = get_apache_beam_pipeline_options_from_args(
        remaining_args
    )

    pipeline.run(apache_beam_pipeline_options, **vars(known_args))
