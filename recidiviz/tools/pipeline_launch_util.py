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

from typing import List

from recidiviz.calculator.pipeline.incarceration import (
    pipeline as incarceration_pipeline,
)
from recidiviz.calculator.pipeline.program import pipeline as program_pipeline
from recidiviz.calculator.pipeline.recidivism import pipeline as recidivism_pipeline
from recidiviz.calculator.pipeline.supervision import pipeline as supervision_pipeline
from recidiviz.calculator.pipeline.utils.pipeline_args_utils import (
    get_apache_beam_pipeline_options_from_args,
)


PIPELINE_MODULES = {
    "incarceration": incarceration_pipeline,
    "recidivism": recidivism_pipeline,
    "supervision": supervision_pipeline,
    "program": program_pipeline,
}


def get_pipeline_module(pipeline: str):  # type: ignore
    """Returns the calculation pipeline module corresponding to the given pipeline type."""
    pipeline_module = PIPELINE_MODULES.get(pipeline)

    if pipeline_module:
        return pipeline_module

    raise ValueError(f"Unexpected pipeline {pipeline}")


def run_pipeline(pipeline_module, argv: List[str]) -> None:  # type: ignore
    """Runs the given pipeline_module with the arguments contained in argv."""
    known_args, remaining_args = pipeline_module.get_arg_parser().parse_known_args(argv)
    apache_beam_pipeline_options = get_apache_beam_pipeline_options_from_args(
        remaining_args
    )

    pipeline_module.run(apache_beam_pipeline_options, **vars(known_args))
