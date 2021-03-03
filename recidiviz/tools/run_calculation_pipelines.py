# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Driver script to launch calculation pipeline jobs.

All of the code needed to execute the jobs is in the recidiviz/ package. It is organized in this way so that it can be
packaged as a Python package and later installed in the VM workers executing the job on Dataflow.

When using DataflowRunner to execute the job, you must specify the path to a setup file (defaults to ./setup.py if you
do not specify). The --setup_file option in Dataflow will trigger creating a source distribution (as if running python
setup.py sdist) and then staging the resulting tarball in the staging area. The workers, upon startup, will install the
tarball.

See recidiviz/calculator/pipeline/utils/pipeline_args_utils.py for more details on each of the arguments.

usage: python -m recidiviz.tools.run_calculation_pipelines.py
          --pipeline PIPELINE_TYPE
          --job_name JOB-NAME \
          --project PROJECT \
          [--setup_file SETUP_FILE] \
          [--bucket BUCKET] \
          [--save_as_template] \
          [--region REGION] \
          [--runner RUNNER] \
          [--input INPUT] \
          [--reference_view_input REFERENCE_VIEW_INPUT] \
          [--static_reference_input STATIC_REFERENCE_VIEW_INPUT]
          [--include_age INCLUDE_AGE] \
          [--include_gender INCLUDE_GENDER] \
          [--include_race INCLUDE_RACE] \
          [--include_ethnicity INCLUDE_ETHNICITY] \
          [--output OUTPUT]

          ..and any other pipeline-specific args

Examples:
    python -m recidiviz.tools.run_calculation_pipelines.py --pipeline incarceration --job_name incarceration-example

    python -m recidiviz.tools.run_calculation_pipelines.py --pipeline incarceration --job_name incarceration-example \
    --region us-central1 --include_race False --save_as_template --calculation_month_count 36

You must also include any arguments required by the given pipeline.
"""
from __future__ import absolute_import

import argparse
import logging
import sys
from typing import List, Tuple

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


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the arguments needed to run pipelines."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--pipeline",
        dest="pipeline",
        type=str,
        choices=PIPELINE_MODULES.keys(),
        help="The type of pipeline that should be run.",
        required=True,
    )

    return parser.parse_known_args(argv)


def get_pipeline_module(pipeline: str):
    """Returns the calculation pipeline module corresponding to the given pipeline type."""
    pipeline_module = PIPELINE_MODULES.get(pipeline)

    if pipeline_module:
        return pipeline_module

    raise ValueError(f"Unexpected pipeline {pipeline}")


def run_calculation_pipelines() -> None:
    """Runs the pipeline designated by the given --pipeline argument."""
    # We drop argv[0] because it's the script name
    known_args, remaining_args = parse_arguments(sys.argv[1:])

    pipeline_module = get_pipeline_module(known_args.pipeline)

    run_pipeline(pipeline_module, remaining_args)


def run_pipeline(pipeline_module, argv: List[str]) -> None:
    """Runs the given pipeline_module with the arguments contained in argv."""
    known_args, remaining_args = pipeline_module.get_arg_parser().parse_known_args(argv)
    apache_beam_pipeline_options = get_apache_beam_pipeline_options_from_args(
        remaining_args
    )

    pipeline_module.run(apache_beam_pipeline_options, **vars(known_args))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_calculation_pipelines()
