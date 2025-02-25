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
          [--reference_input REFERENCE_INPUT] \
          [--include_age INCLUDE_AGE] \
          [--include_gender INCLUDE_GENDER] \
          [--include_race INCLUDE_RACE] \
          [--include_ethnicity INCLUDE_ETHNICITY] \
          [--methodology METHODOLOGY] \
          [--output OUTPUT]

          ..and any other pipeline-specific args

Examples:
    python -m recidiviz.tools.run_calculation_pipelines.py --pipeline incarceration --job_name incarceration-example

    python -m recidiviz.tools.run_calculation_pipelines.py --pipeline incarceration --job_name incarceration-example \
    --region us-central1 --include_race False --save_as_template --calculation_month_limit 36

You must also include any arguments required by the given pipeline.
"""
from __future__ import absolute_import

import logging
import sys
import argparse

from recidiviz.calculator.pipeline.incarceration import \
    pipeline as incarceration_pipeline
from recidiviz.calculator.pipeline.program import \
    pipeline as program_pipeline
from recidiviz.calculator.pipeline.recidivism import \
    pipeline as recidivism_pipeline
from recidiviz.calculator.pipeline.supervision import \
    pipeline as supervision_pipeline


# pylint: disable=line-too-long
def parse_arguments(argv):
    """Parses the arguments needed to run pipelines."""
    parser = argparse.ArgumentParser()

    parser.add_argument('--pipeline',
                        dest='pipeline',
                        type=str,
                        choices=['recidivism', 'supervision', 'program',
                                 'incarceration'],
                        help='The type of pipeline that should be run.',
                        required=True)

    return parser.parse_known_args(argv)


def run_calculation_pipelines():
    """Runs the pipeline designated by the given --pipeline argument."""
    known_args, remaining_args = parse_arguments(sys.argv)

    if known_args.pipeline == 'incarceration':
        incarceration_pipeline.run(remaining_args)
    if known_args.pipeline == 'recidivism':
        recidivism_pipeline.run(remaining_args)
    elif known_args.pipeline == 'supervision':
        supervision_pipeline.run(remaining_args)
    elif known_args.pipeline == 'program':
        program_pipeline.run(remaining_args)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_calculation_pipelines()
