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

All of the code needed to execute the jobs is in the recidiviz/ package. It
is organized in this way so that it can be packaged as a Python package and
later installed in the VM workers executing the job on Dataflow.

When using DataflowRunner to execute the job, you must specify the path to a
setup file. The --setup_file option in Dataflow will trigger creating a source
distribution (as if running python setup.py sdist) and then staging the
resulting tarball in the staging area. The workers, upon startup, will install
the tarball.

usage: python run_calculation_pipelines.py
          --pipeline PIPELINE_TYPE
          --job_name JOB-NAME \
          --project PROJECT \
          --runner RUNNER \
          --setup_file ./setup.py \
          --staging_location gs://YOUR-BUCKET/staging \
          --temp_location gs://YOUR-BUCKET/tmp \
          --worker_machine_type n1-standard-4 \

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

    parser.add_argument('--job_name',
                        type=str,
                        help='Name of the pipeline calculation job.',
                        required=True)

    parser.add_argument('--project',
                        type=str,
                        help='ID of the GCP project.',
                        required=True)

    parser.add_argument('--setup_file',
                        type=str,
                        help='Path to the setup.py file.',
                        required=False)

    parser.add_argument('--runner',
                        type=str,
                        choices=['DirectRunner', 'DataflowRunner'],
                        default='DirectRunner',
                        help='The pipeline runner that will parse the program'
                             ' and construct the pipeline',
                        required=False)

    parser.add_argument('--staging_location',
                        type=str,
                        help='A Cloud Storage path for Cloud Dataflow to stage'
                             ' code packages needed by workers executing the'
                             ' job.',
                        required=False)

    parser.add_argument('--temp_location',
                        type=str,
                        help='A Cloud Storage path for Cloud Dataflow to stage'
                             ' temporary job files created during the execution'
                             ' of the pipeline.',
                        required=False)

    parser.add_argument('--worker_machine_type',
                        type=str,
                        help='The machine type for all job workers to use. See'
                             ' available machine types here: https://cloud.google.com/compute/docs/machine-types',
                        required=False)

    return parser.parse_known_args(argv)


def run_calculation_pipelines():
    """Runs the pipeline designated by the given --pipeline argument."""
    known_args, _ = parse_arguments(sys.argv)

    if known_args.pipeline == 'incarceration':
        incarceration_pipeline.run()
    if known_args.pipeline == 'recidivism':
        recidivism_pipeline.run()
    elif known_args.pipeline == 'supervision':
        supervision_pipeline.run()
    elif known_args.pipeline == 'program':
        program_pipeline.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_calculation_pipelines()
