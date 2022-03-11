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
"""Driver script to launch test calculation pipeline jobs with output directed to
sandbox dataflow datasets.

All of the code needed to execute the jobs is in the recidiviz/ package. It is organized in this way so that it can be
packaged as a Python package and later installed in the VM workers executing the job on Dataflow.

See recidiviz/calculator/pipeline/utils/pipeline_args_utils.py for more details on each of the arguments.

See http://go/run-dataflow/ for more information on running Dataflow pipelines.

usage: python -m recidiviz.tools.calculator.run_sandbox_calculation_pipeline \
          --pipeline PIPELINE_TYPE \
          --job_name JOB-NAME \
          --project PROJECT \
          --state_code STATE_CODE \
          --sandbox_output_dataset SANDBOX_OUTPUT_DATASET \
          --calculation_month_count NUM_MONTHS \
          [--input INPUT] \
          [--reference_view_input REFERENCE_VIEW_INPUT] \
          [--static_reference_input STATIC_REFERENCE_VIEW_INPUT] \
          # Note: The --metric_types arg must be last since it is a list
          [--metric_types METRIC_TYPES]

          ..and any other pipeline-specific args

Examples:
    python -m recidiviz.tools.calculator.run_sandbox_calculation_pipeline
        --pipeline incarceration \
        --project recidiviz-staging \
        --job_name incarceration-test \
        --sandbox_output_dataset username_dataflow_metrics \
        --state_code US_ND

You must also include any arguments required by the given pipeline.
"""
from __future__ import absolute_import

import argparse
import logging
import sys
from typing import List, Optional, Tuple

from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET
from recidiviz.tools.deploy.build_dataflow_source_distribution import (
    build_source_distribution,
)
from recidiviz.tools.pipeline_launch_util import (
    collect_all_pipeline_names,
    load_all_pipelines,
    run_pipeline,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation


def parse_run_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the arguments needed to start a sandbox pipeline."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--pipeline",
        dest="pipeline",
        type=str,
        choices=collect_all_pipeline_names(),
        help="The name of the pipeline that should be run.",
        required=True,
    )

    parser.add_argument(
        "--job_name",
        dest="job_name",
        type=str,
        help="The name of the pipeline job to be run.",
        required=True,
    )

    parser.add_argument(
        "--sandbox_output_dataset",
        dest="sandbox_output_dataset",
        type=str,
        help="Output metrics dataset where results should be written to for test jobs.",
        required=True,
    )

    return parser.parse_known_args(argv)


def validated_run_arguments(
    arguments: List[str],
    job_name: str,
    sandbox_output_dataset: Optional[str],
) -> List[str]:
    """Validates the provided combination of provided arguments."""
    job_name_for_pipeline = job_name

    # Test runs require a specified sandbox output dataset
    if not sandbox_output_dataset:
        raise argparse.ArgumentError(
            argument=None,
            message="Test pipeline runs require a specified "
            "--sandbox_output_dataset argument.",
        )

    if sandbox_output_dataset == DATAFLOW_METRICS_DATASET:
        raise argparse.ArgumentError(
            argument=None,
            message="--sandbox_output_dataset argument for test pipelines must be "
            "different than the standard Dataflow metrics dataset.",
        )

    # Have the user confirm that the sandbox dataflow dataset exists.
    prompt_for_confirmation(
        "Have you already created a sandbox dataflow dataset called "
        f"`{sandbox_output_dataset}` using `create_or_update_dataflow_sandbox`?"
    )

    if "--output" in arguments:
        raise argparse.ArgumentError(
            argument=None,
            message="Cannot specify an --output argument when running a sandbox "
            "calculation pipeline. Please use the --sandbox_output_dataset "
            "argument instead.",
        )

    # Adding to the beginning of the list since the --metric_types argument
    # must be last
    arguments.insert(0, sandbox_output_dataset)
    arguments.insert(0, "--output")

    # If this is a test run, make sure the job name has a -test suffix and that a
    #  has been specified
    if not job_name.endswith("-test"):
        job_name_for_pipeline = job_name + "-test"
        logging.info(
            "Appending -test to the job_name because this is a test job: [%s]",
            job_name_for_pipeline,
        )

    # Adding to the beginning of the list since the --metric_types argument
    # must be last
    arguments.insert(0, job_name_for_pipeline)
    arguments.insert(0, "--job_name")

    arguments.insert(0, build_source_distribution())
    arguments.insert(0, "--extra_package")

    return arguments


def pipeline_module_name_and_arguments(arguments: List[str]) -> Tuple[str, List[str]]:
    """Parses and validates arguments needed to start running a pipelines. Returns the
    name of the pipeline module to be run and the list of arguments remaining for the
    pipeline execution."""
    # We drop arguments[0] because it's the script name
    run_arguments_to_validate, remaining_pipeline_args = parse_run_arguments(
        arguments[1:]
    )

    pipeline_module_name = run_arguments_to_validate.pipeline

    pipeline_arguments = validated_run_arguments(
        remaining_pipeline_args,
        run_arguments_to_validate.job_name,
        run_arguments_to_validate.sandbox_output_dataset,
    )

    return pipeline_module_name, pipeline_arguments


def run_sandbox_calculation_pipeline() -> None:
    """Runs the pipeline designated by the given --pipeline argument."""
    load_all_pipelines()
    pipeline_module_name, pipeline_arguments = pipeline_module_name_and_arguments(
        sys.argv
    )

    run_pipeline(pipeline_module_name, pipeline_arguments)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_sandbox_calculation_pipeline()
