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
"""Driver script to launch test flex template calculation pipeline jobs with output directed to
sandbox dataflow datasets.

See http://go/run-dataflow/ for more information on running Dataflow pipelines.

usage: python -m recidiviz.tools.calculator.run_sandbox_calculation_pipeline \
          --pipeline PIPELINE_NAME \
          --type PIPELINE_TYPE \
          --job_name JOB_NAME \
          --project PROJECT \
          --state_code STATE_CODE \
          --sandbox_output_dataset SANDBOX_OUTPUT_DATASET \
          --calculation_month_count NUM_MONTHS \
          [--normalized_input INPUT] \
          [--data_input INPUT] \
          [--reference_view_input REFERENCE_VIEW_INPUT] \
          [--static_reference_input STATIC_REFERENCE_VIEW_INPUT] \
          # Note: The --metric_types arg must be last since it is a list
          [--metric_types METRIC_TYPES]

          ..and any other pipeline-specific args

Examples:
    python -m recidiviz.tools.calculator.run_sandbox_calculation_pipeline \
        --pipeline recidivism_metrics \
        --type metrics \
        --project recidiviz-staging \
        --job_name my-nd-recidivism-metrics-test \
        --sandbox_output_dataset username_dataflow_metrics \
        --state_code US_ND \
        --metric_types "REINCARCERATION_COUNT REINCARCERATION_RATE"

    python -m recidiviz.tools.calculator.run_sandbox_calculation_pipeline \
        --pipeline comprehensive_normalization \
        --type normalization \
        --project recidiviz-staging \
        --job_name my-nd-normalization-test \
        --sandbox_output_dataset username_normalized_state \
        --state_code US_ND

    python -m recidiviz.tools.calculator.run_sandbox_calculation_pipeline \
        --pipeline us_ix_case_note_extracted_entities_supplemental \
        --type supplemental \
        --project recidiviz-staging \
        --job_name my-id-supplemental-test \
        --sandbox_output_dataset username_supplemental_data \
        --state_code US_IX


You must also include any arguments required by the given pipeline.
"""
from __future__ import absolute_import

import argparse
import json
import logging
import os
import time
from typing import Type

import google.auth
import google.auth.transport.requests
import requests

from recidiviz.calculator import pipeline
from recidiviz.calculator.pipeline.metrics.pipeline_parameters import (
    MetricsPipelineParameters,
)
from recidiviz.calculator.pipeline.normalization.pipeline_parameters import (
    NormalizationPipelineParameters,
)
from recidiviz.calculator.pipeline.pipeline_parameters import PipelineParameters
from recidiviz.calculator.pipeline.supplemental.pipeline_parameters import (
    SupplementalPipelineParameters,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation, run_command


def parse_run_arguments() -> PipelineParameters:
    """Parses the arguments needed to start a sandbox pipeline to a Namespace."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--type",
        type=str,
        dest="pipeline_type",
        help="type of the pipeline",
        choices=["metrics", "normalization", "supplemental"],
        required=True,
    )

    known_args, remaining_args = parser.parse_known_args()

    parameter_cls: Type[PipelineParameters]
    if known_args.pipeline_type == "metrics":
        parameter_cls = MetricsPipelineParameters
    elif known_args.pipeline_type == "normalization":
        parameter_cls = NormalizationPipelineParameters
    elif known_args.pipeline_type == "supplemental":
        parameter_cls = SupplementalPipelineParameters
    else:
        raise ValueError(f"Unexpected pipeline_type [{known_args.pipeline_type}]")

    return parameter_cls.parse_from_args(remaining_args, sandbox_pipeline=True)


def fetch_google_auth_token() -> str:
    creds, _ = google.auth.default()

    # creds.valid is False, and creds.token is None
    # Need to refresh credentials to populate those
    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)

    return creds.token  # type: ignore[attr-defined]


def get_cloudbuild_path() -> str:
    pipeline_root_path = os.path.dirname(pipeline.__file__)
    cloudbuild_path = "cloudbuild.pipelines.dev.yaml"

    return os.path.join(pipeline_root_path, cloudbuild_path)


def get_template_path(pipeline_type: str) -> str:
    pipeline_root_path = os.path.dirname(pipeline.__file__)
    template_path = f"{pipeline_type}/template_metadata.json"
    return os.path.join(pipeline_root_path, template_path)


def run_sandbox_calculation_pipeline() -> None:
    """Runs the pipeline designated by the given --pipeline argument."""
    params = parse_run_arguments()
    _, username = os.path.split(params.template_metadata_subdir)
    launch_body = params.flex_template_launch_body()
    template_gcs_path = params.template_gcs_path(params.project)
    cloudbuild_absolute_path = get_cloudbuild_path()
    template_absolute_path = get_template_path(params.flex_template_name)
    artifact_reg_image_path = (
        f"us-docker.pkg.dev/recidiviz-staging/dataflow-dev/{username}/build:latest"
    )

    # Have the user confirm that the sandbox dataflow dataset exists.
    prompt_for_confirmation(
        "Have you already created a sandbox dataflow dataset called "
        f"`{params.output}` using `create_or_update_dataflow_sandbox`?"
    )

    submit_build_start = time.time()

    # Build and submit the image to "us-docker.pkg.dev/recidiviz-staging/dataflow-dev/{username}-build:latest"
    print(
        "Submitting build (this takes a few minutes, or longer on the first run).....\n"
    )
    run_command(
        f"gcloud builds submit --project={params.project} --config {cloudbuild_absolute_path} --substitutions=_IMAGE_PATH={artifact_reg_image_path}",
        timeout_sec=900,
    )

    submit_build_exec_seconds = time.time() - submit_build_start
    build_minutes, build_seconds = divmod(submit_build_exec_seconds, 60)
    print(f"Submitted build in {build_minutes} minutes and {build_seconds} seconds.\n")

    # Upload the flex template json, tagged with the image that should be used
    # This step is only necessary when the template_metadata.json file or image path has been changed
    print(f"Building flex template (uploading to {template_gcs_path}) .....\n")
    run_command(
        f"gcloud dataflow flex-template build \
        {template_gcs_path} \
        --image {artifact_reg_image_path} \
        --sdk-language PYTHON \
        --metadata-file {template_absolute_path}"
    )

    # Run the dataflow job
    pipeline_launch_body = json.dumps(launch_body, indent=2)
    print("Starting flex template job with body:")
    print(pipeline_launch_body)

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + fetch_google_auth_token(),
    }
    response = requests.post(
        f"https://dataflow.googleapis.com/v1b3/projects/recidiviz-staging/locations/{params.region}/flexTemplates:launch",
        headers=headers,
        data=pipeline_launch_body,
        timeout=60,
    )

    if response.ok:
        print(
            f"Job {params.job_name} successfully launched - go to https://console.cloud.google.com/dataflow/jobs?project={params.project} to monitor job progress"
        )
    else:
        print("Job launch failed..")
        print(response.text)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_sandbox_calculation_pipeline()
