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
          --pipeline_type PIPELINE_TYPE \
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
        --pipeline_type metrics \
        --project recidiviz-staging \
        --job_name my-nd-recidivism-metrics-test \
        --sandbox_output_dataset username_dataflow_metrics \
        --state_code US_ND \
        --metric_types REINCARCERATION_COUNT REINCARCERATION_RATE

    python -m recidiviz.tools.calculator.run_sandbox_calculation_pipeline \
        --pipeline comprehensive_normalization \
        --pipeline_type normalization \
        --project recidiviz-staging \
        --job_name my-nd-normalization-test \
        --sandbox_output_dataset username_normalized_state \
        --state_code US_ND

    python -m recidiviz.tools.calculator.run_sandbox_calculation_pipeline \
        --pipeline us_ix_case_note_extracted_entities_supplemental \
        --pipeline_type supplemental \
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
from typing import Any, Dict

import google.auth
import google.auth.transport.requests
import requests

from recidiviz.airflow.dags.utils.pipeline_parameters import (
    MetricsPipelineParameters,
    NormalizationPipelineParameters,
    PipelineParameters,
    SupplementalPipelineParameters,
)
from recidiviz.calculator import pipeline
from recidiviz.calculator.pipeline.pipeline_runner import collect_all_pipeline_names
from recidiviz.calculator.pipeline.utils.execution_utils import (
    calculation_month_count_arg,
)
from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation, run_command
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING


class ValidateSandboxDataset(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: Any = None,
    ) -> None:
        if values == DATAFLOW_METRICS_DATASET:
            parser.error(
                f"--sandbox_output_dataset argument for test pipelines must be "
                f"different than the standard Dataflow metrics dataset: "
                f"{DATAFLOW_METRICS_DATASET}."
            )
        setattr(namespace, self.dest, values)


class NormalizeJobName(argparse.Action):
    """Since this is a test run, make sure the job name has a -test suffix."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: Any = None,
    ) -> None:
        job_name = values
        if not job_name.endswith("-test"):
            job_name = job_name + "-test"
            logging.info(
                "Appending -test to the job_name because this is a test job: [%s]",
                job_name,
            )
        setattr(namespace, self.dest, job_name)


class NormalizeMetricTypes(argparse.Action):
    """Since this is a test run, make sure the job name has a -test suffix."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: Any = None,
    ) -> None:
        metric_types_str = " ".join(values)
        setattr(namespace, self.dest, metric_types_str)


def parse_run_arguments() -> argparse.Namespace:
    """Parses the arguments needed to start a sandbox pipeline to a Namespace."""
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
        action=NormalizeJobName,
    )

    parser.add_argument(
        "--sandbox_output_dataset",
        # Change output name to match what pipeline args expect
        dest="output",
        type=str,
        help="Output metrics dataset where results should be written to for test jobs.",
        required=True,
        action=ValidateSandboxDataset,
    )

    parser.add_argument(
        "--state_code",
        dest="state_code",
        type=str,
        help="state code",
        required=True,
    )

    parser.add_argument(
        "--project",
        type=str,
        help="ID of the GCP project.",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )

    parser.add_argument(
        "--pipeline_type",
        type=str,
        help="type of the pipeline",
        choices=["metrics", "normalization", "supplemental"],
        required=True,
    )

    parser.add_argument(
        "--region",
        type=str,
        help="The Google Cloud region to run the job on (e.g. us-west1).",
        default="us-west1",
    )

    parser.add_argument(
        "--normalized_input",
        type=str,
        help="BigQuery dataset to query for normalized versions of entities.",
        required=False,
    )

    parser.add_argument(
        "--data_input", type=str, help="BigQuery dataset to query.", required=False
    )

    parser.add_argument(
        "--reference_view_input",
        type=str,
        help="BigQuery reference view dataset to query.",
        required=False,
    )

    parser.add_argument(
        "--person_filter_ids",
        type=int,
        nargs="+",
        help="An optional list of DB person_id values. When present, the pipeline "
        "will only calculate metrics for these people and will not output to BQ.",
    )

    # metric_types, calculation_month_count, static_reference_input can only be used for metric type pipelines
    parser.add_argument(
        "--metric_types",
        dest="metric_types",
        type=str,
        nargs="+",
        help="A list of the types of metric to calculate.",
        action=NormalizeMetricTypes,
        required=False,
    )

    parser.add_argument(
        "--calculation_month_count",
        dest="calculation_month_count",
        type=calculation_month_count_arg,
        help="The number of months (including this one) to limit the monthly "
        "calculation output to. If set to -1, does not limit the "
        "calculations.",
        required=False,
    )

    parser.add_argument(
        "--static_reference_input",
        type=str,
        help="BigQuery static reference table dataset to query.",
        required=False,
    )

    return parser.parse_args()


def fetch_google_auth_token() -> str:
    creds, _ = google.auth.default()

    # creds.valid is False, and creds.token is None
    # Need to refresh credentials to populate those
    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)

    return creds.token  # type: ignore[attr-defined]


def get_pipeline_parameters(
    pipeline_type: str, template_metadata_subdir: str, arguments_dict: Dict[str, Any]
) -> PipelineParameters:
    if pipeline_type == "metrics":
        return MetricsPipelineParameters(
            **arguments_dict, template_metadata_subdir=template_metadata_subdir
        )
    if pipeline_type == "normalization":
        return NormalizationPipelineParameters(
            **arguments_dict, template_metadata_subdir=template_metadata_subdir
        )
    if pipeline_type == "supplemental":
        return SupplementalPipelineParameters(
            **arguments_dict, template_metadata_subdir=template_metadata_subdir
        )
    raise ValueError(f"Received invalid pipeline type: {pipeline_type}")


def get_cloudbuild_path() -> str:
    pipeline_root_path = os.path.dirname(pipeline.__file__)
    cloudbuild_path = "cloudbuild.pipelines.dev.yaml"

    return os.path.join(pipeline_root_path, cloudbuild_path)


def get_template_path(pipeline_type: str) -> str:
    pipeline_root_path = os.path.dirname(pipeline.__file__)
    template_path = f"{pipeline_type}/template_metadata.json"
    return os.path.join(pipeline_root_path, template_path)


def run_sandbox_calculation_pipeline(
    project: str,
    pipeline_type: str,
    # Remainder of arguments passed through to pipelines
    **arguments_dict: Any,
) -> None:
    """Runs the pipeline designated by the given --pipeline argument."""

    username = run_command("git config user.name", timeout_sec=300)
    username = username.replace(" ", "").strip().lower()
    if not username:
        raise ValueError("Found no configured git username")

    template_metadata_subdir = f"template_metadata_dev/{username}"
    params: PipelineParameters = get_pipeline_parameters(
        pipeline_type, template_metadata_subdir, arguments_dict
    )
    launch_body = params.flex_template_launch_body(project)
    template_gcs_path = params.template_gcs_path(project)
    cloudbuild_absolute_path = get_cloudbuild_path()
    template_absolute_path = get_template_path(pipeline_type)
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
        f"gcloud builds submit --project={project} --config {cloudbuild_absolute_path} --substitutions=_IMAGE_PATH={artifact_reg_image_path}",
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
            f"Job {params.job_name} successfully launched - go to https://console.cloud.google.com/dataflow/jobs?project={project} to monitor job progress"
        )
    else:
        print("Job launch failed..")
        print(response.text)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_run_arguments()
    # Collect all parsed arguments and filter out the ones that are not set
    set_kwargs = {k: v for k, v in vars(args).items() if v is not None}
    run_sandbox_calculation_pipeline(**set_kwargs)
