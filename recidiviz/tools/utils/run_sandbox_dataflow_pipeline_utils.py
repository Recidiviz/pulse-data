# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Helper functions for launching sandbox Dataflow pipelines from a local script."""
import json
import os
import time
from typing import Set, Type

import google.auth
import google.auth.transport.requests
import requests

from recidiviz import pipelines
from recidiviz.pipelines.base_pipeline import BasePipeline
from recidiviz.pipelines.flex_pipeline_runner import pipeline_cls_for_pipeline_name
from recidiviz.pipelines.pipeline_parameters import PipelineParameters
from recidiviz.tools.utils.script_helpers import run_command
from recidiviz.utils.environment import get_environment_for_project


def fetch_google_auth_token() -> str:
    creds, _ = google.auth.default()

    # creds.valid is False, and creds.token is None
    # Need to refresh credentials to populate those
    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)

    return creds.token  # type: ignore[attr-defined]


def get_cloudbuild_path() -> str:
    pipeline_root_path = os.path.dirname(pipelines.__file__)
    cloudbuild_path = "cloudbuild.pipelines.dev.yaml"

    return os.path.join(pipeline_root_path, cloudbuild_path)


def get_template_path(pipeline_type: str) -> str:
    pipeline_root_path = os.path.dirname(pipelines.__file__)
    template_path = f"{pipeline_type}/template_metadata.json"
    return os.path.join(pipeline_root_path, template_path)


def get_all_reference_query_input_datasets_for_pipeline(
    pipeline_cls: Type[BasePipeline],
) -> Set[str]:
    """Returns all datasets that reference queries for this pipeline may query from."""
    return {
        parent_table.dataset_id
        for vb in pipeline_cls.all_input_reference_view_builders()
        for parent_table in vb.build().parent_tables
    }


def run_sandbox_dataflow_pipeline(params: PipelineParameters, skip_build: bool) -> None:
    """Runs the pipeline designated by the given |params|."""
    pipeline_cls = pipeline_cls_for_pipeline_name(params.pipeline)
    params.check_for_valid_input_dataset_overrides(
        get_all_reference_query_input_datasets_for_pipeline(pipeline_cls)
    )

    _, username = os.path.split(params.template_metadata_subdir)
    launch_body = params.flex_template_launch_body()
    template_gcs_path = params.template_gcs_path(params.project)
    cloudbuild_absolute_path = get_cloudbuild_path()
    template_absolute_path = get_template_path(params.flex_template_name)
    artifact_reg_image_path = (
        f"us-docker.pkg.dev/{params.project}/dataflow-dev/{username}/build:latest"
    )

    if not skip_build:
        submit_build_start = time.time()
        environment = get_environment_for_project(params.project)
        # Build and submit the image to "us-docker.pkg.dev/recidiviz-staging/dataflow-dev/{username}-build:latest"
        print(
            "Submitting build (this takes a few minutes, or longer on the first run).....\n"
        )
        run_command(
            f"""
                gcloud builds submit \
                --project={params.project} \
                --config {cloudbuild_absolute_path} \
                --substitutions=_IMAGE_PATH={artifact_reg_image_path},_GOOGLE_CLOUD_PROJECT={params.project},_RECIDIVIZ_ENV={environment.value}
            """,
            timeout_sec=900,
        )

        submit_build_exec_seconds = time.time() - submit_build_start
        build_minutes, build_seconds = divmod(submit_build_exec_seconds, 60)
        print(
            f"Submitted build in {build_minutes} minutes and {build_seconds} seconds.\n"
        )

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
    else:
        print("--skip_build is set... skipping build...")

    # Run the dataflow job
    pipeline_launch_body = json.dumps(launch_body, indent=2)
    print("Starting flex template job with body:")
    print(pipeline_launch_body)

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + fetch_google_auth_token(),
    }
    response = requests.post(
        f"https://dataflow.googleapis.com/v1b3/projects/{params.project}/locations/{params.region}/flexTemplates:launch",
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
