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
"""Helper functions for launching sandbox Dataflow pipelines from a local script, and
for waiting until they finish."""
import datetime
import glob
import json
import os
import time
from typing import Set, Type

import attr
import google.auth
import google.auth.transport.requests
import requests
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError

from recidiviz import pipelines
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.base_pipeline import BasePipeline
from recidiviz.pipelines.flex_pipeline_runner import pipeline_cls_for_pipeline_name
from recidiviz.pipelines.pipeline_parameters import PipelineParameters
from recidiviz.tools.utils.script_helpers import run_command
from recidiviz.utils.environment import get_environment_for_project
from recidiviz.utils.types import assert_type

# Values of the Dataflow API's JobState enum, which Google documents at
# https://docs.cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.jobs#Job.JobState
# That page marks five states as terminal: JOB_STATE_DONE, the only success, and the
# four below, which a job can end in without succeeding. The rest are states a job
# passes through on the way to one of those five.
_DATAFLOW_JOB_STATE_DONE = "JOB_STATE_DONE"
_DATAFLOW_JOB_STATES_ENDED_WITHOUT_SUCCESS = frozenset(
    {
        "JOB_STATE_FAILED",
        "JOB_STATE_CANCELLED",
        "JOB_STATE_DRAINED",
        "JOB_STATE_UPDATED",
    }
)
_DATAFLOW_JOB_POLL_INTERVAL_SECONDS = 60
# Stops the wait if a stage's jobs still aren't Done after this long, so a stuck job
# can't leave the script waiting forever. A judgment call, set well past how long a
# stage normally runs. The jobs themselves keep running in Dataflow.
_DATAFLOW_JOB_WAIT_LIMIT = datetime.timedelta(hours=6)
# Reading a job's state needs this permission; launching a job does not, so an account
# can start pipelines it then cannot watch.
_DATAFLOW_JOB_GET_PERMISSION = "dataflow.jobs.get"
_DATAFLOW_ACCESS_FIX = (
    f"the account needs the {_DATAFLOW_JOB_GET_PERMISSION} permission on the project. "
    "Ask the Data Platform team for access that includes it, then rerun."
)


@attr.define(frozen=True, kw_only=True)
class LaunchedDataflowJob:
    """A Dataflow job a sandbox script launched, as the launch call reported it."""

    job_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Dataflow's id for the job, which the jobs.get and jobs.cancel calls take."""

    job_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The name the pipeline parameters gave the job."""

    region: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The region the job runs in."""

    def console_url(self, project_id: str) -> str:
        """Returns the Dataflow console page for this job."""
        return (
            f"https://console.cloud.google.com/dataflow/jobs/{self.region}/"
            f"{self.job_id}?project={project_id}"
        )


def fetch_google_auth_token() -> str:
    creds, _ = google.auth.default()

    # creds.valid is False, and creds.token is None
    # Need to refresh credentials to populate those
    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)

    return creds.token  # type: ignore[attr-defined]


def get_upload_pipeline_docker_image_cloud_build_config_path() -> str:
    pipeline_root_path = os.path.dirname(pipelines.__file__)
    cloudbuild_path = "cloudbuild.pipelines.dev.yaml"

    return os.path.join(pipeline_root_path, cloudbuild_path)


def get_upload_pipeline_docker_image_cloud_build_ignores_path() -> str:
    """Returns the path to the ignores path for dataflow pipelines. Files ignored
    by this ignore file will not be copied to the Docker image as part of the build
    step.
    """
    pipeline_root_path = os.path.dirname(pipelines.__file__)
    cloudbuild_path = "cloudbuild.pipelines.dev.gcloudignore"

    return os.path.join(pipeline_root_path, cloudbuild_path)


def get_sandbox_pipeline_username() -> str:
    """Returns a normalized username that can be associated with a given sandbox
    pipeline run.
    """
    username = run_command("git config user.name", timeout_sec=300)
    username = username.replace(" ", "").strip().lower()
    if not username:
        raise ValueError("Found no configured git username")
    return username


def get_template_path(pipeline_type: str) -> str:
    """Returns the absolute path to the template_metadata.json for
    *pipeline_type*. Matches on the JSON ``name`` field or, as a fallback,
    the immediate parent directory name.
    """
    pipeline_root_path = os.path.dirname(pipelines.__file__)
    for candidate in glob.glob(
        os.path.join(pipeline_root_path, "**/template_metadata.json"), recursive=True
    ):
        dir_name = os.path.basename(os.path.dirname(candidate))
        if dir_name == pipeline_type:
            return candidate
        with open(candidate, encoding="utf-8") as f:
            if json.load(f).get("name") == pipeline_type:
                return candidate
    raise FileNotFoundError(
        f"No template_metadata.json found for pipeline_type={pipeline_type!r} "
        f"under {pipeline_root_path}"
    )


def get_all_reference_query_input_datasets_for_pipeline(
    pipeline_cls: Type[BasePipeline], state_code: StateCode
) -> Set[str]:
    """Returns all datasets that reference queries for this pipeline may query from when
    there are no special address overrides set.
    """
    return {
        parent_table.dataset_id
        for provider in pipeline_cls.all_input_reference_query_providers(
            state_code=state_code, address_overrides=None
        ).values()
        for parent_table in provider.parent_tables
    }


def sandbox_dataflow_docker_image_path(project_id: str, sandbox_username: str) -> str:
    """Returns the path to the Docker image for a sandbox Dataflow pipeline.

    Args:
        project_id: The project the pipeline is running in.
        sandbox_username: The username of the person running the sandbox pipeline.
    """
    return (
        f"us-docker.pkg.dev/{project_id}/dataflow-dev/{sandbox_username}/build:latest"
    )


def push_sandbox_dataflow_pipeline_docker_image(
    project_id: str, sandbox_username: str
) -> None:
    """Runs a Cloud Build job that builds and pushes a Dataflow pipline Docker image
    which incorporates local code changes.
    """
    cloud_build_config_path = get_upload_pipeline_docker_image_cloud_build_config_path()
    artifact_reg_image_path = sandbox_dataflow_docker_image_path(
        project_id=project_id, sandbox_username=sandbox_username
    )
    ignore_file_path = get_upload_pipeline_docker_image_cloud_build_ignores_path()

    submit_build_start = time.time()
    environment = get_environment_for_project(project_id)
    # Build and submit the image to "us-docker.pkg.dev/recidiviz-staging/dataflow-dev/{username}-build:latest"
    print(
        "Submitting build (this takes a few minutes, or longer on the first run). DO "
        "NOT SWITCH BRANCHES UNTIL THIS COMPLETES.....\n"
    )
    run_command(
        f"""
            gcloud builds submit \
            --project={project_id} \
            --config {cloud_build_config_path} \
            --ignore-file {ignore_file_path} \
            --substitutions=_IMAGE_PATH={artifact_reg_image_path},_GOOGLE_CLOUD_PROJECT={project_id},_RECIDIVIZ_ENV={environment.value}
        """,
        timeout_sec=900,
    )

    submit_build_exec_seconds = time.time() - submit_build_start
    build_minutes, build_seconds = divmod(submit_build_exec_seconds, 60)
    print(
        f"Submitted build in {build_minutes} minutes and {build_seconds} seconds. You "
        f"can switch branches now :-) \n"
    )


def run_sandbox_dataflow_pipeline(
    params: PipelineParameters, skip_build: bool
) -> LaunchedDataflowJob | None:
    """Launches the pipeline designated by the given |params| and returns the job
    Dataflow created for it, or None when Dataflow refuses the launch, after printing
    Dataflow's response."""
    if not params.is_sandbox_pipeline:
        raise ValueError(
            f"Parameters should only be for a sandbox pipeline (e.g. an "
            f"output_sandbox_prefix must be supplied). Found parameters: {params}."
        )

    pipeline_cls = pipeline_cls_for_pipeline_name(params.pipeline)
    params.check_for_valid_input_dataset_overrides(
        get_all_reference_query_input_datasets_for_pipeline(
            pipeline_cls, StateCode(params.state_code)
        )
    )

    if not skip_build:
        push_sandbox_dataflow_pipeline_docker_image(
            project_id=params.project,
            sandbox_username=assert_type(params.sandbox_username, str),
        )
    else:
        print("--skip_build is set... skipping build...")

    template_gcs_path = params.template_gcs_path(params.project)
    template_absolute_path = get_template_path(params.flex_template_name)
    artifact_reg_image_path = sandbox_dataflow_docker_image_path(
        project_id=params.project,
        sandbox_username=assert_type(params.sandbox_username, str),
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

    # Run the dataflow job
    pipeline_launch_body = json.dumps(params.flex_template_launch_body(), indent=2)
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

    if not response.ok:
        print("Job launch failed..")
        print(response.text)
        return None
    print(
        f"Job {params.job_name} successfully launched - go to https://console.cloud.google.com/dataflow/jobs?project={params.project} to monitor job progress"
    )
    return LaunchedDataflowJob(
        job_id=assert_type(response.json()["job"]["id"], str),
        job_name=params.job_name,
        region=params.region,
    )


def _current_dataflow_job_state(
    dataflow: Resource, *, project_id: str, job: LaunchedDataflowJob
) -> str:
    """Returns the job's current JobState value from the Dataflow API."""
    try:
        job_info = (
            dataflow.projects()
            .locations()
            .jobs()
            .get(projectId=project_id, location=job.region, jobId=job.job_id)
            # Retries 5xx responses and network errors with backoff, so one blip
            # during an hours-long wait doesn't stop the run while its jobs keep going.
            .execute(num_retries=5)
        )
    except HttpError as e:
        if e.resp.status == 403:
            raise RuntimeError(
                f"Cannot read Dataflow job [{job.job_name}] in [{project_id}]: "
                f"{_DATAFLOW_ACCESS_FIX} Dataflow said: [{e}]"
            ) from e
        raise
    return assert_type(job_info["currentState"], str)


def check_can_poll_sandbox_dataflow_jobs(project_id: str) -> None:
    """Raises unless the account can read Dataflow jobs in the project. Waiting for
    sandbox pipelines needs that, and launching them does not, so a run that cannot
    read them would otherwise fail only once its first pipeline is already running."""
    credentials, _ = google.auth.default()
    resource_manager = build(
        "cloudresourcemanager", "v1", credentials=credentials, cache_discovery=False
    )
    response = (
        resource_manager.projects()
        .testIamPermissions(
            resource=project_id, body={"permissions": [_DATAFLOW_JOB_GET_PERMISSION]}
        )
        .execute()
    )
    # The response omits the key entirely when none of the permissions are held.
    if _DATAFLOW_JOB_GET_PERMISSION not in response.get("permissions", []):
        raise RuntimeError(
            f"Cannot read Dataflow jobs in [{project_id}]: {_DATAFLOW_ACCESS_FIX}"
        )


def wait_for_sandbox_dataflow_jobs(
    *, project_id: str, jobs: list[LaunchedDataflowJob]
) -> None:
    """Returns once every job has succeeded, polling the Dataflow API once a minute and
    printing each job's state as it changes. Raises when a job ends without succeeding,
    when the account cannot read the jobs, or when the jobs are still running after
    the wait limit."""
    credentials, _ = google.auth.default()
    dataflow = build("dataflow", "v1b3", credentials=credentials, cache_discovery=False)
    states_by_job_id: dict[str, str | None] = {job.job_id: None for job in jobs}
    deadline = time.time() + _DATAFLOW_JOB_WAIT_LIMIT.total_seconds()
    while True:
        for job in jobs:
            if states_by_job_id[job.job_id] == _DATAFLOW_JOB_STATE_DONE:
                continue
            state = _current_dataflow_job_state(
                dataflow, project_id=project_id, job=job
            )
            if state != states_by_job_id[job.job_id]:
                print(f"Job {job.job_name}: {state}")
                states_by_job_id[job.job_id] = state
            if state in _DATAFLOW_JOB_STATES_ENDED_WITHOUT_SUCCESS:
                raise RuntimeError(
                    f"Dataflow job [{job.job_name}] ended in state [{state}]. See "
                    f"{job.console_url(project_id)}"
                )
        if all(
            state == _DATAFLOW_JOB_STATE_DONE for state in states_by_job_id.values()
        ):
            return
        if time.time() > deadline:
            still_running = sorted(
                job.job_name
                for job in jobs
                if states_by_job_id[job.job_id] != _DATAFLOW_JOB_STATE_DONE
            )
            raise RuntimeError(
                f"Dataflow jobs {still_running} are still running after "
                f"{_DATAFLOW_JOB_WAIT_LIMIT}. Check them in the console at "
                f"https://console.cloud.google.com/dataflow/jobs?project={project_id}"
            )
        time.sleep(_DATAFLOW_JOB_POLL_INTERVAL_SECONDS)
