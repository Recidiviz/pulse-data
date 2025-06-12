# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
# pylint: disable=anomalous-backslash-in-string
"""Build configuration for creating Terraform plans

This deployment stage can also be used to generate the build configuration for our Terraform plan PR commenter by running
https://console.cloud.google.com/cloud-build/triggers;region=us-west1/edit/d9469072-4bbc-45fe-97d4-45b4a708568c?project=recidiviz-staging

python -m recidiviz.tools.deploy.cloud_build.deployment_stage_runner \
    --project-id recidiviz-staging \
    --commit-ref "\$_COMMIT_REF" \
    --version-tag "latest" \
    --dry-run \
    --stage CreateTerraformPlan \
    --for-pull-requests
"""
import argparse

from google.cloud.devtools.cloudbuild_v1 import Artifacts, BuildOptions, BuildStep

from recidiviz.tools.airflow.utils import get_environment_by_name
from recidiviz.tools.deploy.cloud_build.artifact_registry_repository import (
    ArtifactRegistryDockerImageRepository,
    ImageKind,
)
from recidiviz.tools.deploy.cloud_build.build_configuration import (
    BuildConfiguration,
    DeploymentContext,
    build_step_for_shell_command,
    secret_substitution_name,
)
from recidiviz.tools.deploy.cloud_build.constants import (
    BUILDER_GCLOUD,
    BUILDER_GIT,
    BUILDER_TERRAFORM,
    RECIDIVIZ_SOURCE_VOLUME,
    TERRAFORM_WORKDIR,
)
from recidiviz.tools.deploy.cloud_build.deployment_stage_interface import (
    DeploymentStageInterface,
)
from recidiviz.tools.gsutil_shell_helpers import gcloud_storage_rsync_airflow_command
from recidiviz.utils.secrets import get_secret
from recidiviz.utils.types import assert_type

AIRFLOW_SOURCE_FILES_DIR = "/workspace/airflow_source_files"

PAGERDUTY_SECRET_NAME = "pagerduty_terraform_key"  # nosec


# Disables TTY input, disables colored output
TERRAFORM_CLI_ARGS_ENV = "TF_CLI_ARGS=-input=false -no-color -compact-warnings"

STEP_SHOW_TERRAFORM_PLAN = "Show Terraform plan contents"


def plan_file_name_for_deployment(deployment_context: DeploymentContext) -> str:
    return f"{deployment_context.app_engine_tag}-{deployment_context.commit_ref}.tfplan"


def get_terraform_plan_step(
    deployment_context: DeploymentContext, output_path: str, wait_for: list[str]
) -> BuildStep:
    terraform_variables = {
        "project_id": deployment_context.project_id,
        "docker_image_tag": deployment_context.version_tag,
        "git_hash": deployment_context.commit_ref,
        "pagerduty_token": get_secret(PAGERDUTY_SECRET_NAME),
    }
    plan_args = [
        "plan",
        "-parallelism=32",
        f"-out={output_path}",
    ]

    for name, value in terraform_variables.items():
        plan_args.append(f"-var={name}={value}")

    return BuildStep(
        id="Create Terraform plan",
        name=BUILDER_TERRAFORM,
        dir_=TERRAFORM_WORKDIR,
        args=plan_args,
        env=[TERRAFORM_CLI_ARGS_ENV],
        secret_env=[secret_substitution_name(PAGERDUTY_SECRET_NAME)],
        wait_for=wait_for,
        timeout="900s",  # 15 min timeout
    )


def _get_airflow_file_sync_steps(
    deployment_context: DeploymentContext, app_engine_image: str
) -> list[BuildStep]:
    """Returns build steps for updating the Airflow source files"""
    create_airflow_source_manifest = build_step_for_shell_command(
        id_="Create Airflow source manifest",
        name=app_engine_image,
        dir_="/app/",
        command=(
            "pipenv run python -m recidiviz.tools.airflow.get_airflow_source_files "
            f"--output-path {AIRFLOW_SOURCE_FILES_DIR} "
            "--dry-run False"
        ),
        volumes=[RECIDIVIZ_SOURCE_VOLUME],
        env=["HOME=/home/recidiviz"],
        timeout_seconds=(1 * 60),  # 15 min timeout
    )

    orchestration = get_environment_by_name(
        project_id=deployment_context.project_id,
        name="orchestration-v2",
    )

    copy_airflow_source_files = build_step_for_shell_command(
        command=" ".join(
            gcloud_storage_rsync_airflow_command(
                AIRFLOW_SOURCE_FILES_DIR,
                orchestration.config.dag_gcs_prefix,
                use_gsutil=True,
            )
        ),
        id_="Sync Airflow source files to GCS",
        name=BUILDER_GCLOUD,
        volumes=[RECIDIVIZ_SOURCE_VOLUME],
        wait_for=[create_airflow_source_manifest.id],
        timeout_seconds=(1 * 60),  # 1 min timeout
    )

    return [create_airflow_source_manifest, copy_airflow_source_files]


class CreateTerraformPlan(DeploymentStageInterface):
    """Deployment stage for Terraform"""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--apply",
            action="store_true",
            help="If specified, the plan will be applied",
            default=False,
        )
        parser.add_argument(
            "--for-pull-requests",
            action="store_true",
            default=False,
            help="If specified, creates a BuildConfiguration for planning pull requests and commenting PRs",
        )
        return parser

    def configure_build(
        self,
        deployment_context: DeploymentContext,
        args: argparse.Namespace,
    ) -> BuildConfiguration:
        """Build configration for creating, applying, and previewing terraform plans"""
        if args.apply and args.for_pull_requests:
            raise RuntimeError(
                "Cannot specify both `--apply` and `--for-pull-requests`"
            )

        app_engine = ArtifactRegistryDockerImageRepository.from_file()[
            ImageKind.APP_ENGINE
        ]
        app_engine_image = app_engine.version_url(
            version_tag=deployment_context.version_tag
        )
        plan_path = f"/workspace/{plan_file_name_for_deployment(deployment_context)}"

        terraform_init = BuildStep(
            id="Initialize Terraform backend",
            name=BUILDER_TERRAFORM,
            dir_=TERRAFORM_WORKDIR,
            args=[
                "init",
                "-backend-config",
                f"bucket={deployment_context.project_id}-tf-state",
                "-reconfigure",
            ],
            env=[TERRAFORM_CLI_ARGS_ENV],
            timeout="900s",  # 15 min timeout
        )

        terraform_plan = get_terraform_plan_step(
            deployment_context=deployment_context,
            output_path=plan_path,
            wait_for=[terraform_init.id],
        )

        build_steps = [
            build_step_for_shell_command(
                id_="Prefetch the builder image",
                name=BUILDER_TERRAFORM,
                command='echo "Downloaded latest image!"',
            ),
            terraform_init,
            terraform_plan,
        ]

        if args.apply:
            build_steps.extend(
                [
                    *_get_airflow_file_sync_steps(
                        deployment_context=deployment_context,
                        app_engine_image=app_engine_image,
                    ),
                    BuildStep(
                        id="Apply Terraform plan",
                        name=BUILDER_TERRAFORM,
                        dir_=TERRAFORM_WORKDIR,
                        args=["apply", "-parallelism=32", plan_path],
                        env=[TERRAFORM_CLI_ARGS_ENV],
                        secret_env=[secret_substitution_name(PAGERDUTY_SECRET_NAME)],
                        wait_for=[terraform_plan.id],
                        # No timeout for this step - this could take a long time for certain
                        # upgrades, e.g. upgrades to Cloud Composer versions.
                    ),
                ]
            )

        if args.for_pull_requests:
            build_steps.insert(
                0,
                build_step_for_shell_command(
                    command="git fetch origin $_COMMIT_REF && git checkout $_COMMIT_REF",
                    id_="Fetch webhook-specified commit",
                    name=BUILDER_GIT,
                    timeout_seconds=(15 * 60),  # 15 min timeout
                ),
            )

            build_steps.extend(
                [
                    BuildStep(
                        id=STEP_SHOW_TERRAFORM_PLAN,
                        name=BUILDER_TERRAFORM,
                        dir_=TERRAFORM_WORKDIR,
                        entrypoint="sh",
                        args=[
                            "-c",
                            # The Cloud Build UI and Github UI do not support ANSI color escapes, so we use `-no-color`
                            f"terraform show -no-color {plan_path} > /workspace/terraform-plan-output.txt",
                        ],
                        wait_for=[terraform_plan.id],
                    ),
                    build_step_for_shell_command(
                        id_="Comment plan on Pull Request",
                        name=app_engine_image,
                        dir_="/app/",
                        # https://cloud.google.com/build/docs/configuring-builds/substitute-variable-values#using_default_substitutions
                        command=(
                            "pipenv run python -m recidiviz.tools.github.upsert_terraform_plan "
                            "--pull-request-number $_PR_NUMBER "
                            f"--commit-ref {deployment_context.commit_ref} "
                            "--terraform-plan-output-path /workspace/terraform-plan-output.txt "
                        ),
                        volumes=[RECIDIVIZ_SOURCE_VOLUME],
                        wait_for=[STEP_SHOW_TERRAFORM_PLAN],
                        timeout_seconds=(15 * 60),  # 15 min timeout
                    ),
                ]
            )

        return BuildConfiguration(
            steps=build_steps,
            uses_source=True,
            # 4 hour timeout for the overall job - we expect most runs to take much less
            # time than this, but some infrastructure updates may take a long time and
            # we don't want to fail those in the middle.
            timeout_seconds=4 * 60 * 60,
            secrets=[PAGERDUTY_SECRET_NAME],
            machine_type=assert_type(
                BuildOptions.MachineType.E2_HIGHCPU_32,
                BuildOptions.MachineType,
            ),
            object_artifacts=(
                Artifacts.ArtifactObjects(
                    location=f"gs://{deployment_context.project_id}-tf-state/tf-plans",
                    paths=[plan_path],
                )
                if not args.for_pull_requests
                else None
            ),
        )
