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
"""Module containing configuration objects for deployments"""
import datetime
from argparse import Namespace

import attr
from google.cloud.devtools.cloudbuild_v1 import (
    Artifacts,
    Build,
    BuildOptions,
    BuildStep,
    RepoSource,
    SecretManagerSecret,
    Secrets,
    Source,
    Volume,
)

from recidiviz.common.google_cloud.protobuf_builder import ProtoPlusBuilder
from recidiviz.tools.deploy.cloud_build.constants import (
    BUILDER_GCLOUD,
    RECIDIVIZ_SOURCE_VOLUME,
)
from recidiviz.utils.secrets import get_secret
from recidiviz.utils.types import assert_type


@attr.define
class DeploymentContext:
    project_id: str
    commit_ref: str
    version_tag: str
    stage: str

    @classmethod
    def build_from_argparse(cls, args: Namespace) -> "DeploymentContext":
        return cls(
            project_id=args.project_id,
            commit_ref=args.commit_ref,
            version_tag=args.version_tag,
            stage=args.stage,
        )

    @property
    def app_engine_tag(self) -> str:
        # Replace characters that are not allowed in app engine version names with hyphens
        return self.version_tag.replace(".", "-")


@attr.define
class BuildConfiguration:
    """Simplified entrypoint into `cloud_buildv1.Build` for use with `create_deployment_build_api_obj` factory"""

    steps: list[BuildStep]
    # It is possible to use Secret Manager secret values in the build by using substitutions
    # example: BuildConfiguration(
    #   secrets=["deploy_slack_bot_authorization_token"],
    #   steps=[BuildStep(
    #       command=f"echo {secret_substitution('deploy_slack_bot_authorization_token')}",
    #       secret_env=[secret_substitution_name("deploy_slack_bot_authorization_token")],
    #   )]
    # )
    secrets: list[str] = attr.ib(factory=dict)
    # If set to true, Cloud Build will do a shallow checkout of the specified commit ref prior to running the build
    # The repository is cloned into the /workspace/ directory
    uses_source: bool = attr.ib(default=False)
    # Cloud Build will cancel the build if it does not complete within the specified timeframe
    timeout_seconds: int = attr.ib(
        default=1800,
    )
    # Specifies the kind of machine to run the build on
    machine_type: BuildOptions.MachineType = attr.ib(
        default=assert_type(
            BuildOptions.MachineType.UNSPECIFIED,
            BuildOptions.MachineType,
        ),
    )

    object_artifacts: Artifacts.ArtifactObjects | None = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        for step in self.steps:
            if step.timeout is None:
                continue

            step_timeout_seconds = assert_type(
                step.timeout, datetime.timedelta
            ).total_seconds()

            if step_timeout_seconds > self.timeout_seconds:
                raise ValueError(
                    f"Found step [{step.id}] with timeout [{step_timeout_seconds}] "
                    f"seconds which is greater than the overall build timeout of "
                    f"[{self.timeout_seconds}] seconds. If any individual build step "
                    f"has a timeout defined, it must be less than or equal to the "
                    f"overall build timeout."
                )


def create_deployment_build_api_obj(
    build_configuration: BuildConfiguration, deployment_context: DeploymentContext
) -> Build:
    """Creates a Cloud Build gRPC API object given a deployment context and build configuration"""
    builder = ProtoPlusBuilder(Build).compose(
        Build(
            service_account=get_secret("ci_cd_service_account"),
            logs_bucket="gs://${_PROJECT_ID}-ci-cd-logs",
            timeout=f"{build_configuration.timeout_seconds}s",
            # The default list of substitutions includes PROJECT_ID, BUILD_ID, LOCATION
            # https://cloud.google.com/build/docs/configuring-builds/substitute-variable-values
            substitutions={
                "_PROJECT_ID": deployment_context.project_id,
                "_COMMIT_REF": deployment_context.commit_ref,
                "_VERSION_TAG": deployment_context.version_tag,
            },
            source=Source(
                repo_source=RepoSource(
                    project_id=deployment_context.project_id,
                    repo_name="github_Recidiviz_pulse-data",
                    commit_sha=deployment_context.commit_ref,
                )
            )
            if build_configuration.uses_source
            else None,
            # We want to expose substitutions that are consistently available across all build steps, even if they
            # are unused. Enable ALLOW_LOOSE
            # > If the ALLOW_LOOSE option is not specified, unmatched keys in the substitutions mapping or build
            # > request will result in error.
            options={"substitution_option": "ALLOW_LOOSE"},
            artifacts=Artifacts(objects=build_configuration.object_artifacts),
            tags=[
                # Tags are used to filter builds in the Cloud Build UI
                deployment_context.version_tag,
                deployment_context.commit_ref,
                deployment_context.stage,
            ],
        )
    )

    additional_steps = [
        # Without this step, read/write access to /workspace/ would be forbidden for non-root users
        # https://cloud.google.com/build/docs/troubleshooting#timeout_issues_when_pulling_images_from_docker_registry
        BuildStep(
            name="gcr.io/cloud-builders/docker",
            id="Give non-root users access to /workspace/ volume",
            args=["a+w", "/workspace"],
            entrypoint="chmod",
        ),
    ]

    if build_configuration.uses_source:
        additional_steps.append(
            # This is effectively a no-op for deployment runs as the checked out source will be the same
            # as what is bundled for the image.
            # For Pull Requests / Continuous Delivery, this will allow us to use not-yet deployed code in the build
            build_step_for_shell_command(
                id_="Copy Git source to shared volume",
                name=BUILDER_GCLOUD,
                # Keep file metadata (such as modified time) when copying files by using -p
                command="cp -pr /workspace/recidiviz/* /app/recidiviz",
                volumes=[RECIDIVIZ_SOURCE_VOLUME],
            )
        )

    builder.update_args(
        steps=[
            *additional_steps,
            *build_configuration.steps,
        ],
        options=BuildOptions(machine_type=build_configuration.machine_type),
    )

    available_secrets = Secrets()
    for secret_name in build_configuration.secrets:
        available_secrets.secret_manager.append(
            SecretManagerSecret(
                version_name=f"projects/$_PROJECT_ID/secrets/{secret_name}/versions/latest",
                env=secret_substitution_name(secret_name),
            )
        )
    builder.update_args(available_secrets=available_secrets)
    return builder.build()


def secret_substitution_name(secret_name: str) -> str:
    return f"_{secret_name.upper()}"


def secret_substitution(secret_name: str) -> str:
    return f"$${secret_substitution_name(secret_name)}"


def build_step_for_shell_command(
    command: str,
    *,
    id_: str,
    name: str,
    wait_for: list[str] | str | None = None,
    env: list[str] | None = None,
    dir_: str | None = None,
    secret_env: list[str] | None = None,
    volumes: list[Volume] | None = None,
    timeout_seconds: int | None = None,
) -> BuildStep:
    """Helper function to create a BuildStep that runs a shell command"""
    return BuildStep(
        entrypoint="sh",
        args=["-c", command],
        id=id_,
        dir_=dir_,
        name=name,
        wait_for=wait_for,
        env=env,
        secret_env=secret_env,
        volumes=volumes,
        timeout=f"{timeout_seconds}s" if timeout_seconds is not None else None,
    )


def build_step_for_gcloud_command(
    args: list[str],
    *,
    id_: str,
    wait_for: list[str] | str | None = None,
    timeout_seconds: int | None = None,
) -> BuildStep:
    """Helper function to create a BuildStep that runs a gcloud command with our default arguments"""
    return BuildStep(
        id=id_,
        name=BUILDER_GCLOUD,
        entrypoint="gcloud",
        args=["--quiet", "--verbosity=debug", *args],
        wait_for=wait_for,
        timeout=f"{timeout_seconds}s" if timeout_seconds is not None else None,
    )
