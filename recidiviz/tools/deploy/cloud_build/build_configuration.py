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
from argparse import Namespace

import attr
from google.cloud.devtools.cloudbuild_v1 import (
    Build,
    BuildOptions,
    BuildStep,
    RepoSource,
    SecretManagerSecret,
    Secrets,
    Source,
)

from recidiviz.common.google_cloud.protobuf_builder import ProtoPlusBuilder
from recidiviz.utils.secrets import get_secret
from recidiviz.utils.types import assert_type


@attr.define
class DeploymentContext:
    project_id: str
    commit_ref: str
    version_tag: str

    @classmethod
    def build_from_argparse(cls, args: Namespace) -> "DeploymentContext":
        return cls(
            project_id=args.project_id,
            commit_ref=args.commit_ref,
            version_tag=args.version_tag,
        )


@attr.define
class BuildConfiguration:
    """Simplified entrypoint into `cloud_buildv1.Build` for use with `create_deployment_build_api_obj` factory"""

    steps: list[BuildStep]
    # It is possible to use Secret Manager secret values in the build by using substitutions
    # Map of substitution names -> secrets
    # example: BuildConfiguration(
    #   secrets={"_SLACK_BOT_TOKEN": "deploy_slack_bot_authorization_token"},
    #   steps=[BuildStep(command="echo ${_SLACK_BOT_TOKEN}")]
    # )
    secrets: dict[str, str] = attr.ib(factory=dict)
    # If set to true, Cloud Build will do a shallow checkout of the specified commit ref prior to running the build
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


def create_deployment_build_api_obj(
    build_configuration: BuildConfiguration, deployment_context: DeploymentContext
) -> Build:
    """Creates a Cloud Build gRPC API object given a deployment context and build configuration"""
    builder = ProtoPlusBuilder(Build).compose(
        Build(
            {
                # TODO(#31461): Create the service account + IAM permissions in Terraform
                "service_account": get_secret("ci_cd_service_account"),
                "logs_bucket": "gs://${_PROJECT_ID}-ci-cd-logs",
                "timeout": f"{build_configuration.timeout_seconds}s",
                # The default list of substitutions includes PROJECT_ID, BUILD_ID, LOCATION
                # https://cloud.google.com/build/docs/configuring-builds/substitute-variable-values
                "substitutions": {
                    "_PROJECT_ID": deployment_context.project_id,
                    "_COMMIT_REF": deployment_context.commit_ref,
                    "_VERSION_TAG": deployment_context.version_tag,
                },
                "source": Source(
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
                "options": {"substitution_option": "ALLOW_LOOSE"},
            }
        )
    )
    builder.update_args(
        steps=build_configuration.steps,
        options=BuildOptions(machine_type=build_configuration.machine_type),
    )

    available_secrets = Secrets()
    for (
        secret_substitution_name,
        secret_name,
    ) in build_configuration.secrets.items():
        available_secrets.secret_manager.append(
            SecretManagerSecret(
                version_name=f"projects/$_PROJECT_ID/secrets/{secret_name}/versions/latest",
                env=secret_substitution_name,
            )
        )
    builder.update_args(available_secrets=available_secrets)
    return builder.build()


def build_step_for_shell_command(
    command: str,
    *,
    id_: str,
    name: str,
    wait_for: list[str] | str | None = None,
    env: list[str] | None = None,
) -> BuildStep:
    """Helper function to create a BuildStep that runs a shell command"""
    return BuildStep(
        entrypoint="sh",
        args=["-c", command],
        id=id_,
        name=name,
        wait_for=wait_for,
        env=env,
    )