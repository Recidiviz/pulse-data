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
"""Build configuration for building Docker images"""
import argparse

from google.cloud.devtools.cloudbuild_v1 import BuildOptions, BuildStep

from recidiviz.tools.deploy.cloud_build.artifact_registry_repository import (
    ArtifactRegistryDockerImageRepository,
    ImageKind,
)
from recidiviz.tools.deploy.cloud_build.build_configuration import (
    BuildConfiguration,
    DeploymentContext,
    build_step_for_shell_command,
)
from recidiviz.tools.deploy.cloud_build.constants import (
    BUILDER_ALPINE,
    BUILDER_DOCKER,
    DOCKER_CREDENTIAL_HELPER,
    IMAGE_DOCKERFILES,
)
from recidiviz.tools.deploy.cloud_build.deployment_stage_interface import (
    DeploymentStageInterface,
)
from recidiviz.utils.params import str_to_list
from recidiviz.utils.types import assert_type

DOCKER_CREDENTIAL_PATH = "/workspace/gcloud"

# Downloads the docker-credential-gcr binary which configures a Docker credential helper that allows us to
# authenticate with Artifact Registry for pulling / pushing images
_DOWNLOAD_DOCKER_CREDENTIAL_BUILD_STEP = build_step_for_shell_command(
    name=BUILDER_ALPINE,
    id_="download-docker-credential",
    command=(
        # Download the docker-credential-gcr release
        f'wget -O docker-credential-gcr.tar.gz "{DOCKER_CREDENTIAL_HELPER}" '
        # Extract the docker-credential-gcr binary
        "&& tar xz -f docker-credential-gcr.tar.gz docker-credential-gcr "
        # Make it executable
        "&& chmod +x docker-credential-gcr "
        # Move it to the build's shared `/workspace` volume
        f"&& mkdir /workspace/gcloud && mv docker-credential-gcr {DOCKER_CREDENTIAL_PATH}"
    ),
)


def _generate_build_image_build_step(
    deployment_context: DeploymentContext,
    repository: ArtifactRegistryDockerImageRepository,
    promote: bool,
) -> list[BuildStep]:
    """Generates a build step that builds and pushes docker images to Artifact Registry"""
    dockerfile, build_stage = IMAGE_DOCKERFILES[repository.image_kind]
    builder_name = repository.repository_id
    tag = repository.version_url(version_tag=deployment_context.version_tag)
    cache_repo = repository.build_url(commit_ref="cache")
    build_and_push_image_command = (
        # Add the docker-credential-gcr binary to `PATH` so that `docker` can find it when authenticating
        f'export PATH="{DOCKER_CREDENTIAL_PATH}:$${{PATH}}" '
        # Add the `docker-credential-gcr` entry for `us-docker.pkg.dev` to `.docker/config.json`
        f"&& /workspace/gcloud/docker-credential-gcr configure-docker --registries={repository.host_name} "
        # Build the image using BuildKit
        f"&& docker buildx build . -f {dockerfile} "
        f"--builder {builder_name} "
        # Tag to store the image to
        f"--tag {tag} "
        # Push cache layers
        f"--cache-to type=registry,ref={cache_repo},mode=max "
        # Fetch cache layers
        f"--cache-from type=registry,ref={cache_repo} "
        # Push images to registry
        "--push "
    )
    if build_stage:
        # Target the specified mult-stage build stage when applicable
        build_and_push_image_command += f"--target={build_stage} "

    if promote:
        build_and_push_image_command += f"--tag {repository.latest_url()}"

    # Create a new build context that can build images in parallel with other build contexts
    create_build_context = BuildStep(
        name=BUILDER_DOCKER,
        args=["buildx", "create", "--name", builder_name],
        id=f"create-build-context-{repository.repository_id}",
        wait_for=[_DOWNLOAD_DOCKER_CREDENTIAL_BUILD_STEP.id],
    )

    return [
        create_build_context,
        build_step_for_shell_command(
            name=BUILDER_DOCKER,
            env=["DOCKER_BUILDKIT=1"],
            id_=f"build-{repository.repository_id}",
            wait_for=[create_build_context.id],
            command=build_and_push_image_command,
        ),
    ]


class BuildImages(DeploymentStageInterface):
    """Deployment stage for building images and pushing to Artifact Registry"""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        image_kinds = ", ".join(list(ImageKind))
        parser.add_argument(
            "--images",
            type=lambda images: [ImageKind(image) for image in str_to_list(images)],
            help=f"Comma delimited string of ImageKind values. Choices: {image_kinds}",
        )

        parser.add_argument(
            "--promote",
            action=argparse.BooleanOptionalAction,
            default=False,
            help="If specified, tags the images as :latest. --no-promote also accepted for the inverse",
        )

        return parser

    def configure_build(
        self,
        deployment_context: DeploymentContext,
        args: argparse.Namespace,
    ) -> BuildConfiguration:
        """Generates a configuration for building Docker images and uploading them to Artifact Registry"""

        repositories = ArtifactRegistryDockerImageRepository.from_file()
        build_steps = [
            _DOWNLOAD_DOCKER_CREDENTIAL_BUILD_STEP,
        ]

        for image in args.images:
            if image not in repositories:
                raise ValueError(
                    f"Cannot find image {image} in list of Docker image repositories"
                )

            repository = repositories[image]
            build_steps.extend(
                _generate_build_image_build_step(
                    deployment_context=deployment_context,
                    repository=repository,
                    promote=args.promote,
                )
            )

        return BuildConfiguration(
            steps=build_steps,
            uses_source=True,
            # Building Docker images is resource-intensive and done in parallel.
            # Use multiple CPUs for this step
            machine_type=assert_type(
                BuildOptions.MachineType.E2_HIGHCPU_8,
                BuildOptions.MachineType,
            ),
        )
