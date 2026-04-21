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
    IMAGE_BUILD_DEPENDENCIES,
    IMAGE_BUILD_PLATFORMS,
    IMAGE_DOCKERFILES,
    PLATFORM_LINUX_ARM64,
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


# Cloud Build workers are amd64 machines, so they can't natively execute arm64
# instructions. QEMU is an emulator that translates arm64 instructions to amd64,
# allowing Docker to build arm64 images on amd64 hardware. This step registers
# QEMU as a "binary format handler" in the Linux kernel so that arm64 binaries
# are transparently routed through QEMU during `docker buildx` multi-platform
# builds. Must run before any docker buildx build commands that build for arm64.
_SETUP_QEMU_BUILD_STEP = BuildStep(
    name=BUILDER_DOCKER,
    id="setup-qemu",
    args=["run", "--privileged", "--rm", "tonistiigi/binfmt", "--install", "arm64"],
)


def _build_step_id_for_repository(
    repository: ArtifactRegistryDockerImageRepository,
) -> str:
    """Step id for the build-and-push step for the given image repository."""
    return f"build-{repository.repository_id}"


def _generate_build_image_build_step(
    deployment_context: DeploymentContext,
    repository: ArtifactRegistryDockerImageRepository,
    extra_build_wait_for_steps: list[str],
    cache_scope_key: str,
) -> list[BuildStep]:
    """Generates a build step that builds and pushes docker images to Artifact Registry.

    extra_build_wait_for_steps is a list of step ids that the build-and-push
    step should wait on in addition to its own create-build-context step — used
    to serialize cross-image Dockerfile FROM dependencies (see
    IMAGE_BUILD_DEPENDENCIES).

    cache_scope_key is the (pre-sanitization) scope for the BuildKit registry
    cache tag. For GitHub-triggered builds, pass "$BRANCH_NAME" so each branch
    gets its own `cache-<branch>` tag and does not thrash other branches'
    caches. The value is sanitized to Docker-tag-safe chars at build time.
    `cache-main` is always included as a `--cache-from` fallback so branches
    cut from main get a warm first build.
    """
    dockerfile, build_stage = IMAGE_DOCKERFILES[repository.image_kind]
    builder_name = repository.repository_id
    tag = repository.build_url(tag=deployment_context.commit_ref)
    scoped_cache_url = repository.build_cache_url("$${SAFE_CACHE_SCOPE_KEY}")
    main_cache_url = repository.build_cache_url("main")
    platforms = IMAGE_BUILD_PLATFORMS[repository.image_kind]
    needs_arm64 = PLATFORM_LINUX_ARM64 in platforms

    build_and_push_image_command = (
        # Add the docker-credential-gcr binary to `PATH` so that `docker` can find it when authenticating
        f'export PATH="{DOCKER_CREDENTIAL_PATH}:$${{PATH}}" '
        # Add the `docker-credential-gcr` entry for `us-docker.pkg.dev` to `.docker/config.json`
        f"&& /workspace/gcloud/docker-credential-gcr configure-docker --registries={repository.host_name} "
        # Derive a Docker-tag-safe scope key at build time; '/' and '.' are not
        # valid in tag names, so translate both to '-' (e.g. "releases/v1.676-rc"
        # → "releases-v1-676-rc"). CACHE_SCOPE_KEY is sourced from the step's
        # `env` (see build_step_for_shell_command call below) rather than
        # interpolated directly into the shell command, so that special chars
        # in the value cannot escape the quoting and execute arbitrary commands.
        '&& SAFE_CACHE_SCOPE_KEY=$$(echo "$$CACHE_SCOPE_KEY" | tr "/." "--") '
        # Build the image using BuildKit
        f"&& docker buildx build . -f {dockerfile} "
        f"--builder {builder_name} "
        # Tag to store the image to
        f"--tag={tag} "
        # Push cache layers to the scope-specific tag only, so branches don't
        # overwrite each other's cache manifest.
        f"--cache-to type=registry,ref={scoped_cache_url},mode=max "
        # Fetch cache layers from the scope-specific tag, with cache-main as a
        # fallback so branches cut from main get a warm first build.
        f"--cache-from type=registry,ref={scoped_cache_url} "
        f"--cache-from type=registry,ref={main_cache_url} "
        # Push images to registry
        "--push "
        # Target platforms
        f"--platform={','.join(platforms)} "
    )
    if build_stage:
        # Target the specified multi-stage build stage when applicable
        build_and_push_image_command += f"--target={build_stage} "

    wait_for_steps = [_DOWNLOAD_DOCKER_CREDENTIAL_BUILD_STEP.id]
    if needs_arm64:
        wait_for_steps.append(_SETUP_QEMU_BUILD_STEP.id)

    # Create a new build context that can build images in parallel with other build contexts
    create_build_context = BuildStep(
        name=BUILDER_DOCKER,
        args=["buildx", "create", "--name", builder_name],
        id=f"create-build-context-{repository.repository_id}",
        wait_for=wait_for_steps,
    )

    return [
        create_build_context,
        build_step_for_shell_command(
            name=BUILDER_DOCKER,
            # Pass cache_scope_key via env rather than string-interpolating into
            # the shell command: Cloud Build resolves `$BRANCH_NAME` into the
            # env value at step setup, and bash expansion of that env var
            # inside double quotes is not re-parsed for shell metacharacters,
            # so a malicious branch name cannot break out of the quoting.
            env=["DOCKER_BUILDKIT=1", f"CACHE_SCOPE_KEY={cache_scope_key}"],
            id_=_build_step_id_for_repository(repository),
            wait_for=[create_build_context.id, *extra_build_wait_for_steps],
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
            "--cache-scope-key",
            required=True,
            help=(
                "Key used to scope the Docker build cache tag; sanitized to "
                "Docker-tag-safe characters at build time. Pass '$BRANCH_NAME' "
                "for GitHub-triggered builds so each branch gets its own cache."
            ),
        )

        return parser

    def configure_build(
        self,
        deployment_context: DeploymentContext,
        args: argparse.Namespace,
    ) -> BuildConfiguration:
        """Generates a configuration for building Docker images and uploading them to Artifact Registry"""

        repositories = ArtifactRegistryDockerImageRepository.from_file(
            deployment_context
        )
        needs_qemu = any(
            PLATFORM_LINUX_ARM64 in IMAGE_BUILD_PLATFORMS[image]
            for image in args.images
        )

        build_steps: list[BuildStep] = [
            _DOWNLOAD_DOCKER_CREDENTIAL_BUILD_STEP,
        ]
        if needs_qemu:
            build_steps.append(_SETUP_QEMU_BUILD_STEP)

        images_set = set(args.images)
        for image in args.images:
            if image not in repositories:
                raise ValueError(
                    f"Cannot find image {image} in list of Docker image repositories"
                )

            repository = repositories[image]
            extra_build_wait_for_steps: list[str] = [
                _build_step_id_for_repository(repositories[dep])
                for dep in IMAGE_BUILD_DEPENDENCIES.get(image, [])
                if dep in images_set
            ]
            build_steps.extend(
                _generate_build_image_build_step(
                    deployment_context=deployment_context,
                    repository=repository,
                    extra_build_wait_for_steps=extra_build_wait_for_steps,
                    cache_scope_key=args.cache_scope_key,
                )
            )

        # Builds with QEMU emulation are 2-4x slower, so use more CPUs.
        machine_type = (
            BuildOptions.MachineType.E2_HIGHCPU_32
            if needs_qemu
            else BuildOptions.MachineType.E2_HIGHCPU_8
        )

        return BuildConfiguration(
            steps=build_steps,
            uses_source=True,
            machine_type=assert_type(machine_type, BuildOptions.MachineType),
        )
