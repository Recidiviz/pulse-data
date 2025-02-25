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
"""Build configuration for tagging images"""
import argparse

from recidiviz.tools.deploy.cloud_build.artifact_registry_repository import (
    ArtifactRegistryDockerImageRepository,
    ImageKind,
)
from recidiviz.tools.deploy.cloud_build.build_configuration import (
    BuildConfiguration,
    DeploymentContext,
    build_step_for_gcloud_command,
    build_step_for_shell_command,
)
from recidiviz.tools.deploy.cloud_build.constants import BUILDER_GCLOUD
from recidiviz.tools.deploy.cloud_build.deployment_stage_interface import (
    DeploymentStageInterface,
)
from recidiviz.utils.params import str_to_list


class TagImages(DeploymentStageInterface):
    """Deployment stage for tagging images"""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        image_kinds = ", ".join(list(ImageKind))
        parser.add_argument(
            "--images",
            type=lambda images: [ImageKind(image) for image in str_to_list(images)],
            help=f"Comma delimited string of ImageKind values. Choices: {image_kinds}",
        )

        return parser

    def configure_build(
        self,
        deployment_context: DeploymentContext,
        args: argparse.Namespace,
    ) -> BuildConfiguration:
        """Generates a build configuration that promotes our built docker images"""
        artifact_registries = ArtifactRegistryDockerImageRepository.from_file()
        # Downloads the gcloud builder image so that each of the `build_steps` below have it in cache
        prefetch_builder_image = build_step_for_shell_command(
            id_="prefetch-builder-image",
            name=BUILDER_GCLOUD,
            command='echo "Downloaded builder image!"',
        )
        build_steps = [prefetch_builder_image]

        for image in args.images:
            repository = artifact_registries[image]
            build_steps.append(
                # Tags the build/commit_ref image with the default:version and default:latest
                build_step_for_gcloud_command(
                    id_=f"tag-{repository.repository_id}-image",
                    args=[
                        "container",
                        "images",
                        "add-tag",
                        repository.build_url(commit_ref=deployment_context.commit_ref),
                        repository.version_url(
                            version_tag=deployment_context.version_tag
                        ),
                        repository.latest_url(),
                    ],
                    wait_for=[prefetch_builder_image.id],
                )
            )

        return BuildConfiguration(steps=build_steps)
