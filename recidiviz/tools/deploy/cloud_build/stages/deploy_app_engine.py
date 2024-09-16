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
"""Build configuration for deploying app engine"""
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
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import project_id

GCLOUD_IMAGE = "gcr.io/google.com/cloudsdktool/cloud-sdk:slim"


APP_ENGINE_CONFIGS = {
    GCP_PROJECT_STAGING: "/workspace/staging.yaml",
    GCP_PROJECT_PRODUCTION: "/workspace/prod.yaml",
}


class DeployAppEngine(DeploymentStageInterface):
    """Deployment stage for deploying app engine"""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--promote",
            action=argparse.BooleanOptionalAction,
            default=False,
        )
        return parser

    def configure_build(
        self,
        deployment_context: DeploymentContext,
        args: argparse.Namespace,
    ) -> BuildConfiguration:
        """Build configration for deploying app engine"""
        try:
            app_engine_config = APP_ENGINE_CONFIGS[project_id()]
        except KeyError as e:
            raise KeyError("Must be deploying app engine to staging/production") from e

        app_engine = ArtifactRegistryDockerImageRepository.from_file()[
            ImageKind.APP_ENGINE
        ]
        app_engine_image = app_engine.version_url(
            version_tag=deployment_context.version_tag
        )
        build_steps = [
            build_step_for_shell_command(
                id_="prefetch-builder-image",
                name=BUILDER_GCLOUD,
                command='echo "Downloaded latest image!"',
            ),
            build_step_for_gcloud_command(
                id_="deploy-app-engine",
                args=[
                    "--project",
                    deployment_context.project_id,
                    "app",
                    "deploy",
                    app_engine_config,
                    "--promote" if args.promote else "--no-promote",
                    "--image-url",
                    app_engine_image,
                    "--version",
                    deployment_context.app_engine_tag,
                ],
            ),
        ]

        # If we aren't promoting the version to serve traffic, it can be stopped to avoid billing for idle instances.
        if not args.promote:
            build_steps.append(
                build_step_for_gcloud_command(
                    id_="stop-idle-version",
                    args=[
                        "--project",
                        deployment_context.project_id,
                        "app",
                        "versions",
                        "stop",
                        deployment_context.version_tag,
                    ],
                )
            )

        return BuildConfiguration(
            steps=build_steps,
            uses_source=True,
            timeout_seconds=15 * 60,
        )
