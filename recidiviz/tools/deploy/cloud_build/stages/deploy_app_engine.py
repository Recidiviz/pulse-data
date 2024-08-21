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

from google.cloud.devtools.cloudbuild_v1 import BuildStep

from recidiviz.tools.deploy.cloud_build.artifact_registry_repository import (
    ArtifactRegistryDockerImageRepository,
    ImageKind,
)
from recidiviz.tools.deploy.cloud_build.build_configuration import (
    BuildConfiguration,
    DeploymentContext,
    build_step_for_shell_command,
)
from recidiviz.tools.deploy.cloud_build.constants import BUILDER_GCLOUD
from recidiviz.tools.deploy.cloud_build.deployment_stage_interface import (
    DeploymentStageInterface,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import project_id

GCLOUD_IMAGE = "gcr.io/google.com/cloudsdktool/cloud-sdk:slim"


class DeployAppEngine(DeploymentStageInterface):
    """Deployment stage for deploying app engine"""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        return argparse.ArgumentParser()

    def configure_build(
        self, deployment_context: DeploymentContext, _: argparse.Namespace
    ) -> BuildConfiguration:
        """Build configration for deploying app engine"""
        if project_id() == GCP_PROJECT_STAGING:
            app_engine_config = "staging.yaml"
        elif project_id() == GCP_PROJECT_PRODUCTION:
            app_engine_config = "production.yaml"
        else:
            raise ValueError("Must be deploying app engine to staging/production")

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
            build_step_for_shell_command(
                id_="fetch-appengine-config",
                name=app_engine_image,
                # wait_for: `-` indicates that the step should be run immediately rather than the default of running
                # after all previous steps.
                # https://cloud.google.com/build/docs/configuring-builds/configure-build-step-order
                wait_for="-",
                # Extracts the app engine YAML config from the `appengine` docker image and extracts it to the
                # Cloud Build workspace
                command=f"cp /app/{app_engine_config} /workspace/{app_engine_config}",
            ),
            BuildStep(
                id="deploy-appengine",
                name=BUILDER_GCLOUD,
                entrypoint="gcloud",
                args=[
                    "-q",
                    "app",
                    "deploy",
                    "--no-promote",
                    f"/workspace/{app_engine_config}",
                    "--project",
                    "${_PROJECT_ID}",
                    "--image-url",
                    app_engine_image,
                    "--verbosity=debug",
                ],
            ),
        ]

        return BuildConfiguration(
            steps=build_steps,
            timeout_seconds=15 * 60,
        )
