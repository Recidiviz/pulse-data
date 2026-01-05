# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Build configuration for creating Cloud Function source archives + deploying function revisions


python -m recidiviz.tools.deploy.cloud_build.deployment_stage_runner \
    --project-id recidiviz-staging \
    --commit-ref "\$_COMMIT_REF" \
    --version-tag "latest" \
    --dry-run \
    --stage BuildCloudFunctions
"""
import argparse

from recidiviz.tools.cloud_functions.create_function_source_file_archive import (
    CLOUD_FUNCTION_SOURCE_CHANGED,
    get_cloud_function_source_archive_path,
)
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
    BUILDER_GCLOUD,
    RECIDIVIZ_SOURCE_VOLUME,
)
from recidiviz.tools.deploy.cloud_build.deployment_stage_interface import (
    DeploymentStageInterface,
)

BUILD_RESULTS_PATH = "/workspace/build_results.json"


class BuildCloudFunctions(DeploymentStageInterface):
    """Deployment stage for building Cloud Functions"""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        return parser

    def configure_build(
        self,
        deployment_context: DeploymentContext,
        args: argparse.Namespace,
    ) -> BuildConfiguration:
        """Returns build steps for building and deploy Cloud Functions"""

        app_engine = ArtifactRegistryDockerImageRepository.from_file()[
            ImageKind.APP_ENGINE
        ]
        app_engine_image = app_engine.version_url(
            version_tag=deployment_context.version_tag
        )

        archive_path = get_cloud_function_source_archive_path()

        create_function_source_archive = build_step_for_shell_command(
            id_="Create Cloud Function source archive",
            name=app_engine_image,
            dir_="/app/",
            command=(
                "uv run python -m recidiviz.tools.cloud_functions.create_function_source_file_archive "
                f"--project_id {deployment_context.project_id} "
                f"--result_path {BUILD_RESULTS_PATH} "
                "--dry_run False"
            ),
            volumes=[RECIDIVIZ_SOURCE_VOLUME],
            env=["HOME=/home/recidiviz"],
        )

        deploy_function_revisions = build_step_for_shell_command(
            command=f"""
               BUILD_RESULT=$(cat "{BUILD_RESULTS_PATH}")
               if [ "$$BUILD_RESULT" = "{CLOUD_FUNCTION_SOURCE_CHANGED}" ]; then
                   echo "Source files have changed, deploying new function revisions"
                   gcloud functions list \
                     --project={deployment_context.project_id} \
                     --filter='NOT name:"cloud-build-pagerduty"' \
                     --format="value(name)" | \
                     xargs -I{{}} \
                         gcloud functions deploy {{}} \
                         --region=us-central1 \
                         --source={archive_path.uri()}
               else 
                   echo "Source files have not changed, skipping function deployment"
               fi
               """,
            id_="Deploy function revisions",
            name=BUILDER_GCLOUD,
            volumes=[RECIDIVIZ_SOURCE_VOLUME],
            wait_for=[create_function_source_archive.id],
        )

        return BuildConfiguration(
            steps=[
                create_function_source_archive,
                deploy_function_revisions,
            ],
            uses_source=True,
            # 10 minute timeout
            timeout_seconds=10 * 60,
        )
