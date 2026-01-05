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
"""Build configuration running migrations"""
import argparse

from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    CLOUDSQL_PROXY_MIGRATION_PORT,
    SQLAlchemyEngineManager,
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
    BUILDER_ALPINE,
    CLOUD_SQL_PROXY_URI,
)
from recidiviz.tools.deploy.cloud_build.deployment_stage_interface import (
    DeploymentStageInterface,
)
from recidiviz.utils.params import str_to_list


class RunMigrations(DeploymentStageInterface):
    """Deployment stage for running migrations"""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        available_schema_types = [
            schema_type
            for schema_type in SchemaType
            if schema_type.has_cloud_sql_instance
        ]
        schema_types_str = ", ".join(
            [schema_type.value for schema_type in available_schema_types]
        )
        parser.add_argument(
            "--schema_types",
            type=lambda schema_types: [
                SchemaType(schema_type)
                for schema_type in str_to_list(schema_types)
                if SchemaType(schema_type) in available_schema_types
            ],
            default=available_schema_types,
            help=f"Comma delimited string of SchemaType values. Choices: {schema_types_str}",
        )

        return parser

    def configure_build(
        self,
        deployment_context: DeploymentContext,
        args: argparse.Namespace,
    ) -> BuildConfiguration:
        """Builds a configuration for running migrations"""
        app_engine = ArtifactRegistryDockerImageRepository.from_file()[
            ImageKind.APP_ENGINE
        ]
        app_engine_image = app_engine.version_url(
            version_tag=deployment_context.version_tag
        )

        # Downloads the Cloud SQL Proxy binary and extracts it into the build's shared `/workspace` volume
        copy_proxy_step = build_step_for_shell_command(
            id_="copy-proxy-to-workspace",
            name=BUILDER_ALPINE,
            command=(
                f"wget -O /workspace/cloud-sql-proxy {CLOUD_SQL_PROXY_URI} && "
                "chmod +x /workspace/cloud-sql-proxy"
            ),
        )

        # Downloads the latest `appengine` image so that it is in cache for each of the `schema_step`s below
        prefetch_image_step = build_step_for_shell_command(
            id_="prefetch-latest-image",
            name=app_engine_image,
            command=f'echo "Downloaded latest image!" {deployment_context.version_tag}',
        )

        steps = [
            copy_proxy_step,
            prefetch_image_step,
        ]

        for schema_type in args.schema_types:
            if not schema_type.has_cloud_sql_instance:
                continue

            instance_string = SQLAlchemyEngineManager.get_full_cloudsql_instance_id(
                schema_type
            )

            schema_step = build_step_for_shell_command(
                id_=f"run-migrations-{schema_type.value.lower().replace('_', '-')}",
                wait_for=[copy_proxy_step.id, prefetch_image_step.id],
                name=app_engine_image,
                dir_="/app/",
                env=["HOME=/home/recidiviz"],
                command=(
                    # Run the Cloud SQL Proxy and wait for it to be healthy
                    f"/workspace/cloud-sql-proxy {instance_string} --port {CLOUDSQL_PROXY_MIGRATION_PORT} "
                    "--run-connection-test & sleep 2; "
                    "uv run python -m recidiviz.tools.migrations.run_migrations_to_head "
                    "--no-launch-proxy "
                    f"--project-id {deployment_context.project_id} "
                    f"--database {schema_type.value} "
                    "--skip-db-name-check "
                ),
            )

            steps.append(schema_step)

        return BuildConfiguration(steps=steps)
