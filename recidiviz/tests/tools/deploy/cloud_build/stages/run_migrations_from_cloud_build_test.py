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
"""Test for run migrations stage"""
import unittest
from unittest.mock import patch

from recidiviz.tests.utils.with_secrets import with_secrets
from recidiviz.tools.deploy.cloud_build.build_configuration import DeploymentContext
from recidiviz.tools.deploy.cloud_build.stages.run_migrations_from_cloud_build import (
    RunMigrations,
)
from recidiviz.tools.migrations.run_migrations_to_head import create_parser
from recidiviz.utils.environment import GCP_PROJECT_STAGING


class RunMigrationsTest(unittest.TestCase):
    """Test for run migrations stage"""

    def setUp(self) -> None:
        self.mock_project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_patcher.start().return_value = GCP_PROJECT_STAGING

    def tearDown(self) -> None:
        self.mock_project_id_patcher.stop()

    @with_secrets(
        {
            "insights_cloudsql_instance_id": "123",
            "operations_v2_cloudsql_instance_id": "123",
        }
    )
    def test_parse_args(self) -> None:
        """Tests that the generated shell commands can actually be parsed by the `run_migrations_to_head` script"""
        deployment_stage_parser = RunMigrations.get_parser()

        deployment_context = DeploymentContext(
            project_id=GCP_PROJECT_STAGING,
            version_tag="v1",
            commit_ref="a1b2c3d4",
            stage="stage",
        )

        build_configuration = RunMigrations().configure_build(
            deployment_context=deployment_context,
            args=deployment_stage_parser.parse_args(
                ["--schema_types", "OPERATIONS,INSIGHTS"]
            ),
        )

        migration_script_parser = create_parser()
        migration_steps = [
            step
            for step in build_configuration.steps
            if step.id.startswith("run-migrations-")
        ]

        for migration_step in migration_steps:
            _, shell_command = migration_step.args
            _, uv_command = shell_command.split(";")
            migrations_args = (
                uv_command.replace("\n", "")
                .replace(
                    "uv run python -m recidiviz.tools.migrations.run_migrations_to_head ",
                    "",
                )
                .strip()
            )

            self.assertIsNotNone(
                migration_script_parser.parse_args(migrations_args.split(" "))
            )
