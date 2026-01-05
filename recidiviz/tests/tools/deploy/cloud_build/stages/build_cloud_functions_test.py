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
"""Test for BuildCloudFunctions stage"""
import unittest
from unittest.mock import patch

from more_itertools import one

from recidiviz.tools.cloud_functions.create_function_source_file_archive import (
    create_parser,
)
from recidiviz.tools.deploy.cloud_build.build_configuration import DeploymentContext
from recidiviz.tools.deploy.cloud_build.stages.build_cloud_functions import (
    BuildCloudFunctions,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING


class BuildCloudFunctionsTest(unittest.TestCase):
    """Test for BuildCloudFunctions stage"""

    def setUp(self) -> None:
        self.mock_project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_patcher.start().return_value = GCP_PROJECT_STAGING

    def tearDown(self) -> None:
        self.mock_project_id_patcher.stop()

    def test_parse_args(self) -> None:
        """Tests that the generated shell commands can actually be parsed by the `create_function_source_file_archive` script"""
        deployment_stage_parser = BuildCloudFunctions.get_parser()

        deployment_context = DeploymentContext(
            project_id=GCP_PROJECT_STAGING,
            version_tag="v1",
            commit_ref="a1b2c3d4",
            stage="BuildCloudFunctions",
        )

        build_configuration = BuildCloudFunctions().configure_build(
            deployment_context=deployment_context,
            args=deployment_stage_parser.parse_args(""),
        )

        create_source_archive_parser = create_parser()
        create_source_archive_step = one(
            [
                step
                for step in build_configuration.steps
                if step.id == "Create Cloud Function source archive"
            ]
        )

        _, uv_command = create_source_archive_step.args
        migrations_args = (
            uv_command.replace("\n", "")
            .replace(
                "uv run python -m recidiviz.tools.cloud_functions.create_function_source_file_archive ",
                "",
            )
            .strip()
        )

        self.assertIsNotNone(
            create_source_archive_parser.parse_args(migrations_args.split(" "))
        )
