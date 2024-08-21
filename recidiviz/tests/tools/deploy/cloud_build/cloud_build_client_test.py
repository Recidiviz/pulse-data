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
"""Tests for CloudBuildClient"""
import unittest
from datetime import datetime
from unittest.mock import MagicMock

import freezegun
from google.cloud.devtools.cloudbuild_v1 import Build

from recidiviz.tools.deploy.cloud_build.build_configuration import (
    BuildConfiguration,
    DeploymentContext,
    create_deployment_build_api_obj,
)
from recidiviz.tools.deploy.cloud_build.cloud_build_client import CloudBuildClient


class CloudBuildClientTest(unittest.TestCase):
    """Test case for CloudBuildClient"""

    deployment_context = DeploymentContext(
        project_id="test-project",
        commit_ref="a1b2c3d4",
        version_tag="v1.0",
    )

    @freezegun.freeze_time(
        time_to_freeze=datetime(2024, 8, 12, 0, 0, 0),
        auto_tick_seconds=20,
    )
    def test_run_build_timeout(self) -> None:
        mock_client = MagicMock()

        client = CloudBuildClient(
            project_id="project-123", location="global", client=mock_client
        )

        with self.assertRaises(TimeoutError):
            client.run_build(
                build=create_deployment_build_api_obj(
                    build_configuration=BuildConfiguration(
                        steps=[], timeout_seconds=10
                    ),
                    deployment_context=self.deployment_context,
                )
            )

    def test_run_build_failure(self) -> None:
        mock_client = MagicMock()
        build_operation = MagicMock()
        build_operation.metadata.build.id = "build1"
        mock_client.create_build.return_value = build_operation
        mock_client.get_build.return_value = Build(
            mapping={"status": Build.Status.FAILURE}
        )

        client = CloudBuildClient(
            project_id="project-123", location="global", client=mock_client
        )

        with self.assertRaises(RuntimeError):
            client.run_build(
                create_deployment_build_api_obj(
                    build_configuration=BuildConfiguration(steps=[]),
                    deployment_context=self.deployment_context,
                )
            )
