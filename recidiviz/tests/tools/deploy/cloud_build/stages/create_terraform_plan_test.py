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
"""Test for CreatTerraformPlanTest stage"""
import unittest
from unittest.mock import patch

from google.cloud.orchestration.airflow.service_v1 import (
    Environment,
    EnvironmentConfig,
    EnvironmentsClient,
)
from more_itertools import one

from recidiviz.tools.deploy.cloud_build.build_configuration import DeploymentContext
from recidiviz.tools.deploy.cloud_build.stages.create_terraform_plan import (
    CreateTerraformPlan,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION


class CreateTerraformPlanTest(unittest.TestCase):
    """Test for CreateTerraformPlan stage"""

    def setUp(self) -> None:
        self.mock_project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_patcher.start().return_value = GCP_PROJECT_PRODUCTION

        self.environments_patcher = patch("recidiviz.tools.airflow.utils.service")
        mock_service = self.environments_patcher.start()
        mock_client = mock_service.EnvironmentsClient.return_value

        mock_client.common_location_path = EnvironmentsClient.common_location_path
        mock_client.environment_path = EnvironmentsClient.environment_path
        mock_client.list_environments = lambda request: {
            f"projects/{GCP_PROJECT_PRODUCTION}/locations/us-central1": [
                Environment(
                    name=f"projects/{GCP_PROJECT_PRODUCTION}/locations/us-central1/environments/orchestration-v2",
                    config=EnvironmentConfig(
                        dag_gcs_prefix="gs://test-bucket",
                    ),
                )
            ]
        }[request["parent"]]

    def tearDown(self) -> None:
        self.mock_project_id_patcher.stop()
        self.environments_patcher.stop()

    def test_airflow_bucket(self) -> None:
        """Tests that the generated gcloud storage command points to the airflow bucket"""
        deployment_stage_parser = CreateTerraformPlan.get_parser()

        deployment_context = DeploymentContext(
            project_id=GCP_PROJECT_PRODUCTION,
            version_tag="v1",
            commit_ref="a1b2c3d4",
            stage="CreateTerraformPlan",
        )

        build_configuration = CreateTerraformPlan().configure_build(
            deployment_context=deployment_context,
            args=deployment_stage_parser.parse_args(["--apply"]),
        )

        sync_airflow_source_step = one(
            [
                step
                for step in build_configuration.steps
                if step.id == "Sync Airflow source files to GCS"
            ]
        )

        _, gcloud_command = sync_airflow_source_step.args
        self.assertEqual(
            gcloud_command,
            "gcloud storage rsync /workspace/airflow_source_files gs://test-bucket --recursive --checksums-only --delete-unmatched-destination-objects --exclude=airflow_monitoring\\.py",
        )
