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
"""Tests for deployment_step_runner"""
import argparse
import unittest

from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.tests.utils.with_secrets import with_secrets
from recidiviz.tools.deploy.cloud_build.artifact_registry_repository import ImageKind
from recidiviz.tools.deploy.cloud_build.build_configuration import DeploymentContext
from recidiviz.tools.deploy.cloud_build.deployment_stage_runner import (
    AVAILABLE_DEPLOYMENT_STAGES,
)
from recidiviz.tools.deploy.cloud_build.stages.build_images import BuildImages
from recidiviz.tools.deploy.cloud_build.stages.create_terraform_plan import (
    CreateTerraformPlan,
)
from recidiviz.tools.deploy.cloud_build.stages.run_migrations_from_cloud_build import (
    RunMigrations,
)
from recidiviz.tools.deploy.cloud_build.stages.tag_images import TagImages
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


class DeploymentStepRunnerTest(unittest.TestCase):
    """Test case for ArtifactRegistryRepository"""

    @with_secrets({"operations_v2_cloudsql_instance_id": "123"})
    def test_parse(self) -> None:
        stage_args = {
            CreateTerraformPlan: argparse.Namespace(
                apply=False,
                for_pull_requests=False,
            ),
            BuildImages: argparse.Namespace(
                images=[ImageKind.APP_ENGINE], promote=False
            ),
            TagImages: argparse.Namespace(images=[ImageKind.APP_ENGINE]),
            RunMigrations: argparse.Namespace(schema_types=[SchemaType.OPERATIONS]),
        }

        with local_project_id_override(GCP_PROJECT_STAGING):
            for deployment_stage in AVAILABLE_DEPLOYMENT_STAGES:
                build = deployment_stage().configure_build(
                    DeploymentContext(
                        project_id="test-project",
                        commit_ref="1a2b3c4d",
                        version_tag="v1.0",
                    ),
                    stage_args.get(deployment_stage, argparse.Namespace()),
                )
                self.assertGreater(len(build.steps), 0)
