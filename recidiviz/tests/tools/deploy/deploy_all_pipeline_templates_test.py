# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests the pulse-data/recidiviz/tools/deploy/deploy_all_pipeline_templates.py"""
import os
import unittest
from unittest import mock

from recidiviz.tools.deploy.deploy_all_pipeline_templates import (
    deploy_pipeline_templates,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING


class TestDeployAllPipelineTemplates(unittest.TestCase):
    """Tests for deploy_all_pipeline_templates.py"""

    @mock.patch(
        "recidiviz.tools.deploy.deploy_all_pipeline_templates.find_and_deploy_single_pipeline_template"
    )
    @mock.patch(
        "recidiviz.tools.deploy.deploy_all_pipeline_templates.build_source_distribution",
        return_value="mock-source-distribution",
    )
    def test_deploy_pipeline_templates_does_not_include_staging_only(
        self,
        mock_source_distribution: mock.MagicMock,
        find_and_deploy_mock: mock.MagicMock,
    ) -> None:
        """Test that staging_only templates are not deployed to any other environment."""
        fake_pipeline_template_path = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                "../../calculator/",
                "fake_calculation_pipeline_templates.yaml",
            )
        )
        deploy_pipeline_templates(
            fake_pipeline_template_path, project_id=GCP_PROJECT_PRODUCTION
        )

        find_and_deploy_mock.assert_has_calls(
            [
                mock.call(
                    project_id=GCP_PROJECT_PRODUCTION,
                    source_distribution=mock_source_distribution(),
                    job_name="full-us-xx-pipeline_no_limit-calculations",
                ),
                mock.call(
                    project_id=GCP_PROJECT_PRODUCTION,
                    source_distribution=mock_source_distribution(),
                    job_name="us-xx-pipeline_with_limit-calculations-36",
                ),
                mock.call(
                    project_id=GCP_PROJECT_PRODUCTION,
                    source_distribution=mock_source_distribution(),
                    job_name="us-yy-pipeline_with_limit-calculations-24",
                ),
                mock.call(
                    project_id=GCP_PROJECT_PRODUCTION,
                    source_distribution=mock_source_distribution(),
                    job_name="us-yy-historical-pipeline_with_limit-calculations-240",
                ),
            ]
        )

    @mock.patch(
        "recidiviz.tools.deploy.deploy_all_pipeline_templates.find_and_deploy_single_pipeline_template"
    )
    @mock.patch(
        "recidiviz.tools.deploy.deploy_all_pipeline_templates.build_source_distribution",
        return_value="mock-source-distribution",
    )
    def test_deploy_pipeline_templates_staging_only(
        self,
        mock_source_distribution: mock.MagicMock,
        find_and_deploy_mock: mock.MagicMock,
    ) -> None:
        """Test that staging_only templates are deployed to staging environment."""
        fake_pipeline_template_path = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                "../../calculator/",
                "fake_calculation_pipeline_templates.yaml",
            )
        )
        deploy_pipeline_templates(
            fake_pipeline_template_path, project_id=GCP_PROJECT_STAGING
        )

        find_and_deploy_mock.assert_has_calls(
            [
                mock.call(
                    project_id=GCP_PROJECT_STAGING,
                    source_distribution=mock_source_distribution(),
                    job_name="full-us-xx-pipeline_no_limit-calculations",
                ),
                mock.call(
                    project_id=GCP_PROJECT_STAGING,
                    source_distribution=mock_source_distribution(),
                    job_name="us-xx-pipeline_with_limit-calculations-36",
                ),
                mock.call(
                    project_id=GCP_PROJECT_STAGING,
                    source_distribution=mock_source_distribution(),
                    job_name="us-yy-pipeline_with_limit-calculations-24",
                ),
                mock.call(
                    project_id=GCP_PROJECT_STAGING,
                    source_distribution=mock_source_distribution(),
                    job_name="us-yy-pipeline_staging_only-calculations-36",
                ),
                mock.call(
                    project_id=GCP_PROJECT_STAGING,
                    source_distribution=mock_source_distribution(),
                    job_name="us-yy-historical-pipeline_with_limit-calculations-240",
                ),
            ]
        )
