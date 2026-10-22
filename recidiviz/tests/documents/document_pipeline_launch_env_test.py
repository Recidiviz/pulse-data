# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for document_pipeline_launch_env.py."""

import unittest

from recidiviz.documents.document_pipeline_launch_env import DocumentPipelineLaunchEnv
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING


class TestDocumentPipelineLaunchEnv(unittest.TestCase):
    """Tests for DocumentPipelineLaunchEnv."""

    def test_is_at_least(self) -> None:
        self.assertTrue(
            DocumentPipelineLaunchEnv.PRODUCTION.is_at_least(
                DocumentPipelineLaunchEnv.STAGING
            )
        )
        self.assertTrue(
            DocumentPipelineLaunchEnv.STAGING.is_at_least(
                DocumentPipelineLaunchEnv.STAGING
            )
        )
        self.assertFalse(
            DocumentPipelineLaunchEnv.STAGING.is_at_least(
                DocumentPipelineLaunchEnv.PRODUCTION
            )
        )
        self.assertFalse(
            DocumentPipelineLaunchEnv.LOCAL_ONLY.is_at_least(
                DocumentPipelineLaunchEnv.STAGING
            )
        )

    def test_is_launched_in_project(self) -> None:
        self.assertFalse(
            DocumentPipelineLaunchEnv.LOCAL_ONLY.is_launched_in_project(
                GCP_PROJECT_STAGING
            )
        )
        self.assertFalse(
            DocumentPipelineLaunchEnv.LOCAL_ONLY.is_launched_in_project(
                GCP_PROJECT_PRODUCTION
            )
        )

        self.assertTrue(
            DocumentPipelineLaunchEnv.STAGING.is_launched_in_project(
                GCP_PROJECT_STAGING
            )
        )
        self.assertFalse(
            DocumentPipelineLaunchEnv.STAGING.is_launched_in_project(
                GCP_PROJECT_PRODUCTION
            )
        )

        self.assertTrue(
            DocumentPipelineLaunchEnv.PRODUCTION.is_launched_in_project(
                GCP_PROJECT_PRODUCTION
            )
        )
        self.assertTrue(
            DocumentPipelineLaunchEnv.PRODUCTION.is_launched_in_project(
                GCP_PROJECT_STAGING
            )
        )

    def test_is_launched_in_non_deploy_project_is_treated_as_staging(self) -> None:
        test_project_id = "recidiviz-testing"
        self.assertFalse(
            DocumentPipelineLaunchEnv.LOCAL_ONLY.is_launched_in_project(test_project_id)
        )
        self.assertTrue(
            DocumentPipelineLaunchEnv.STAGING.is_launched_in_project(test_project_id)
        )
        self.assertTrue(
            DocumentPipelineLaunchEnv.PRODUCTION.is_launched_in_project(test_project_id)
        )
