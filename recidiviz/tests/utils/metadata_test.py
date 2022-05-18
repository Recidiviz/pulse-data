# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for utils/metadata.py."""
import unittest
from unittest.mock import Mock, patch

import responses
from responses import matchers

from recidiviz.utils import metadata
from recidiviz.utils.metadata import CloudRunMetadata, local_project_id_override


@patch.dict("recidiviz.utils.metadata._metadata_cache", {})
class MetadataTest(unittest.TestCase):
    """Tests for utils/metadata.py."""

    def setUp(self) -> None:
        metadata.allow_local_metadata_call = True

    def tearDown(self) -> None:
        metadata.allow_local_metadata_call = False

    def test_local_project_id_override(self) -> None:
        original_project = metadata.project_id()

        with local_project_id_override("recidiviz-456"):
            self.assertEqual("recidiviz-456", metadata.project_id())

        self.assertEqual(original_project, metadata.project_id())

        with local_project_id_override("recidiviz-678"):
            self.assertEqual("recidiviz-678", metadata.project_id())

        self.assertEqual(original_project, metadata.project_id())

    def test_local_project_id_override_throws_if_called_nested(self) -> None:
        original_project = metadata.project_id()

        with local_project_id_override("recidiviz-456"):
            self.assertEqual("recidiviz-456", metadata.project_id())
            with self.assertRaises(ValueError):
                with local_project_id_override("recidiviz-678"):
                    pass
            self.assertEqual("recidiviz-456", metadata.project_id())

        self.assertEqual(original_project, metadata.project_id())

    @responses.activate
    def test_project_id(self) -> None:
        responses.add(
            responses.GET,
            "http://metadata/computeMetadata/v1/project/project-id",
            body="recidiviz-456",
        )

        self.assertEqual("recidiviz-456", metadata.project_id())

    @responses.activate
    def test_region(self) -> None:
        responses.add(
            responses.GET,
            "http://metadata/computeMetadata/v1/instance/zone",
            body="projects/123/zones/us-east1-c",
        )

        self.assertEqual("us-east1", metadata.region())

    @responses.activate
    def test_zone(self) -> None:
        responses.add(
            responses.GET,
            "http://metadata/computeMetadata/v1/instance/zone",
            body="projects/123/zones/us-east1-c",
        )

        self.assertEqual("us-east1-c", metadata.zone())

    @responses.activate
    @patch("recidiviz.utils.metadata.project_id", return_value="fake-project")
    @patch("recidiviz.utils.metadata.region", return_value="us-east1")
    def test_service_metadata(self, _mock_project_id: Mock, _mock_region: Mock) -> None:
        responses.add(
            responses.GET,
            "http://metadata/computeMetadata/v1/instance/service-accounts/default/token",
            json={"access_token": "fake-token"},
        )

        responses.add(
            responses.GET,
            "https://us-east1-run.googleapis.com/apis/serving.knative.dev/v1/namespaces/fake-project/services/test-service",
            json={"status": {"url": "http://test-service.cloudrun"}},
            match=[matchers.header_matcher({"Authorization": "Bearer fake-token"})],
        )

        self.assertEqual(
            CloudRunMetadata.build_from_metadata_server("test-service"),
            CloudRunMetadata(
                project_id="fake-project",
                region="us-east1",
                url="http://test-service.cloudrun",
            ),
        )
