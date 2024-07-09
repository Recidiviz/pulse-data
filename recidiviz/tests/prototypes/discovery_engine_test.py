# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""This class implements tests for the Prototypes DiscoveryEngineInterface class."""

from unittest import TestCase
from unittest.mock import Mock, patch

from google.api_core.exceptions import GoogleAPICallError, RetryError

from recidiviz.tools.prototypes.discovery_engine import DiscoveryEngineInterface


class TestDiscoveryEngineInterface(TestCase):
    """Implements tests for the Prototypes TestDiscoveryEngineInterface class."""

    def setUp(self) -> None:
        self.project_id = "test_project"
        self.engine_id = "test_engine"
        self.interface = DiscoveryEngineInterface(
            project_id=self.project_id, engine_id=self.engine_id
        )

    def test_serving_config(self) -> None:
        expected = f"projects/{self.project_id}/locations/global/collections/default_collection/engines/{self.engine_id}/servingConfigs/default_config"
        self.assertEqual(self.interface.serving_config, expected)

    def test_get_external_id_filter_condition(self) -> None:
        external_id = "test_external_id"
        expected = f'external_id: ANY("{external_id}")'
        self.assertEqual(
            self.interface.get_external_id_filter_condition(external_id), expected
        )

    def test_extract_document_ids(self) -> None:
        search_response = Mock()
        search_response.results = [
            Mock(document=Mock(id="doc1")),
            Mock(document=Mock(id="doc2")),
        ]
        self.assertEqual(
            self.interface.extract_document_ids(search_response), ["doc1", "doc2"]
        )

    @patch(
        "recidiviz.tools.prototypes.discovery_engine.discoveryengine.SearchServiceClient"
    )
    def test_search_for_document_ids(self, MockSearchServiceClient: Mock) -> None:
        mock_client = MockSearchServiceClient.return_value
        mock_response = Mock()
        mock_response.results = [
            Mock(document=Mock(id="doc1")),
            Mock(document=Mock(id="doc2")),
        ]
        mock_client.search.return_value = mock_response

        query = "test_query"
        results = self.interface.search_for_document_ids(query)
        self.assertEqual(results, ["doc1", "doc2"])

    @patch(
        "recidiviz.tools.prototypes.discovery_engine.discoveryengine.SearchServiceClient"
    )
    def test_search_for_document_ids_with_external_id(
        self, MockSearchServiceClient: Mock
    ) -> None:
        mock_client = MockSearchServiceClient.return_value
        mock_response = Mock()
        mock_response.results = [Mock(document=Mock(id="doc1"))]
        mock_client.search.return_value = mock_response

        query = "test_query"
        external_id = "test_external_id"
        results = self.interface.search_for_document_ids(query, external_id=external_id)
        self.assertEqual(results, ["doc1"])

    @patch(
        "recidiviz.tools.prototypes.discovery_engine.discoveryengine.SearchServiceClient"
    )
    def test_search_for_document_ids_api_call_error(
        self, MockSearchServiceClient: Mock
    ) -> None:
        mock_client = MockSearchServiceClient.return_value
        mock_client.search.side_effect = GoogleAPICallError("API call error")

        query = "test_query"
        results = self.interface.search_for_document_ids(query)
        self.assertEqual(results, [])

    @patch(
        "recidiviz.tools.prototypes.discovery_engine.discoveryengine.SearchServiceClient"
    )
    def test_search_for_document_ids_retry_error(
        self, MockSearchServiceClient: Mock
    ) -> None:
        mock_client = MockSearchServiceClient.return_value
        mock_client.search.side_effect = RetryError(
            "Retry error", cause=Exception("Underlying cause")
        )

        query = "test_query"
        results = self.interface.search_for_document_ids(query)
        self.assertEqual(results, [])

    @patch(
        "recidiviz.tools.prototypes.discovery_engine.discoveryengine.SearchServiceClient"
    )
    def test_search_for_document_ids_unexpected_error(
        self, MockSearchServiceClient: Mock
    ) -> None:
        mock_client = MockSearchServiceClient.return_value
        mock_client.search.side_effect = Exception("Unexpected error")

        query = "test_query"
        results = self.interface.search_for_document_ids(query)
        self.assertEqual(results, [])
