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

from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from google.cloud.discoveryengine_v1 import SearchRequest, SearchResponse
from google.cloud.discoveryengine_v1.services.search_service import (
    SearchServiceAsyncClient,
)

from recidiviz.tools.prototypes.discovery_engine import DiscoveryEngineInterface


class TestDiscoveryEngineInterface(IsolatedAsyncioTestCase):
    """Implements tests for the Prototypes TestDiscoveryEngineInterface class."""

    def setUp(self) -> None:
        self.project_id = "test_project"
        self.engine_id = "test_engine"
        self.interface = DiscoveryEngineInterface(
            project_id=self.project_id, engine_id=self.engine_id
        )

    def test_serving_config(self) -> None:
        expected = f"projects/{self.project_id}/locations/us/collections/default_collection/engines/{self.engine_id}/servingConfigs/default_config"
        self.assertEqual(self.interface.serving_config, expected)

    def test_format_filter_condition(self) -> None:
        filter_condition = {
            "external_id": ["12345", "23456"],
            "note_type": ["Supervision Notes"],
        }
        expected = (
            'external_id: ANY("12345", "23456") AND note_type: ANY("Supervision Notes")'
        )
        self.assertEqual(
            self.interface._format_filter_condition(  # pylint: disable=protected-access
                filter_condition
            ),
            expected,
        )

    @patch.object(SearchServiceAsyncClient, "search", new_callable=AsyncMock)
    @patch(
        "google.cloud.discoveryengine_v1.services.search_service.SearchServiceAsyncClient.__init__",
        lambda self, *args, **kwargs: None,
    )
    async def test_search_success(self, mock_search: MagicMock) -> None:
        mock_response = MagicMock(spec=SearchResponse)
        mock_search.return_value = mock_response

        query = "test query"
        page_size = 10
        include_filter_conditions = {"field": ["value"]}
        exclude_filter_conditions = {"field_to_exclude": ["exclude_value"]}

        response = await self.interface.search(
            query=query,
            page_size=page_size,
            include_filter_conditions=include_filter_conditions,
            exclude_filter_conditions=exclude_filter_conditions,
            with_snippet=True,
            with_summary=True,
        )

        self.assertEqual(response, mock_response)
        mock_search.assert_called_once()
        called_request = mock_search.call_args[0][0]

        self.assertIsInstance(called_request, SearchRequest)
        self.assertEqual(called_request.query, query)
        self.assertEqual(called_request.page_size, page_size)
        self.assertEqual(
            called_request.filter,
            'field: ANY("value") AND NOT field_to_exclude: ANY("exclude_value")',
        )
        self.assertEqual(called_request.serving_config, self.interface.serving_config)

    @patch.object(SearchServiceAsyncClient, "search", new_callable=AsyncMock)
    @patch(
        "google.cloud.discoveryengine_v1.services.search_service.SearchServiceAsyncClient.__init__",
        lambda self, *args, **kwargs: None,
    )
    async def test_search_with_error(self, mock_search: MagicMock) -> None:
        mock_search.side_effect = Exception("Test Exception")

        query = "test query"

        # Ensure the exception is raised
        with self.assertRaises(Exception) as context:
            await self.interface.search(query=query)

        self.assertIn("Test Exception", str(context.exception))
        mock_search.assert_called_once()
