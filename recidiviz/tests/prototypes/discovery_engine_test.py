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
from unittest.mock import MagicMock, patch

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
        expected = f"projects/{self.project_id}/locations/us/collections/default_collection/engines/{self.engine_id}/servingConfigs/default_config"
        self.assertEqual(self.interface.serving_config, expected)

    def test_format_filter_condition(self) -> None:
        filter_condition = {
            "external_id": ["12345", "23456"],
            "note_type": ["Supervision Notes"],
        }
        expected = (
            'external_id: ANY("12345", "23456") note_type: ANY("Supervision Notes")'
        )
        self.assertEqual(
            self.interface._format_filter_condition(  # pylint: disable=protected-access
                filter_condition
            ),
            expected,
        )

    @patch(
        "recidiviz.tools.prototypes.discovery_engine.discoveryengine.SearchServiceClient"
    )
    @patch("recidiviz.tools.prototypes.discovery_engine.discoveryengine.SearchRequest")
    def test_search_success(
        self, MockSearchRequest: MagicMock, MockSearchServiceClient: MagicMock
    ) -> None:
        mock_client_instance = MagicMock()
        mock_client_instance.search.return_value = "this is the query return value"
        MockSearchServiceClient.return_value = mock_client_instance

        mock_search_request_instance = MagicMock()
        MockSearchRequest.return_value = mock_search_request_instance

        query = "this is a query"
        page_size = 10
        filter_conditions = {"field": ["value"]}

        result = self.interface.search(
            query=query, page_size=page_size, filter_conditions=filter_conditions
        )

        self.assertEqual(result, "this is the query return value")
        mock_client_instance.search.assert_called_once_with(
            mock_search_request_instance
        )
