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
"""This class implements tests for the case note search module."""
from typing import Any
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from google.cloud.discoveryengine_v1.services.search_service.pagers import SearchPager

from recidiviz.prototypes.case_note_search.case_note_search import (
    CASE_NOTE_SEARCH_ENGINE_ID,
    case_note_search,
    extract_case_notes_results_unstructured_data,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING

# Not a real engine ID. We mock out the call to the discovery engine.
TEST_SEARCH_ENGINE_ID = "TEST_SEARCH_ENGINE_ID"


class MockDocument:
    def __init__(
        self, document_id: Any, struct_data: Any, derived_struct_data: Any
    ) -> None:
        self.id = document_id
        self.struct_data = struct_data
        self.derived_struct_data = derived_struct_data


class MockResult:
    def __init__(self, document: Any) -> None:
        self.document = document


class TestCaseNoteFunctions(IsolatedAsyncioTestCase):
    """This class implements tests for the case note search module."""

    @patch("recidiviz.prototypes.case_note_search.case_note_search.GCSBucketReader")
    def test_extract_case_notes_results(
        self, mock_gcs_bucket_reader: MagicMock
    ) -> None:
        mock_pager = MagicMock(spec=SearchPager)
        mock_document = MockDocument(
            document_id="123",
            struct_data={
                "date": "2024-03-28",
                "contact_mode": "Phone",
                "note_type": "Supervision",
                "external_id": "456",
            },
            derived_struct_data={
                "link": "http://link_to_gcs_bucket.com",
                "extractive_answers": [{"content": "This is an extractive answer"}],
            },
        )
        mock_result = MockResult(document=mock_document)
        mock_pager.results = [mock_result]

        mock_gcs_reader_instance = mock_gcs_bucket_reader.return_value
        mock_gcs_reader_instance.download_blob.return_value = "Full case note content."

        expected_result = [
            {
                "document_id": "123",
                "date": "2024-03-28",
                "contact_mode": "Phone",
                "note_type": "Supervision",
                "note_title": None,
                "extractive_answer": "This is an extractive answer",
                "case_note": "Full case note content.",
                "snippet": None,
                "preview": "This is an extractive answer",  # Uses extractive_answer since snippet is None.
            }
        ]
        results = extract_case_notes_results_unstructured_data(mock_pager)
        self.assertEqual(results, expected_result)

    @patch(
        "recidiviz.prototypes.case_note_search.case_note_search.extract_case_notes_results_structured_data"
    )
    @patch(
        "recidiviz.prototypes.case_note_search.case_note_search.DiscoveryEngineInterface"
    )
    @patch("recidiviz.prototypes.case_note_search.case_note_search.exact_match_search")
    async def test_case_note_search(
        self,
        mock_exact_match_search: MagicMock,
        mock_discovery_engine_interface: AsyncMock,
        mock_extract_case_notes_results: MagicMock,
    ) -> None:
        query = "test query"
        page_size = 10
        filter_conditions = {"external_id": ["1234", "6789"], "state_code": ["US_ME"]}

        # Set up the mock for the DiscoveryEngineInterface instance
        mock_search_pager = MagicMock(spec=SearchPager)
        mock_discovery_interface_instance = mock_discovery_engine_interface.return_value
        mock_discovery_interface_instance.search = AsyncMock(
            return_value=mock_search_pager
        )

        case_note_data = {
            "document_id": "123",
            "date": "2024-03-28",
            "contact_mode": "Phone",
            "note_type": "Supervision",
            "note_title": None,
            "extractive_answer": "This is an extractive answer",
            "case_note": "Full case note content.",
            "snippet": None,
            "preview": "This is an extractive answer",  # Uses extractive_answer since snippet is None.
        }

        mock_extract_case_notes_results.return_value = [case_note_data]

        # Mock exact_match_search to return an empty dictionary
        mock_exact_match_search.return_value = {}

        results = await case_note_search(query, page_size, filter_conditions)
        self.assertEqual(results, {"results": [case_note_data], "error": None})

        mock_discovery_engine_interface.assert_called_once_with(
            project_id=GCP_PROJECT_STAGING, engine_id=CASE_NOTE_SEARCH_ENGINE_ID
        )
        exclude_filter_conditions = {
            "note_type": [
                "Investigation (Confidential)",
                "Mental Health (Confidential)",
                "FIAT - Confidential",
            ]
        }
        mock_discovery_interface_instance.search.assert_called_once_with(
            query=query,
            page_size=page_size,
            include_filter_conditions=filter_conditions,
            exclude_filter_conditions=exclude_filter_conditions,
            with_snippet=False,
        )
        mock_extract_case_notes_results.assert_called_once_with(mock_search_pager)

        # Ensure exact_match_search is called with the correct parameters
        mock_exact_match_search.assert_called_once_with(
            query_term=query,
            include_filter_conditions=filter_conditions,
            exclude_filter_conditions=exclude_filter_conditions,
            limit=page_size,
        )

    @patch(
        "recidiviz.prototypes.case_note_search.case_note_search.extract_case_notes_results_structured_data"
    )
    @patch(
        "recidiviz.prototypes.case_note_search.case_note_search.DiscoveryEngineInterface"
    )
    @patch("recidiviz.prototypes.case_note_search.case_note_search.exact_match_search")
    async def test_exact_search_result_order(
        self,
        mock_exact_match_search: MagicMock,
        mock_discovery_engine_interface: AsyncMock,
        mock_extract_case_notes_results: MagicMock,
    ) -> None:
        query = "content"
        page_size = 10
        filter_conditions = {"external_id": ["1234", "6789"], "state_code": ["US_ME"]}

        # Set up the mock for the DiscoveryEngineInterface instance
        mock_search_pager = MagicMock(spec=SearchPager)
        mock_discovery_interface_instance = mock_discovery_engine_interface.return_value
        mock_discovery_interface_instance.search = AsyncMock(
            return_value=mock_search_pager
        )

        case_note_data1 = {
            "state_code": "US_ME",
            "external_id": "1234",
            "note_id": "129",
            "note_date": "2024-03-29 11:27:00",
            "note_mode": "Phone",
            "note_type": "Supervision",
            "note_title": None,
            "note_body": "Full case note content.",
        }
        case_note_data_extracted1 = {
            "document_id": "129",
            "date": "2024-03-29 11:27:00",
            "contact_mode": "Phone",
            "note_type": "Supervision",
            "note_title": None,
            "extractive_answer": None,
            "snippet": None,
            "preview": "Full case note content.",
            "case_note": "Full case note content.",
        }
        case_note_data2 = {
            "state_code": "US_ME",
            "external_id": "1234",
            "note_id": "130",
            "note_date": "2024-03-30 11:27:00",
            "note_mode": "Phone",
            "note_type": "Supervision",
            "note_title": None,
            "note_body": "Full case note content.",
        }
        case_note_data_extracted2 = {
            "document_id": "130",
            "date": "2024-03-30 11:27:00",
            "contact_mode": "Phone",
            "note_type": "Supervision",
            "note_title": None,
            "extractive_answer": None,
            "snippet": None,
            "preview": "Full case note content.",
            "case_note": "Full case note content.",
        }
        case_note_data3 = {
            "state_code": "US_ME",
            "external_id": "1234",
            "note_id": "131",
            "note_date": "2024-03-31 11:27:00",
            "note_mode": "Phone",
            "note_type": "Supervision",
            "note_title": None,
            "note_body": "Full case note content.",
        }
        case_note_data_extracted3 = {
            "document_id": "131",
            "date": "2024-03-31 11:27:00",
            "contact_mode": "Phone",
            "note_type": "Supervision",
            "note_title": None,
            "extractive_answer": None,
            "snippet": None,
            "preview": "Full case note content.",
            "case_note": "Full case note content.",
        }

        # Regular search returns no notes, and exact search returns 3 notes
        mock_extract_case_notes_results.return_value = []
        mock_exact_match_search.return_value = {
            "US_ME_1234_129": case_note_data1,
            "US_ME_1234_130": case_note_data2,
            "US_ME_1234_131": case_note_data3,
        }

        results = await case_note_search(query, page_size, filter_conditions)
        # Returned results should be in reverse chronological order
        self.assertEqual(
            results,
            {
                "results": [
                    case_note_data_extracted3,
                    case_note_data_extracted2,
                    case_note_data_extracted1,
                ],
                "error": None,
            },
        )

    @patch(
        "recidiviz.prototypes.case_note_search.case_note_search.extract_case_notes_results_structured_data"
    )
    @patch(
        "recidiviz.prototypes.case_note_search.case_note_search.DiscoveryEngineInterface"
    )
    @patch("recidiviz.prototypes.case_note_search.case_note_search.exact_match_search")
    async def test_case_note_search_result_order(
        self,
        mock_exact_match_search: MagicMock,
        mock_discovery_engine_interface: AsyncMock,
        mock_extract_case_notes_results: MagicMock,
    ) -> None:
        query = "test query"
        page_size = 10
        filter_conditions = {"external_id": ["1234", "6789"], "state_code": ["US_ME"]}

        # Set up the mock for the DiscoveryEngineInterface instance
        mock_search_pager = MagicMock(spec=SearchPager)
        mock_discovery_interface_instance = mock_discovery_engine_interface.return_value
        mock_discovery_interface_instance.search = AsyncMock(
            return_value=mock_search_pager
        )

        exact_match1 = {
            "state_code": "US_ME",
            "external_id": "1234",
            "note_id": "126",
            "note_date": "2024-03-26 11:27:00",
            "note_body": "Full case note content.",
            "note_mode": "Phone",
            "note_type": "Supervision",
            "note_title": None,
        }
        exact_match_extracted1 = {
            "document_id": "126",
            "date": "2024-03-26 11:27:00",
            "contact_mode": "Phone",
            "note_type": "Supervision",
            "note_title": None,
            "extractive_answer": None,
            "snippet": None,
            "preview": "Full case note content.",
            "case_note": "Full case note content.",
        }
        exact_match2 = {
            "state_code": "US_ME",
            "external_id": "1234",
            "note_id": "127",
            "note_date": "2024-03-27 11:27:00",
            "note_body": "Full case note content.",
            "note_mode": "Phone",
            "note_type": "Supervision",
            "note_title": None,
        }
        exact_match_extracted2 = {
            "document_id": "127",
            "date": "2024-03-27 11:27:00",
            "contact_mode": "Phone",
            "note_type": "Supervision",
            "note_title": None,
            "extractive_answer": None,
            "snippet": None,
            "preview": "Full case note content.",
            "case_note": "Full case note content.",
        }
        exact_match3 = {
            "state_code": "US_ME",
            "external_id": "1234",
            "note_id": "128",
            "note_date": "2024-03-28 11:27:00",
            "note_body": "Full case note content.",
            "note_mode": "Phone",
            "note_type": "Supervision",
            "note_title": None,
        }
        exact_match_extracted3 = {
            "document_id": "128",
            "date": "2024-03-28 11:27:00",
            "contact_mode": "Phone",
            "note_type": "Supervision",
            "note_title": None,
            "extractive_answer": None,
            "snippet": None,
            "preview": "Full case note content.",
            "case_note": "Full case note content.",
        }
        exact_match4 = {
            "state_code": "US_ME",
            "external_id": "1234",
            "note_id": "129",
            "note_date": "2024-03-29 11:27:00",
            "note_body": "Full case note content.",
            "note_mode": "Phone",
            "note_type": "Supervision",
            "note_title": None,
        }
        exact_match_extracted4 = {
            "document_id": "129",
            "date": "2024-03-29 11:27:00",
            "contact_mode": "Phone",
            "note_type": "Supervision",
            "note_title": None,
            "extractive_answer": None,
            "snippet": None,
            "preview": "Full case note content.",
            "case_note": "Full case note content.",
        }
        non_exact_match1 = {
            "document_id": "130",
            "date": "2024-03-30 11:27:00",
            "contact_mode": "Phone",
            "note_type": "Supervision",
            "note_title": None,
            "extractive_answer": None,
            "snippet": None,
            "preview": "Full case note content.",
            "case_note": "Full case note content.",
        }
        non_exact_match2 = {
            "document_id": "131",
            "date": "2024-03-31 11:27:00",
            "contact_mode": "Phone",
            "note_type": "Supervision",
            "note_title": None,
            "extractive_answer": None,
            "snippet": None,
            "preview": "Full case note content.",
            "case_note": "Full case note content.",
        }

        # Regular search includes two exact match results and two non-exact match results
        mock_extract_case_notes_results.return_value = [
            exact_match_extracted2,
            non_exact_match1,
            exact_match_extracted3,
            non_exact_match2,
        ]
        mock_exact_match_search.return_value = {
            "US_ME_1234_126": exact_match1,
            "US_ME_1234_127": exact_match2,
            "US_ME_1234_128": exact_match3,
            "US_ME_1234_129": exact_match4,
        }

        results = await case_note_search(query, page_size, filter_conditions)
        # Result order should be:
        # * exact matches returned by regular search, in relevance order
        # * exact matches returned only by exact search, in reverse chronological order
        # * non-exact matches returned by regular search, in relevance order
        self.assertEqual(
            results,
            {
                "results": [
                    # exact matches returned by regular search, in relevance order
                    exact_match_extracted2,
                    exact_match_extracted3,
                    # exact matches returned only by exact search, in reverse chronological order
                    exact_match_extracted4,
                    exact_match_extracted1,
                    # non-exact matches returned by regular search, in relevance order
                    non_exact_match1,
                    non_exact_match2,
                ],
                "error": None,
            },
        )
