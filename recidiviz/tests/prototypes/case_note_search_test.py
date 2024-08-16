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
from unittest import TestCase
from unittest.mock import MagicMock, patch

from google.cloud.discoveryengine_v1.services.search_service.pagers import SearchPager

from recidiviz.prototypes.case_note_search.case_note_search import (
    FAKE_CASE_NOTES_ENGINE_ID,
    case_note_search,
    extract_case_notes_results,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING


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


class TestCaseNoteFunctions(TestCase):
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
        results = extract_case_notes_results(mock_pager)
        self.assertEqual(results, expected_result)

    @patch(
        "recidiviz.prototypes.case_note_search.case_note_search.extract_case_notes_results"
    )
    @patch(
        "recidiviz.prototypes.case_note_search.case_note_search.DiscoveryEngineInterface"
    )
    def test_case_note_search(
        self,
        mock_discovery_engine_interface: MagicMock,
        mock_extract_case_notes_results: MagicMock,
    ) -> None:
        query = "test query"
        page_size = 10
        filter_conditions = {"external_id": ["1234", "6789"]}

        mock_search_pager = MagicMock(spec=SearchPager)
        mock_discovery_interface_instance = mock_discovery_engine_interface.return_value
        mock_discovery_interface_instance.search.return_value = mock_search_pager

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

        results = case_note_search(query, page_size, filter_conditions)
        self.assertEqual(results, {"results": [case_note_data], "error": None})

        mock_discovery_engine_interface.assert_called_once_with(
            project_id=GCP_PROJECT_STAGING, engine_id=FAKE_CASE_NOTES_ENGINE_ID
        )
        mock_discovery_interface_instance.search.assert_called_once_with(
            query=query,
            page_size=page_size,
            filter_conditions=filter_conditions,
            with_snippet=False,
        )
        mock_extract_case_notes_results.assert_called_once_with(mock_search_pager)
