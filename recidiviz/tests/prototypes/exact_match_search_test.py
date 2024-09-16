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
"""This class implements tests for the exact match search module."""
import re
import unittest
from typing import Any
from unittest.mock import MagicMock, patch

from recidiviz.prototypes.case_note_search.exact_match import exact_match_search


class TestExactMatchSearchQuery(unittest.TestCase):
    """This class implements tests for the exact match search module."""

    @patch("recidiviz.prototypes.case_note_search.exact_match.bigquery.Client")
    def test_exact_match_search_basic_query(self, mock_bq_client: Any) -> None:
        # Mock BigQuery client and query method
        mock_client_instance = MagicMock()
        mock_bq_client.return_value = mock_client_instance

        # Call the function
        query_term = "query_term"
        exact_match_search(query_term=query_term)

        # Check if the query string is as expected
        expected_query = """
        select *
        from `recidiviz-staging.case_notes_prototype_views.case_notes_materialized`
        where regexp_contains(lower(json_extract_scalar(JsonData, '$.note_body')), lower(@query_term))
        limit 20
        """

        # Extract the actual query passed to the client
        actual_query = mock_client_instance.query.call_args[0][0]

        def remove_whitespace(query: str) -> str:
            """Comparing SQL queries by removing whitespace and newlines."""
            return re.sub(r"\s+", "", query)

        # Assert that the actual query matches the expected query
        self.assertEqual(
            remove_whitespace(expected_query), remove_whitespace(actual_query)
        )

        # Check if the correct query parameter for 'query_term' was passed
        actual_query_parameters = mock_client_instance.query.call_args[1][
            "job_config"
        ].query_parameters
        query_term_param = next(
            (p for p in actual_query_parameters if p.name == "query_term"), None
        )

        # Assert that the query_term was correctly passed as a parameter
        self.assertIsNotNone(query_term_param)
        if query_term_param is not None:
            self.assertEqual(query_term_param.value, "query_term")

    @patch("recidiviz.prototypes.case_note_search.exact_match.bigquery.Client")
    def test_exact_match_search_acronym(self, mock_bq_client: Any) -> None:
        # Mock BigQuery client and query method
        mock_client_instance = MagicMock()
        mock_bq_client.return_value = mock_client_instance

        # Query an acronym.
        query_term = "UA"
        exact_match_search(query_term=query_term)

        expected_query = """
        select *
        from `recidiviz-staging.case_notes_prototype_views.case_notes_materialized`
        where regexp_contains(lower(json_extract_scalar(JsonData, '$.note_body')), lower(@query_term))
        limit 20
        """

        # Extract the actual query passed to the client
        actual_query = mock_client_instance.query.call_args[0][0]

        def remove_whitespace(query: str) -> str:
            """Comparing SQL queries by removing whitespace and newlines."""
            return re.sub(r"\s+", "", query)

        # Assert that the actual query matches the expected query
        self.assertEqual(
            remove_whitespace(expected_query), remove_whitespace(actual_query)
        )

        # Check if the correct query parameter for 'query_term' was passed
        actual_query_parameters = mock_client_instance.query.call_args[1][
            "job_config"
        ].query_parameters
        query_term_param = next(
            (p for p in actual_query_parameters if p.name == "query_term"), None
        )

        # Assert that the query_term was correctly passed as a word-bound regex match
        self.assertIsNotNone(query_term_param)
        if query_term_param is not None:
            self.assertEqual(query_term_param.value, "\\bUA\\b")

    @patch("recidiviz.prototypes.case_note_search.exact_match.bigquery.Client")
    def test_exact_match_search_with_filters(self, mock_bq_client: Any) -> None:
        # Mock BigQuery client and query method
        mock_client_instance = MagicMock()
        mock_bq_client.return_value = mock_client_instance

        # Call the function with filter conditions
        query_term = "query_term"
        include_filter_conditions = {"state_code": ["US_ME"]}
        exact_match_search(
            query_term=query_term, include_filter_conditions=include_filter_conditions
        )

        # Check if the query string is as expected with filter conditions
        expected_query = """
        select *
        from `recidiviz-staging.case_notes_prototype_views.case_notes_materialized`
        where regexp_contains(lower(json_extract_scalar(JsonData, '$.note_body')), lower(@query_term))
        and JSON_EXTRACT_SCALAR(JsonData, '$.state_code') in ('US_ME')
        limit 20
        """

        # Extract the actual query passed to the client
        actual_query = mock_client_instance.query.call_args[0][0]

        def remove_whitespace(query: str) -> str:
            """Comparing SQL queries by removing whitespace and newlines."""
            return re.sub(r"\s+", "", query)

        # Assert that the actual query matches the expected query
        self.assertEqual(
            remove_whitespace(expected_query), remove_whitespace(actual_query)
        )

        # Check if the correct query parameter for 'query_term' was passed
        actual_query_parameters = mock_client_instance.query.call_args[1][
            "job_config"
        ].query_parameters
        query_term_param = next(
            (p for p in actual_query_parameters if p.name == "query_term"), None
        )

        # Assert that the query_term was correctly passed as a parameter
        self.assertIsNotNone(query_term_param)
        if query_term_param is not None:
            self.assertEqual(query_term_param.value, "query_term")
