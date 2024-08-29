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
"""An interface for querying Google Vertex AI's Discovery Engine. This interface
connects to the Discovery Engine using the Google Cloud client library."""

import logging
from functools import cached_property
from typing import Dict, List, Optional

import attr
from google.api_core.client_options import ClientOptions
from google.cloud import discoveryengine_v1 as discoveryengine
from google.cloud.discoveryengine_v1.services.search_service.pagers import (
    SearchAsyncPager,
)

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

MAX_EXTRACTIVE_ANSWER_COUNT = 1
MAX_SNIPPET_COUNT = 1
SUMMARY_RESULT_COUNT = 10


@attr.define(kw_only=True)
class DiscoveryEngineInterface:
    """An interface for querying Google Vertex AI's Discovery Engine. This interface
    connects to the Discovery Engine using the Google Cloud client library."""

    project_id: str

    engine_id: str

    @cached_property
    def serving_config(self) -> str:
        """The resource name of the Search serving configuration."""
        return f"projects/{self.project_id}/locations/us/collections/default_collection/engines/{self.engine_id}/servingConfigs/default_config"

    @staticmethod
    def _format_filter_condition(
        include_filter_conditions: Optional[Dict[str, List[str]]] = None,
        exclude_filter_conditions: Optional[Dict[str, List[str]]] = None,
    ) -> Optional[str]:
        """Format the filter conditions for SearchRequest use.

        Converts `include_filter_conditions`
            * from: {'state_code': ['US_ME'], 'external_id':['XXXX', 'YYYY']}
            * to: state_code: ANY('US_ME') AND external_id: ANY('XXXX', 'YYYY')

        Converts `exclude_filter_conditions`
            * from: {'note_type': ['Confidential', 'Private']}
            * to: NOT note_type: ANY('Confidential', 'Private')
        """
        if include_filter_conditions is None and exclude_filter_conditions is None:
            return None
        if include_filter_conditions is None:
            include_filter_conditions = {}
        if exclude_filter_conditions is None:
            exclude_filter_conditions = {}

        formatted_conditions = []
        for field, values in include_filter_conditions.items():
            if len(values) == 0:
                continue
            values_as_str = ", ".join([f'"{value}"' for value in values])
            formatted_conditions.append(f"{field}: ANY({values_as_str})")

        for field, values in exclude_filter_conditions.items():
            if len(values) == 0:
                continue
            values_as_str = ", ".join([f'"{value}"' for value in values])
            formatted_conditions.append(f"NOT {field}: ANY({values_as_str})")

        return " AND ".join(formatted_conditions)

    async def search(
        self,
        query: str,
        page_size: int = 10,
        include_filter_conditions: Optional[Dict[str, List[str]]] = None,
        exclude_filter_conditions: Optional[Dict[str, List[str]]] = None,
        with_snippet: bool = False,
        with_summary: bool = False,
    ) -> SearchAsyncPager:
        """
        Executes a search query against Google Vertex AI's Discovery Engine.

        Args:
            query: The search query string.
            page_size: Optional. The number of search results to return per page. Defaults to 10.
            filter_conditions: Optional. A dictionary of filter conditions where keys are
                field names and values are lists of acceptable values for those fields.
                (e.g. filter_condition = {"external_id": ["XXX"]})
            with_snippet: If True, includes snippets in the search results. Defaults to False.
            with_summary: If True, includes summaries in the search results. Defaults to False.

        Returns:
            Optional[SearchPager]: A SearchPager object containing the search results,
                or None if an error occurs.
        """
        try:
            # Refer to the `SearchRequest` reference for all supported fields:
            # https://cloud.google.com/python/docs/reference/discoveryengine/latest/google.cloud.discoveryengine_v1.types.SearchRequest
            content_search_spec = discoveryengine.SearchRequest.ContentSearchSpec(
                extractive_content_spec=discoveryengine.SearchRequest.ContentSearchSpec.ExtractiveContentSpec(
                    max_extractive_answer_count=MAX_EXTRACTIVE_ANSWER_COUNT  # Ensuring extractive answer is always present.
                ),
                snippet_spec=discoveryengine.SearchRequest.ContentSearchSpec.SnippetSpec(
                    max_snippet_count=MAX_SNIPPET_COUNT  # Only one snippet per document.
                )
                if with_snippet
                else None,
                summary_spec=discoveryengine.SearchRequest.ContentSearchSpec.SummarySpec(
                    summary_result_count=SUMMARY_RESULT_COUNT  # The number of documents used to generate the summary. Maximum is 10.
                )
                if with_summary
                else None,
            )
            request = discoveryengine.SearchRequest(
                serving_config=self.serving_config,
                query=query,
                page_size=page_size,
                filter=DiscoveryEngineInterface._format_filter_condition(
                    include_filter_conditions=include_filter_conditions,
                    exclude_filter_conditions=exclude_filter_conditions,
                ),
                query_expansion_spec=discoveryengine.SearchRequest.QueryExpansionSpec(
                    condition=discoveryengine.SearchRequest.QueryExpansionSpec.Condition.AUTO,
                ),
                spell_correction_spec=discoveryengine.SearchRequest.SpellCorrectionSpec(
                    mode=discoveryengine.SearchRequest.SpellCorrectionSpec.Mode.AUTO
                ),
                content_search_spec=content_search_spec
                if with_snippet or with_summary
                else None,
            )
            # Using the US-based client for all searches.
            client = discoveryengine.SearchServiceAsyncClient(
                client_options=ClientOptions(
                    api_endpoint="us-discoveryengine.googleapis.com"
                )
            )
            response = await client.search(request)
            return response

        # Log error and raise.
        except Exception as e:
            logger.error("DiscoveryEngineInterface error: %s", e)
            raise
