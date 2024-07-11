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
from google.api_core.exceptions import GoogleAPICallError, RetryError
from google.cloud import discoveryengine_v1 as discoveryengine
from google.cloud.discoveryengine_v1.services.search_service.pagers import SearchPager

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


@attr.define(kw_only=True)
class DiscoveryEngineInterface:
    """An interface for querying Google Vertex AI's Discovery Engine. This interface
    connects to the Discovery Engine using the Google Cloud client library."""

    project_id: str

    engine_id: str

    @cached_property
    def serving_config(self) -> str:
        """The resource name of the Search serving configuration."""
        return f"projects/{self.project_id}/locations/global/collections/default_collection/engines/{self.engine_id}/servingConfigs/default_config"

    @staticmethod
    def _format_filter_condition(filter_conditions: Dict[str, List[str]]) -> str:
        """Format the filter conditions for SearchRequest use."""
        formatted_conditions = []
        for field, values in filter_conditions.items():
            values_as_str = ", ".join([f'"{value}"' for value in values])
            formatted_conditions.append(f"{field}: ANY({values_as_str})")
        return " ".join(formatted_conditions)

    def search(
        self,
        query: str,
        page_size: int = 10,
        filter_conditions: Optional[Dict[str, List[str]]] = None,
        with_snippet: bool = False,
        with_summary: bool = False,
    ) -> Optional[SearchPager]:
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
                snippet_spec=discoveryengine.SearchRequest.ContentSearchSpec.SnippetSpec(
                    max_snippet_count=1  # Only one snippet per document.
                )
                if with_snippet
                else None,
                summary_spec=discoveryengine.SearchRequest.ContentSearchSpec.SummarySpec(
                    summary_result_count=10  # The number of documents used to generate the summary. Maximum is 10.
                )
                if with_summary
                else None,
            )
            request = discoveryengine.SearchRequest(
                serving_config=self.serving_config,
                query=query,
                page_size=page_size,
                filter=DiscoveryEngineInterface._format_filter_condition(
                    filter_conditions
                )
                if filter_conditions
                else None,
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
            client = discoveryengine.SearchServiceClient()
            response = client.search(request)
            return response

        # Log errors and return an empty list.
        except GoogleAPICallError as e:
            logger.error("API call error during the search: %s", e)
        except RetryError as e:
            logger.error("Retry error during the search: %s", e)
        except Exception as e:
            logger.error("An error occurred during the search: %s", e)
        return None
