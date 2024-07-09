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
connects to the Discovery Engine using the Google Cloud client library.

Usage:

pipenv run python -m recidiviz.tools.prototypes.discovery_engine \
  --page-size=5 --external-id="{external_id}" --query="{query}"

"""

import argparse
import logging
from functools import cached_property
from typing import List, Optional

import attr
from google.api_core.exceptions import GoogleAPICallError, RetryError
from google.cloud import discoveryengine_v1 as discoveryengine
from google.cloud.discoveryengine_v1.services.search_service.pagers import SearchPager

from recidiviz.utils.environment import GCP_PROJECT_STAGING

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

engine_id = "id-case-notes-new_1717011992933"


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--query",
        type=str,
        required=True,
        help="The query to search with.",
    )
    parser.add_argument(
        "--external-id",
        type=str,
        required=False,
        default=None,
        help="Filter the search results to only return documents with this external id."
        "If external_id is None, the search results will not be filtered by external id.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        required=False,
        default=5,
        help="The maximum number of results to return. The service may return fewer "
        "results than this value if there are not enough documents that relate to the "
        "provided query.",
    )
    return parser


@attr.define(kw_only=True)
class DiscoveryEngineInterface:
    """An interface for querying Google Vertex AI's Discovery Engine. This interface
    connects to the Discovery Engine using the Google Cloud client library.
    """

    project_id: str

    engine_id: str

    @cached_property
    def serving_config(self) -> str:
        """The resource name of the Search serving configuration."""
        return f"projects/{self.project_id}/locations/global/collections/default_collection/engines/{self.engine_id}/servingConfigs/default_config"

    @staticmethod
    def get_external_id_filter_condition(external_id: str) -> str:
        """Return a filter condition that filters search results by external id."""
        return f'external_id: ANY("{external_id}")'

    @staticmethod
    def extract_document_ids(
        search_pager: SearchPager,
    ) -> List[str]:
        """Extract document ids from a Discovery Engine SearchResponse."""
        return [result.document.id for result in search_pager.results]

    def search_for_document_ids(
        self,
        query: str,
        page_size: int = 10,
        external_id: Optional[str] = None,
    ) -> List[str]:
        """Return the document ids that are the most relevant to the search query.

        Parameters:
            query (str): The query to search with.

            external_id (str): Filter the search results to only return documents with
                this external id. If external_id is None, the search results will not be
                filtered by external id.

            page_size (int): The maximum number of results to return. The service may
                return fewer results than this value if there are not enough documents
                that relate to the provided query.
        """
        try:
            # Refer to the `SearchRequest` reference for all supported fields:
            # https://cloud.google.com/python/docs/reference/discoveryengine/latest/google.cloud.discoveryengine_v1.types.SearchRequest
            request = discoveryengine.SearchRequest(
                serving_config=self.serving_config,
                query=query,
                page_size=page_size,
                filter=DiscoveryEngineInterface.get_external_id_filter_condition(
                    external_id
                )
                if external_id is not None
                else None,
                query_expansion_spec=discoveryengine.SearchRequest.QueryExpansionSpec(
                    condition=discoveryengine.SearchRequest.QueryExpansionSpec.Condition.AUTO,
                ),
                spell_correction_spec=discoveryengine.SearchRequest.SpellCorrectionSpec(
                    mode=discoveryengine.SearchRequest.SpellCorrectionSpec.Mode.AUTO
                ),
            )
            client = discoveryengine.SearchServiceClient()
            response = client.search(request)
            return DiscoveryEngineInterface.extract_document_ids(response)

        # Log errors and return an empty list.
        except GoogleAPICallError as e:
            logger.error("API call error during the search: %s", e)
        except RetryError as e:
            logger.error("Retry error during the search: %s", e)
        except Exception as e:
            logger.error("An error occurred during the search: %s", e)
        return []


if __name__ == "__main__":
    args = create_parser().parse_args()

    # Example usage of the DiscoveryEngineInterface class.
    discovery_interface = DiscoveryEngineInterface(
        project_id=GCP_PROJECT_STAGING,
        engine_id=engine_id,
    )
    result = discovery_interface.search_for_document_ids(
        query=args.query,
        page_size=args.page_size,
        external_id=args.external_id,
    )
    print(result)
