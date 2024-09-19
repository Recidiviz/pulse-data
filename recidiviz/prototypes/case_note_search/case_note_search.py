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
"""
For performing case note searches using Google Vertex AI's Discovery Engine and
extracting relevant information from the search results.

For sanity checking these functions, you can run this file using:
pipenv run python -m recidiviz.prototypes.case_note_search.case_note_search

"""

import asyncio
import json
from typing import Any, Dict, List, Optional

import dateutil.parser
import sentry_sdk
from google.cloud.discoveryengine_v1.services.search_service.pagers import (
    SearchAsyncPager,
)

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.prototypes.case_note_search.exact_match import exact_match_search
from recidiviz.tools.prototypes.discovery_engine import DiscoveryEngineInterface
from recidiviz.tools.prototypes.gcs_bucket_reader import GCSBucketReader
from recidiviz.utils.environment import GCP_PROJECT_STAGING

IDAHO_CASE_NOTES_ENGINE_ID = "id-case-notes-new_1717011992933"
FAKE_CASE_NOTES_ENGINE_ID = "fake-case-note-search_1723492600856"
CASE_NOTE_SEARCH_ENGINE_ID = "case-note-search_1723818974584"

# The discovery engine returns different results depending on whether we are using
# structured or unstructured data.
STRUCTURED_DATASETS = {CASE_NOTE_SEARCH_ENGINE_ID}

# We currently only support exact match for case notes stored in BigQuery.
EXACT_MATCH_SUPPORTED = {CASE_NOTE_SEARCH_ENGINE_ID}


def set_hardcoded_excludes() -> Dict[str, List[str]]:
    """The case note types we are excluding are:
        * `Investigation (Confidential)` - only occurs in Maine notes.
        * `Mental Health (Confidential)` - only occurs in Maine notes.
        * `FIAT - Confidential` - only occurs in Maine notes.

    These note types are only present in Maine, but we are excluding the note types from
    all case note search results. This is a simpler exclude-logic to get right, and
    should be fine for now.

    TODO(#32811): Extend excludes logic to support different roles other than POs.

    Note: The includes/excludes matching is case-sensitive.
    """
    return {
        "note_type": [
            "Investigation (Confidential)",
            "Mental Health (Confidential)",
            "FIAT - Confidential",
        ]
    }


def download_full_case_note(gcs_link: str) -> Optional[str]:
    """Download the full case note document from GCS."""
    gcs_file = GcsfsFilePath.from_absolute_path(gcs_link)
    gcs_reader = GCSBucketReader(bucket_name=gcs_file.bucket_name)
    case_note = gcs_reader.download_blob(blob_name=gcs_file.blob_name)
    return case_note


def get_extractive_answer(result: Any) -> Optional[str]:
    """Gets the extractive answer from the search result, if one exists."""
    extractive_answers = result.document.derived_struct_data.get(
        "extractive_answers", []
    )
    if extractive_answers:
        return extractive_answers[0].get("content", None)
    return None


def get_snippet(result: Any) -> Optional[str]:
    """Gets a snippet from the search result, if one exists."""
    snippets = result.document.derived_struct_data.get("snippets", None)
    if not snippets:
        return None

    # Expects only one snippet.
    snippet = snippets[0]
    if not snippet.get("snippet_status", None) == "SUCCESS":
        return None
    return snippet.get("snippet", None)


def get_preview(
    full_case_note: Optional[str],
    snippet: Optional[str] = None,
    extractive_answer: Optional[str] = None,
    n: int = 250,
) -> str:
    """Generates the preview to be displayed in the frontend.

    If snippet is present, use the snippet.
    Else use the extractive_answer if it is present.
    If neither are present, use the first n characters of the case note.
    """
    if snippet:
        return snippet
    if extractive_answer:
        return extractive_answer
    if full_case_note:
        return full_case_note[:n]
    raise ValueError("No case note generated.")


def exact_match_json_data_to_results(json_data: Dict[str, Any]) -> Dict[str, Any]:
    """Converts a jsonData dict (the format stored in BigQuery) into a Results dict (the
    format this endpoint returns).

    The jsonData object is a dict with the following fields:
        * state_code
        * external_id
        * note_id
        * note_body
        * note_title
        * note_date
        * note_type
        * note_mode
    """
    return {
        "document_id": json_data.get("note_id", None),
        "date": json_data.get("note_date", None),
        "contact_mode": json_data.get("note_mode", None),
        "note_type": json_data.get("note_type", None),
        "note_title": json_data.get("note_title", None),
        "extractive_answer": None,
        "snippet": None,
        "preview": get_preview(json_data.get("note_body", None)),
        "case_note": json_data.get("note_body", None),
    }


def extract_case_notes_results_unstructured_data(
    pager: SearchAsyncPager,
) -> List[Dict[str, Any]]:
    """
    Extract relevant information from a SearchAsyncPager object for case note search results.
    """
    results = []
    for result in pager.results:
        document_id = result.document.id
        try:
            gcs_link = result.document.derived_struct_data.get("link")
            full_case_note = download_full_case_note(gcs_link)
            extractive_answer = get_extractive_answer(result)
            snippet = get_snippet(result)
            results.append(
                {
                    "document_id": document_id,
                    "date": result.document.struct_data.get("date", None),
                    "contact_mode": result.document.struct_data.get(
                        "contact_mode", None
                    ),
                    "note_type": result.document.struct_data.get("note_type", None),
                    "note_title": None,
                    "extractive_answer": extractive_answer,
                    "snippet": snippet,
                    "preview": get_preview(
                        extractive_answer=extractive_answer,
                        snippet=snippet,
                        full_case_note=full_case_note,
                    ),
                    "case_note": full_case_note,
                }
            )
        except Exception as e:
            raise ValueError(
                f"Could not parse SearchAsyncPager results. document.id = {document_id}"
            ) from e
    return results


def get_note_body(result: Any) -> Optional[str]:
    """Gets the note body from the search result."""
    note_body = result.document.struct_data.get("note_body", None)
    return note_body


def extract_case_notes_results_structured_data(
    pager: SearchAsyncPager,
) -> List[Dict[str, Any]]:
    """
    Extract relevant information from a SearchAsyncPager object for case note search results.
    """
    results = []
    for result in pager.results:
        document_id = result.document.id
        try:
            full_case_note = get_note_body(result)
            results.append(
                {
                    "document_id": document_id,
                    "date": result.document.struct_data.get("note_date", None),
                    "contact_mode": result.document.struct_data.get("note_mode", None),
                    "note_type": result.document.struct_data.get("note_type", None),
                    "note_title": result.document.struct_data.get("note_title", None),
                    "extractive_answer": None,
                    "snippet": None,
                    "preview": get_preview(
                        full_case_note=full_case_note,
                    ),
                    "case_note": full_case_note,
                }
            )
        except Exception as e:
            raise ValueError(
                f"Could not parse SearchAsyncPager results. document.id = {document_id}"
            ) from e
    return results


async def case_note_search(
    query: str,
    page_size: int = 10,
    filter_conditions: Optional[Dict[str, List[str]]] = None,
    with_snippet: bool = False,
) -> Dict[str, Any]:
    """
    Perform a case note search using the Discovery Engine interface.

    Results are returned in the following order:
    * Discovery Engine results that are exact matches (in Discovery Engine order)
    * Exact match results missed by Discovery Engine (in exact match order, which is reverse-chronological)
    * All other Discovery Engine results (in Discovery Engine order)

    Args:
        query: The search query string.
        page_size: The number of search results to return per page. Defaults to 10.
        filter_conditions: Optional. A dictionary of filter conditions where keys are
            field names and values are lists of acceptable values for those fields.
            (e.g. {"external_id": ["1234", "6789"]})
        with_snippet: Include snippets in the search results. Defaults to False.

    Returns (Dict[str, Any]):
        In the success case, returns {"results": [<case_note_data>, <case_note_data>], "error": None},
        where results is a list of dictionaries containing case note information.
        Each case note dictionary has the following keys:
            - 'document_id' (str): The ID of the document.
            - 'date' (Optional[str]): The date associated with the document.
            - 'contact_mode' (Optional[str]): The contact mode associated with the document.
            - 'note_type' (Optional[str]): The note type associated with the document.
            - 'link' (Optional[str]): A link to the document.
            - 'extractive_answer' (Optional[str]): An extractive answer. Is null if snippet is populated.
            - 'snippet' (Optional[str]): A snippet extracted from the document, if requested.
            - 'case_note' (Optional[str]): The full case note content downloaded from GCS.
            - 'relevance_ordering' (Optional[int]): Used for sorting the notes by
                relevance to the query term. '0' being the most relevant case note.

        In the error case, the response is {"results": [], "error": <error_description>}.
    """

    # Use real case notes for local development, staging, and production development.
    engine_id = CASE_NOTE_SEARCH_ENGINE_ID

    # Explicitly disable snippets if running the engine on structured data, since
    # snippets, extractive answers, and summaries are disabled for structured data stores.
    # See https://github.com/googleapis/google-cloud-ruby/issues/26036.
    if engine_id in STRUCTURED_DATASETS:
        with_snippet = False

    try:
        discovery_interface = DiscoveryEngineInterface(
            project_id=GCP_PROJECT_STAGING,
            engine_id=engine_id,
        )
        search_pager = await discovery_interface.search(
            query=query,
            page_size=page_size,
            include_filter_conditions=filter_conditions,
            exclude_filter_conditions=set_hardcoded_excludes(),
            with_snippet=with_snippet,
        )
        if engine_id in STRUCTURED_DATASETS:
            results = extract_case_notes_results_structured_data(search_pager)
        else:
            results = extract_case_notes_results_unstructured_data(search_pager)

        # Supplement results with exact match results.
        # For now, we ONLY support exact match supplementation for engines with data in
        # BigQuery.
        if engine_id in EXACT_MATCH_SUPPORTED:
            exact_match_json_data: Dict[str, Any] = exact_match_search(
                query_term=query,
                include_filter_conditions=filter_conditions,
                exclude_filter_conditions=set_hardcoded_excludes(),
                limit=page_size,
            )
            # Exact matches are formatted as jsonData currently, and need to be
            # reformatted as Results.
            exact_match_results = [
                exact_match_json_data_to_results(json_data)
                for json_data in exact_match_json_data.values()
            ]

            # Sort the exact matches in reverse chronological order
            exact_match_results.sort(
                key=lambda result: dateutil.parser.parse(result["date"]), reverse=True
            )

            # Results are combined in the following order:
            # * Discovery Engine results that are exact matches (in Discovery Engine order)
            # * Exact match results missed by Discovery Engine (in exact match order, which is reverse-chronological)
            # * All other Discovery Engine results (in Discovery Engine order)
            discovery_engine_ids = [result["document_id"] for result in results]
            exact_match_ids = [result["document_id"] for result in exact_match_results]

            consolidated_results = [
                result for result in results if result["document_id"] in exact_match_ids
            ]
            consolidated_results.extend(
                [
                    result
                    for result in exact_match_results
                    if result["document_id"] not in discovery_engine_ids
                ]
            )
            consolidated_results.extend(
                [
                    result
                    for result in results
                    if result["document_id"] not in exact_match_ids
                ]
            )

            results = consolidated_results

            # Truncate, if the page size is exceeded.
            results = results[:page_size]

        # Attach 'relevance_ordering' to each result.
        for index, result in enumerate(results):
            result["relevance_ordering"] = index

        return {
            "results": results,
            "error": None,
        }
    except Exception as e:
        sentry_sdk.capture_exception(e)
        return {
            "results": [],
            "error": str(e),
        }


if __name__ == "__main__":
    # Use asyncio to run the async function. This is necessary for running an async
    # function in a syncronous environment.
    case_note_response = asyncio.run(
        case_note_search(
            query="job status",
            page_size=10,
            with_snippet=True,
            filter_conditions={"state_code": ["US_ME"]},
        )
    )

    print(json.dumps(case_note_response, indent=2, default=str))
