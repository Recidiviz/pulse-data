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

import json
from typing import Any, Dict, List, Optional

from google.cloud.discoveryengine_v1.services.search_service.pagers import SearchPager

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.tools.prototypes.discovery_engine import DiscoveryEngineInterface
from recidiviz.tools.prototypes.gcs_bucket_reader import GCSBucketReader
from recidiviz.utils.environment import GCP_PROJECT_STAGING

IDAHO_CASE_NOTES_ENGINE_ID = "id-case-notes-new_1717011992933"


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
    snippet: Optional[str],
    extractive_answer: Optional[str],
    full_case_note: Optional[str],
    n: int = 30,
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


def extract_case_notes_responses(pager: SearchPager) -> List[Dict[str, Any]]:
    """
    Extract relevant information from a SearchPager object for case note search results.
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
                f"Could not parse SearchPager results. document.id = {document_id}"
            ) from e
    return results


def case_note_search(
    query: str,
    page_size: int = 10,
    filter_conditions: Optional[Dict[str, List[str]]] = None,
    with_snippet: bool = False,
) -> List[Dict[str, Any]]:
    """
    Perform a case note search using the Discovery Engine interface.

    Args:
        query: The search query string.
        page_size: The number of search results to return per page. Defaults to 10.
        filter_conditions: Optional. A dictionary of filter conditions where keys are
            field names and values are lists of acceptable values for those fields.
            (e.g. {"external_id": ["1234", "6789"]})
        with_snippet: Include snippets in the search results. Defaults to False.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing case note information.
        Each dictionary contains the following keys:
            - 'document_id' (str): The ID of the document.
            - 'date' (Optional[str]): The date associated with the document.
            - 'contact_mode' (Optional[str]): The contact mode associated with the document.
            - 'note_type' (Optional[str]): The note type associated with the document.
            - 'link' (Optional[str]): A link to the document.
            - 'extractive_answer' (Optional[str]): An extractive answer. Is null if snippet is populated.
            - 'snippet' (Optional[str]): A snippet extracted from the document, if requested.
            - 'case_note' (Optional[str]): The full case note content downloaded from GCS.

        In the case of an error, this instead returns [{"error": "description"}].
    """
    try:
        discovery_interface = DiscoveryEngineInterface(
            project_id=GCP_PROJECT_STAGING,
            engine_id=IDAHO_CASE_NOTES_ENGINE_ID,
        )
        search_pager = discovery_interface.search(
            query=query,
            page_size=page_size,
            filter_conditions=filter_conditions,
            with_snippet=with_snippet,
        )
        return extract_case_notes_responses(search_pager)
    except Exception as e:
        return [{"error": str(e)}]


if __name__ == "__main__":
    # Example usage of case_note_search.
    case_note_response = case_note_search(
        query="job status",
        page_size=1,
        with_snippet=True,
    )
    print(json.dumps(case_note_response, indent=2, default=str))
