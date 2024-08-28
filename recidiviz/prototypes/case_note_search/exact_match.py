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
Returns all case notes that have an exact match (case-insensitive) to the query term.

For sanity checking these functions, you can run this file using:
pipenv run python -m recidiviz.prototypes.case_note_search.exact_match
"""

import json
from typing import Any, Dict, List, Optional

import pandas as pd
import pandas_gbq

from recidiviz.utils.environment import GCP_PROJECT_STAGING

CASE_NOTES_BQ_TABLE_NAME = (
    "recidiviz-staging.case_notes_prototype_views.case_notes_materialized"
)

ID_COLUMN_NAME = "id"
DATA_COLUMN_NAME = "jsonData"

# Maps frontend state codes to state codes used in the data storage backend system.
STATE_CODE_CONVERTER = {"us_id": "US_IX", "us_me": "US_ME"}

# We use this set for validation purposes - to catch misuse early and have helpful error
# responses.
SUPPORTED_FILTER_FIELDS = {"state_code", "external_id", "note_type"}


def validate_state_codes(state_codes: List[str]) -> None:
    """Validates that the provided state code is supported."""
    for state_code in state_codes:
        if state_code not in STATE_CODE_CONVERTER.values():
            raise ValueError(
                f"State code {state_code} is not present in the state code converter. Supported values are {list(STATE_CODE_CONVERTER.values())}"
            )


def validate_includes_excludes(includes_excludes: Dict[str, List[str]]) -> None:
    """Detect if the user is trying to filter on unsupported field values."""
    for field, values in includes_excludes.items():
        if field not in SUPPORTED_FILTER_FIELDS:
            raise ValueError(
                f"Filter field {field} is not supported by exact match search. Supported values are {list(SUPPORTED_FILTER_FIELDS)}"
            )
        # Validate state code inputs.
        if field == "state_code":
            validate_state_codes(values)


def exact_match_search(
    query_term: str,
    include_filter_conditions: Optional[Dict[str, List[str]]] = None,
    exclude_filter_conditions: Optional[Dict[str, List[str]]] = None,
    limit: Optional[int] = 20,
) -> Dict[str, Any]:
    """
    Load case notes from bigquery that contain the query substring and are present
    in the list of external_ids and state_codes.

    Returns a map from document_id to the document jsonData.
    """
    if include_filter_conditions is None:
        include_filter_conditions = {}
    if exclude_filter_conditions is None:
        exclude_filter_conditions = {}
    # Validate includes and excludes fields, to aid in debugging.
    validate_includes_excludes(include_filter_conditions)
    validate_includes_excludes(exclude_filter_conditions)

    # Base query that filters for a substring match.
    query = f"""
    select *
    from {CASE_NOTES_BQ_TABLE_NAME}
    where LOWER(JSON_EXTRACT_SCALAR(JsonData, '$.note_body')) LIKE LOWER('%{query_term}%')
    """

    def format_condition(values: List[str]) -> str:
        """Helper function to format filter conditions."""
        return "','".join(values)

    # Incorporate include filter conditions.
    for field, values in include_filter_conditions.items():
        formatted_values = format_condition(values)
        query = (
            query
            + f"and JSON_EXTRACT_SCALAR(JsonData, '$.{field}') in ('{formatted_values}')"
        )

    # Incorporate exclude filter conditions.
    for field, values in exclude_filter_conditions.items():
        formatted_values = format_condition(values)
        query = (
            query
            + f"and not JSON_EXTRACT_SCALAR(JsonData, '$.{field}') in ('{formatted_values}')"
        )

    # Optionally limit the number of results returned, since loading a ton of notes from
    # bigquery is slow.
    if limit is not None:
        query = query + f"limit {limit}"

    contains_exact_match_df: pd.DataFrame = pandas_gbq.read_gbq(
        query, project_id=GCP_PROJECT_STAGING
    )

    # Return a dict instead of a dataframe object.
    document_id_to_data: Dict = {}
    for _, row in contains_exact_match_df.iterrows():
        document_id = row[ID_COLUMN_NAME]
        json_data = json.loads(row[DATA_COLUMN_NAME])
        document_id_to_data[document_id] = json_data

    return document_id_to_data
