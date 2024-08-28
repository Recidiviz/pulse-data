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

STATE_CODE_CONVERTER = {"us_id": "US_IX", "us_me": "US_ME"}


def validate_state_codes(state_codes: List[str]) -> None:
    """Validates that the provided state code is supported."""
    for state_code in state_codes:
        if state_code not in STATE_CODE_CONVERTER.values():
            raise ValueError(
                f"State code {state_code} is not present in the state code converter."
            )


def exact_match_search(
    query_term: str,
    external_ids: Optional[List[str]] = None,
    state_codes: Optional[List[str]] = None,
    limit: Optional[int] = 20,
) -> Dict[str, Any]:
    """
    Load case notes from bigquery that contain the query substring and are present
    in the list of external_ids and state_codes.

    Returns a map from document_id to the document jsonData.
    """
    # Base query that filters for a substring match.
    query = f"""
    select *
    from {CASE_NOTES_BQ_TABLE_NAME}
    where LOWER(JSON_EXTRACT_SCALAR(JsonData, '$.note_body')) LIKE LOWER('%{query_term}%')
    """

    # Optionally add external_ids condition
    if external_ids is not None:
        format_external_ids = "','".join(external_ids)
        query = (
            query
            + f"and JSON_EXTRACT_SCALAR(JsonData, '$.external_id') in ('{format_external_ids}')"
        )

    # Optionally add state_codes condition
    if state_codes is not None:
        validate_state_codes(state_codes)
        format_state_codes = "','".join(state_codes)
        query = (
            query
            + f"and JSON_EXTRACT_SCALAR(JsonData, '$.state_code') in ('{format_state_codes}')"
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
