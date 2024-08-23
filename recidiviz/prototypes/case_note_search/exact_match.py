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
from typing import Any, Dict

import pandas as pd
import pandas_gbq

from recidiviz.utils.environment import GCP_PROJECT_STAGING

CASE_NOTES_BQ_TABLE_NAME = (
    "recidiviz-staging.bpacker_case_notes_prototype_views.case_notes_materialized"
)

ID_COLUMN_NAME = "id"
DATA_COLUMN_NAME = "jsonData"

STATE_CODE_CONVERTER = {"us_id": "US_IX", "us_me": "US_ME"}


def load_all_case_notes(external_id: str, state_code: str) -> pd.DataFrame:
    # Convert state codes into how they are represented in the BQ view.
    if state_code.lower() in STATE_CODE_CONVERTER:
        state_code = STATE_CODE_CONVERTER[state_code]

    query = f"""
    select *
    from {CASE_NOTES_BQ_TABLE_NAME}
    where JSON_EXTRACT_SCALAR(JsonData, '$.external_id') = '{external_id}'
    and JSON_EXTRACT_SCALAR(JsonData, '$.state_code') = '{state_code}'
    """
    return pandas_gbq.read_gbq(query, project_id=GCP_PROJECT_STAGING)


def filter_dataframe_by_note_body(df: pd.DataFrame, query_term: str) -> pd.DataFrame:
    """
    Filters the DataFrame to return only rows where the JsonData.note_body field
    contains an exact match of the query_term.
    """

    def contains_exact_match(json_str: str, query_term: str) -> bool:
        try:
            data = json.loads(json_str)
            return query_term.lower() in data.get("note_body").lower()
        except Exception:
            return False

    filtered_df = df[
        df[DATA_COLUMN_NAME].apply(contains_exact_match, query_term=query_term)
    ]
    return filtered_df


def exact_match_search(
    query_term: str, external_id: str, state_code: str
) -> Dict[str, Any]:
    """The current implementation reads all case notes from the BQ materialized view,
    filters by state_code and external_id, and then returns all case notes that have an
    exact match to the search term.

    Returns a map from document_id to the document data.
    """
    all_case_notes: pd.DataFrame = load_all_case_notes(
        external_id=external_id, state_code=state_code
    )

    contains_exact_match_df: pd.DataFrame = filter_dataframe_by_note_body(
        all_case_notes, query_term=query_term
    )

    # Return a dict instead of a dataframe object.
    document_id_to_data: Dict = {}
    for _, row in contains_exact_match_df.iterrows():
        document_id = row[ID_COLUMN_NAME]
        json_data = json.loads(row[DATA_COLUMN_NAME])
        document_id_to_data[document_id] = json_data

    return document_id_to_data
