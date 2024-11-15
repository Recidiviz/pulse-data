# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
Helper methods for the sentence sessions view builders
"""
from typing import List

from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans


def convert_normalized_ledger_data_to_sessions_query(
    table_name: str,
    index_columns: List[str],
    update_column_name: str,
    attribute_columns: List[str],
) -> str:
    """Return a view query that sessionizes a normalized state sentence table across the set of attribute columns"""
    index_columns_string = list_to_query_string(index_columns)
    attribute_columns_string = list_to_query_string(attribute_columns)
    return f"""
WITH 
-- Convert the update date column into start_date and end_date columns
parsed_spans AS (
    SELECT
        {index_columns_string},
        {attribute_columns_string},
        {update_column_name} AS start_date,
        LEAD({update_column_name}) OVER (
            PARTITION BY {index_columns_string}
            ORDER BY {update_column_name}
        ) AS end_date_exclusive,
    FROM `{{project_id}}.normalized_state.{table_name}`
),
-- Collapse any adjacent spans that have the same attribute values
collapsed_spans AS (
{aggregate_adjacent_spans(
    table_name="parsed_spans",
    index_columns=index_columns,
    attribute=attribute_columns,
    end_date_field_name="end_date_exclusive",
)}
)
-- Format the query output and drop 0 day spans
SELECT
    {index_columns_string},
    DATE(start_date) AS start_date,
    DATE(end_date_exclusive) AS end_date_exclusive,
    {attribute_columns_string},
FROM collapsed_spans
-- Drop zero day spans
WHERE CAST(start_date AS DATE) != {nonnull_end_date_clause("CAST(end_date_exclusive AS DATE)")}
"""
