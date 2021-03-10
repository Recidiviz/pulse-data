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
Script for identifying all of the fields in our schema which are *not* filled in for a given state.

This script works by querying each table in the state dataset in BigQuery to identify the full set of columns that
have only null values for rows with a given state code.

This is useful for identifying and documenting which parts of our schema are "filled in" for a given state.
Example usage (run from `pipenv shell`):

python -m recidiviz.tools.schema_hydration_doc_generator --project-id recidiviz-staging --state-code us_nd
"""

import argparse
import logging
from typing import List, Tuple

from pytablewriter import MarkdownTableWriter

from recidiviz.big_query.big_query_client import BigQueryClientImpl, BigQueryClient
from recidiviz.persistence.database.schema_utils import (
    get_non_history_state_database_entities,
    get_state_database_entity_with_name,
)


_ANY_ROWS_QUERY = """
SELECT COUNT(*) as count FROM `{project_id}.state.{table_name}` WHERE state_code = '{state_code}';
"""


# pylint:disable=anomalous-backslash-in-string
_SEARCH_QUERY = """
SELECT null_column
FROM `{project_id}.state.{table_name}` AS t,
  UNNEST(REGEXP_EXTRACT_ALL(
    TO_JSON_STRING(t),
    r'\"([a-zA-Z0-9\_]+)\":null')
  ) AS null_column
WHERE state_code = '{state_code}'
GROUP BY null_column
HAVING COUNT(*) = (SELECT COUNT(*) FROM `{project_id}.state.{table_name}` WHERE state_code = '{state_code}');
"""


def _get_all_table_names() -> List[Tuple[str, str]]:
    return sorted(
        [
            (e.__name__, e.get_entity_name())
            for e in get_non_history_state_database_entities()
        ]
    )


def _get_all_columns_for_table(table_name: str) -> List[str]:
    entity_type = get_state_database_entity_with_name(table_name)
    return sorted(entity_type.get_column_property_names())


def _get_all_null_columns(
    bq_client: BigQueryClient, project_id: str, table_name: str, state_code: str
) -> List[str]:
    formatted_query = _SEARCH_QUERY.format(
        project_id=project_id, state_code=state_code, table_name=table_name
    )
    query_job = bq_client.run_query_async(formatted_query)
    return sorted([row["null_column"] for row in query_job])


def _has_any_rows(
    bq_client: BigQueryClient, project_id: str, table_name: str, state_code: str
) -> bool:
    formatted_query = _ANY_ROWS_QUERY.format(
        project_id=project_id, state_code=state_code, table_name=table_name
    )
    query_job = bq_client.run_query_async(formatted_query)
    for row in query_job:
        return int(row["count"]) > 0

    raise ValueError(
        f"Unable to check for {state_code} rows in {project_id}.state.{table_name}"
    )


def _create_schema_hydration_docs(project_id: str, state_code: str) -> str:
    """Produces documentation on schema hydration.

    This is a series of Markdown tables for each entity in the state schema, one Markdown table per entity.
    Each row in a Markdown table corresponds to a field on that entity and includes whether it is hydrated by our
    current ingest mappings and a "Source" column to be populated manually, *for now*.
    """
    table_names = _get_all_table_names()

    bq_client = BigQueryClientImpl(project_id=project_id)

    documentation = ""

    for entity, table in table_names:
        all_columns = _get_all_columns_for_table(entity)

        has_any_rows = _has_any_rows(bq_client, project_id, table, state_code)

        null_columns = (
            all_columns
            if not has_any_rows
            else _get_all_null_columns(bq_client, project_id, table, state_code)
        )

        def _column_checkbox(column: str, empty_columns: List[str]) -> str:
            return "NO" if column in empty_columns else "YES"

        table_matrix = [
            [
                _column_checkbox(column, null_columns),
                column,
                "          ",
            ]
            for column in all_columns
        ]
        writer = MarkdownTableWriter(
            headers=["Filled In?", "Column", "Source"],
            value_matrix=table_matrix,
            margin=1,
        )

        documentation += (
            f"## {table}\n\n*Ingested? {has_any_rows}*\n\n{writer.dumps()}\n\n"
        )

    return documentation


def main(*, project_id: str, state_code: str) -> None:
    docs = _create_schema_hydration_docs(project_id, state_code)
    print(docs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--project-id", required=True, help="The project_id for the destination table"
    )
    parser.add_argument(
        "--state-code", required=True, help="The state_code to search with"
    )

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    main(project_id=args.project_id, state_code=args.state_code)
