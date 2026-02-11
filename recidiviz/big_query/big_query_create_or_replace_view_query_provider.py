# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Query providers for setting column descriptions on BigQuery views."""

from recidiviz.big_query.big_query_query_provider import BigQueryQueryProvider
from recidiviz.big_query.big_query_utils import escape_backslashes_for_query
from recidiviz.big_query.big_query_view import BigQueryView


def _escape_description(description: str) -> str:
    """Escapes a description string for use in a triple-quoted BigQuery OPTIONS
    clause (e.g. OPTIONS(description='''...''')). Escapes backslashes and single quotes.
    """
    [escaped] = escape_backslashes_for_query([description])
    escaped = escaped.replace("'", "\\'")
    return escaped


class CreateOrReplaceViewQueryProvider(BigQueryQueryProvider):
    """Generates a CREATE OR REPLACE VIEW statement that includes column
    descriptions inline via OPTIONS clauses.

    See: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_view_statement

    Example output:
        CREATE OR REPLACE VIEW `project.dataset.view`
        (
          col1 OPTIONS(description='''Description 1'''),
          col2 OPTIONS(description='''Description 2''')
        )
        OPTIONS(description='''View description''')
        AS
        SELECT col1, col2 FROM ...
    """

    def __init__(
        self,
        *,
        # The view object to create the view from
        view: BigQueryView,
        # Project where the view should be created
        project_id: str,
        # True to only use emulator-safe SQL (no column descriptions)
        # TODO(#58592): remove this arg once the emulator supports the full query
        use_emulator_sql: bool = False,
    ) -> None:
        self._view = view
        self._project_id = project_id
        self._use_emulator_sql = use_emulator_sql

    def get_query(self) -> str:
        project_specific_address = self._view.address.to_project_specific_address(
            self._project_id
        )
        formatted_address = project_specific_address.format_address_for_query()

        view_query = self._view.view_query

        parts = [f"CREATE OR REPLACE VIEW {formatted_address}"]

        if self._view.schema and not self._use_emulator_sql:
            column_defs = []
            for column in self._view.schema:
                escaped_desc = _escape_description(column.description)
                column_defs.append(
                    f"  {column.name} OPTIONS(description='''{escaped_desc}''')"
                )
            parts.append("(\n" + ",\n".join(column_defs) + "\n)")

        if self._view.bq_description:
            escaped_view_desc = _escape_description(self._view.bq_description)
            parts.append(f"OPTIONS(description='''{escaped_view_desc}''')")

        parts.append("AS")
        parts.append(view_query)

        return "\n".join(parts)
