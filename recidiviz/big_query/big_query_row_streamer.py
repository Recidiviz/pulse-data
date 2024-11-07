# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Class that can be used for streaming rows into a given table."""
from typing import Any, Dict, Sequence

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient


class BigQueryRowStreamer:
    """Class that can be used for streaming rows into a given table."""

    def __init__(
        self,
        bq_client: BigQueryClient,
        table_address: BigQueryAddress,
        expected_table_schema: list[bigquery.SchemaField],
    ) -> None:
        self.bq_client = bq_client
        self.table_address = table_address
        self.expected_table_schema = expected_table_schema

    def _check_row_has_expected_columns(self, row: dict[str, Any]) -> None:
        row_columns = set(row.keys())
        expected_columns = set(c.name for c in self.expected_table_schema)

        if missing := expected_columns - row_columns:
            raise ValueError(
                f"Cannot output to table [{self.table_address.to_str()}] - rows "
                f"missing columns {sorted(missing)}."
            )

        if extra := row_columns - expected_columns:
            raise ValueError(
                f"Cannot output to table [{self.table_address.to_str()}] - rows "
                f"have extra unexpected columns {sorted(extra)}."
            )

    def stream_rows(self, rows: Sequence[Dict[str, Any]]) -> None:
        """Streams the provided rows into the streamer's table."""
        if not rows:
            return

        self._check_row_has_expected_columns(rows[0])
        self.bq_client.stream_into_table(self.table_address, rows)
