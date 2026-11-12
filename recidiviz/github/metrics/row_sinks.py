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
"""Destinations for the rows the GitHub metrics crawl produces."""
import json
from pathlib import Path
from typing import Any, Protocol

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient

# Seconds to wait for one load job to finish before the crawl fails.
_LOAD_TIMEOUT_SECONDS = 300


class RowSink(Protocol):
    """Writes a batch of rows to one table."""

    def write(self, address: BigQueryAddress, rows: list[dict[str, Any]]) -> None:
        """Writes rows to the table at address, appending to what is there."""


class BigQueryRowSink(RowSink):
    """Appends rows to BigQuery with one load job per write, and waits for each
    job to finish so that writes land in the order they are made."""

    def __init__(self, bq_client: BigQueryClient) -> None:
        self._bq_client = bq_client

    def write(self, address: BigQueryAddress, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        job = self._bq_client.load_into_table_async(address=address, rows=rows)
        job.result(timeout=_LOAD_TIMEOUT_SECONDS)


class NdjsonRowSink(RowSink):
    """Appends rows to one newline-delimited JSON file per table, for example
    <output_dir>/files.ndjson, so a local run never writes to BigQuery."""

    def __init__(self, output_dir: Path) -> None:
        self._output_dir = output_dir

    def write(self, address: BigQueryAddress, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        self._output_dir.mkdir(parents=True, exist_ok=True)
        path = self._output_dir / f"{address.table_id}.ndjson"
        with path.open("a", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")
