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
"""Tests for the sinks the GitHub metrics crawl writes rows to."""
import json
from pathlib import Path
from unittest import mock

from recidiviz.github.metrics.row_sinks import BigQueryRowSink, NdjsonRowSink
from recidiviz.github.metrics.tables import CHILD_TABLE_ADDRESSES, PULL_REQUESTS_ADDRESS

[FILES_ADDRESS] = [a for a in CHILD_TABLE_ADDRESSES if a.table_id == "files"]

_ROWS = [
    {"repository_full_name": "Recidiviz/synthetic-repo", "pr_number": 1},
    {"repository_full_name": "Recidiviz/synthetic-repo", "pr_number": 2},
]


def test_bigquery_sink_awaits_each_load() -> None:
    bq_client = mock.MagicMock()
    jobs = [mock.MagicMock(), mock.MagicMock()]
    bq_client.load_into_table_async.side_effect = jobs
    sink = BigQueryRowSink(bq_client)

    sink.write(FILES_ADDRESS, _ROWS)
    sink.write(PULL_REQUESTS_ADDRESS, _ROWS[:1])

    assert bq_client.load_into_table_async.call_args_list == [
        mock.call(address=FILES_ADDRESS, rows=_ROWS),
        mock.call(address=PULL_REQUESTS_ADDRESS, rows=_ROWS[:1]),
    ]
    for job in jobs:
        job.result.assert_called_once_with(timeout=300)


def test_bigquery_sink_skips_empty() -> None:
    bq_client = mock.MagicMock()
    sink = BigQueryRowSink(bq_client)

    sink.write(FILES_ADDRESS, [])

    bq_client.load_into_table_async.assert_not_called()


def test_ndjson_sink_appends_one_line_per_row(tmp_path: Path) -> None:
    output_dir = tmp_path / "crawl"
    sink = NdjsonRowSink(output_dir)

    sink.write(FILES_ADDRESS, _ROWS)
    sink.write(FILES_ADDRESS, _ROWS[:1])
    sink.write(PULL_REQUESTS_ADDRESS, [])

    lines = (output_dir / "files.ndjson").read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == [*_ROWS, _ROWS[0]]
    assert not (output_dir / "pull_requests.ndjson").exists()
