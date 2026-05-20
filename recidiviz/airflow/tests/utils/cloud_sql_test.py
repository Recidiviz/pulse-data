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
"""Unit tests for cloud_sql utilities."""
import unittest

from recidiviz.airflow.dags.utils.cloud_sql import postgres_quote_escape


class TestPostgresQuoteEscape(unittest.TestCase):
    def test_no_quote_returns_input_unchanged(self) -> None:
        assert postgres_quote_escape("plain text") == "plain text"

    def test_empty_string(self) -> None:
        assert postgres_quote_escape("") == ""

    def test_single_quote_is_doubled(self) -> None:
        assert postgres_quote_escape("it's broken") == "it''s broken"

    def test_multiple_quotes_each_doubled(self) -> None:
        assert postgres_quote_escape("'a' 'b'") == "''a'' ''b''"

    def test_already_doubled_quote_gets_doubled_again(self) -> None:
        # Idempotency is not a requirement: ' -> '' and '' -> '''' .
        assert postgres_quote_escape("a''b") == "a''''b"
