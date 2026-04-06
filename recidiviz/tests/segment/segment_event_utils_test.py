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
"""Tests for segment_event_utils.py"""
import unittest

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.segment.segment_event_utils import segment_event_schema


class TestSegmentEventSchema(unittest.TestCase):
    """Tests for segment_event_schema()."""

    def test_schema_fixed_columns(self) -> None:
        address = BigQueryAddress(
            dataset_id="pulse_dashboard_segment_metrics",
            table_id="frontend_caseload_search",
        )
        schema = segment_event_schema(
            segment_events_source_table_address=address,
            additional_attribute_cols=[],
        )
        col_info = [(c.name, c.field_type, c.mode) for c in schema]
        self.assertEqual(
            col_info,
            [
                ("state_code", bigquery.SqlTypeNames.STRING, "NULLABLE"),
                ("user_id", bigquery.SqlTypeNames.STRING, "NULLABLE"),
                ("email_address", bigquery.SqlTypeNames.STRING, "NULLABLE"),
                ("event_ts", bigquery.SqlTypeNames.DATETIME, "NULLABLE"),
                ("person_id", bigquery.SqlTypeNames.INTEGER, "NULLABLE"),
                ("session_id", bigquery.SqlTypeNames.STRING, "NULLABLE"),
                ("context_page_path", bigquery.SqlTypeNames.STRING, "NULLABLE"),
                ("context_page_url", bigquery.SqlTypeNames.STRING, "NULLABLE"),
                ("product_type", bigquery.SqlTypeNames.STRING, "NULLABLE"),
            ],
        )

    def test_schema_includes_additional_attribute_cols(self) -> None:
        address = BigQueryAddress(
            dataset_id="pulse_dashboard_segment_metrics",
            table_id="frontend_caseload_search",
        )
        schema = segment_event_schema(
            segment_events_source_table_address=address,
            additional_attribute_cols=["search_type", "search_count"],
        )
        col_info = [(c.name, c.field_type, c.mode) for c in schema]
        self.assertEqual(len(col_info), 11)
        self.assertEqual(
            col_info[-2:],
            [
                ("search_type", bigquery.SqlTypeNames.STRING, "NULLABLE"),
                ("search_count", bigquery.SqlTypeNames.INTEGER, "NULLABLE"),
            ],
        )

    def test_schema_attribute_col_not_in_yaml_raises(self) -> None:
        address = BigQueryAddress(
            dataset_id="pulse_dashboard_segment_metrics",
            table_id="frontend_caseload_search",
        )
        with self.assertRaises(ValueError):
            segment_event_schema(
                segment_events_source_table_address=address,
                additional_attribute_cols=["nonexistent_column"],
            )
