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
"""Tests for recidiviz/tools/analyst/notion_roadmap_snapshot.py"""
import datetime
import unittest

import pandas as pd
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.source_tables.source_table_config import SourceTableConfig
from recidiviz.tools.analyst.notion_roadmap_snapshot import (
    _dedupe_column_names,
    _extract_slug_id,
    _parse_working_period,
    _to_bq_safe_column_name,
    get_notion_roadmap_snapshots_source_table_config,
    parse_notion_date_columns,
)


class ColumnRenamingTest(unittest.TestCase):
    """Tests for _to_bq_safe_column_name() and _dedupe_column_names()."""

    def test_lowercases_and_collapses_non_alphanumeric_runs(self) -> None:
        self.assertEqual(
            _to_bq_safe_column_name("Contract to Data (weeks)"),
            "contract_to_data_weeks",
        )

    def test_strips_leading_and_trailing_symbols(self) -> None:
        self.assertEqual(
            _to_bq_safe_column_name("*Global Prio Flag"), "global_prio_flag"
        )

    def test_raises_on_no_usable_characters(self) -> None:
        with self.assertRaises(ValueError):
            _to_bq_safe_column_name("***")

    def test_dedupes_collisions(self) -> None:
        self.assertEqual(
            _dedupe_column_names(["state", "state", "state"]),
            ["state", "state_2", "state_3"],
        )


class ExtractSlugIdTest(unittest.TestCase):
    """Tests for _extract_slug_id()."""

    def test_extracts_slug_id_from_overview_url(self) -> None:
        self.assertEqual(
            _extract_slug_id(
                "https://linear.app/recidiviz/project/us-mo-tasks-v2-74c48ad27169/overview"
            ),
            "74c48ad27169",
        )

    def test_extracts_slug_id_at_end_of_url(self) -> None:
        self.assertEqual(
            _extract_slug_id(
                "https://linear.app/recidiviz/project/us-mo-tasks-v2-74c48ad27169"
            ),
            "74c48ad27169",
        )

    def test_returns_none_for_blank(self) -> None:
        self.assertIsNone(_extract_slug_id(""))

    def test_returns_none_when_no_match(self) -> None:
        self.assertIsNone(_extract_slug_id("https://example.com/not-a-linear-url"))


class GetNotionRoadmapSnapshotsSourceTableConfigTest(unittest.TestCase):
    """Tests for get_notion_roadmap_snapshots_source_table_config()."""

    def test_returns_config_matching_registered_yaml(self) -> None:
        config = get_notion_roadmap_snapshots_source_table_config()

        self.assertEqual(config.address.dataset_id, "notion_snapshots")
        self.assertEqual(config.address.table_id, "notion_roadmap_snapshots")
        self.assertTrue(config.has_column("initiative"))
        self.assertTrue(config.has_column("linear_project"))
        self.assertTrue(config.has_column("linear_project_slug_id"))
        self.assertTrue(config.has_column("snapshot_date"))
        self.assertTrue(config.has_column("upload_datetime"))

    def test_date_columns_have_date_types(self) -> None:
        config = get_notion_roadmap_snapshots_source_table_config()
        field_types = {field.name: field.field_type for field in config.schema_fields}

        self.assertEqual(field_types["target_fsl"], "DATE")
        self.assertEqual(field_types["working_period_start_date"], "DATE")
        self.assertEqual(field_types["working_period_end_date_exclusive"], "DATE")
        self.assertEqual(field_types["created_time"], "DATETIME")
        self.assertEqual(field_types["working_period"], "STRING")


class ParseWorkingPeriodTest(unittest.TestCase):
    """Tests for _parse_working_period()."""

    def test_parses_range_with_exclusive_end(self) -> None:
        self.assertEqual(
            _parse_working_period("July 1, 2025 → August 31, 2025"),
            (datetime.date(2025, 7, 1), datetime.date(2025, 9, 1)),
        )

    def test_single_date_covers_one_day(self) -> None:
        self.assertEqual(
            _parse_working_period("July 1, 2025"),
            (datetime.date(2025, 7, 1), datetime.date(2025, 7, 2)),
        )

    def test_blank_returns_nones(self) -> None:
        self.assertEqual(_parse_working_period(""), (None, None))

    def test_raises_on_unexpected_format(self) -> None:
        with self.assertRaises(ValueError):
            _parse_working_period("2025-07-01 → 2025-08-31")


class ParseNotionDateColumnsTest(unittest.TestCase):
    """Tests for parse_notion_date_columns()."""

    def setUp(self) -> None:
        self.table_config = SourceTableConfig(
            address=BigQueryAddress(dataset_id="my_dataset", table_id="my_table"),
            description="Test table",
            schema_fields=[
                bigquery.SchemaField("initiative", "STRING"),
                bigquery.SchemaField("target_fsl", "DATE"),
                bigquery.SchemaField("data_received", "DATE", mode="REPEATED"),
                bigquery.SchemaField("created_time", "DATETIME"),
                bigquery.SchemaField("snapshot_date", "DATE"),
            ],
        )

    def test_parses_dates_datetimes_and_date_lists(self) -> None:
        df = pd.DataFrame(
            {
                "initiative": ["XX Meetings", "XX Tasks", "XX Ingest"],
                "target_fsl": [
                    "March 31, 2026",
                    "",
                    "December 2, 2024 → January 13, 2025",
                ],
                "data_received": [
                    "February 27, 2020,August 12, 2019",
                    "",
                    "May 1, 2020",
                ],
                "created_time": [
                    "March 25, 2026 8:47 AM",
                    "July 6, 2026 5:43 PM",
                    "",
                ],
                "snapshot_date": ["2026-10-01"] * 3,
            }
        )

        parse_notion_date_columns(df, table_config=self.table_config)

        self.assertEqual(
            df["target_fsl"].tolist(),
            [datetime.date(2026, 3, 31), None, datetime.date(2025, 1, 13)],
        )
        self.assertEqual(
            df["data_received"].tolist(),
            [
                [datetime.date(2019, 8, 12), datetime.date(2020, 2, 27)],
                [],
                [datetime.date(2020, 5, 1)],
            ],
        )
        self.assertEqual(
            df["created_time"].tolist(),
            [
                datetime.datetime(2026, 3, 25, 8, 47),
                datetime.datetime(2026, 7, 6, 17, 43),
                None,
            ],
        )
        self.assertEqual(
            df["initiative"].tolist(), ["XX Meetings", "XX Tasks", "XX Ingest"]
        )
        self.assertEqual(df["snapshot_date"].tolist(), ["2026-10-01"] * 3)

    def test_raises_naming_column_on_unexpected_format(self) -> None:
        df = pd.DataFrame(
            {
                "initiative": ["XX Meetings"],
                "target_fsl": ["Q1 2026"],
                "data_received": [""],
                "created_time": ["March 25, 2026 8:47 AM"],
                "snapshot_date": ["2026-10-01"],
            }
        )

        with self.assertRaisesRegex(
            ValueError, r"^Notion column \[target_fsl\] has value \[Q1 2026\]"
        ):
            parse_notion_date_columns(df, table_config=self.table_config)
