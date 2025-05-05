# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for big_query_external_table_utils.py"""
import unittest

from google.cloud.bigquery import ExternalConfig, GoogleSheetsOptions, SchemaField

from recidiviz.big_query.big_query_external_table_utils import (
    external_config_has_non_schema_updates,
    get_external_config_non_schema_updates,
)
from recidiviz.utils.types import assert_type


class TestBigQueryExternalTableUtils(unittest.TestCase):
    """Tests for big_query_external_table_utils.py"""

    def _basic_google_sheets_config(
        self, schema: list[SchemaField] | None
    ) -> ExternalConfig:
        config = ExternalConfig("GOOGLE_SHEETS")
        config.autodetect = False
        config.schema = schema
        config.source_uris = ["https://docs.google.com/spreadsheets/d/AbCdEf12345/"]
        config.google_sheets_options = GoogleSheetsOptions.from_api_repr(
            {"skipLeadingRows": 1, "range": "my_sheet"}
        )
        return config

    def test_no_schemas_no_updates(self) -> None:
        config_old = self._basic_google_sheets_config(schema=None)
        config_new = self._basic_google_sheets_config(schema=None)

        self.assertEqual(
            {}, get_external_config_non_schema_updates(config_old, config_new)
        )

        self.assertFalse(external_config_has_non_schema_updates(config_old, config_new))

    def test_both_have_schemas_no_updates(self) -> None:
        schema = [
            SchemaField("Name", "STRING", "REQUIRED"),
            SchemaField("Number", "INTEGER", "REQUIRED"),
            SchemaField("Date", "DATE", "REQUIRED"),
        ]
        config_old = self._basic_google_sheets_config(schema=schema)
        config_new = self._basic_google_sheets_config(schema=schema)

        self.assertEqual(
            {}, get_external_config_non_schema_updates(config_old, config_new)
        )

        self.assertFalse(external_config_has_non_schema_updates(config_old, config_new))

    def test_only_schema_updates(self) -> None:
        schema_1 = [
            SchemaField("Name", "STRING", "REQUIRED"),
            SchemaField("Number", "INTEGER", "REQUIRED"),
            SchemaField("Date", "DATE", "REQUIRED"),
        ]

        config_old = self._basic_google_sheets_config(schema=None)
        config_new = self._basic_google_sheets_config(schema=schema_1)

        self.assertEqual(
            {}, get_external_config_non_schema_updates(config_old, config_new)
        )

        self.assertFalse(external_config_has_non_schema_updates(config_old, config_new))

        schema_2 = [
            SchemaField("Name", "STRING", "REQUIRED"),
            SchemaField("Number", "INTEGER", "REQUIRED"),
        ]

        config_old = self._basic_google_sheets_config(schema=schema_2)
        config_new = self._basic_google_sheets_config(schema=schema_1)

        self.assertEqual(
            {}, get_external_config_non_schema_updates(config_old, config_new)
        )

        self.assertFalse(external_config_has_non_schema_updates(config_old, config_new))

    def test_schema_and_non_schema_updates(self) -> None:
        schema_1 = [
            SchemaField("Name", "STRING", "REQUIRED"),
            SchemaField("Number", "INTEGER", "REQUIRED"),
            SchemaField("Date", "DATE", "REQUIRED"),
        ]

        config_old = self._basic_google_sheets_config(schema=None)
        config_new = self._basic_google_sheets_config(schema=schema_1)

        assert_type(
            config_new.google_sheets_options, GoogleSheetsOptions
        ).range = "my_other_sheet"
        config_new.source_uris = ["https://docs.google.com/spreadsheets/d/AbCdEf78910/"]

        self.assertEqual(
            {
                "googleSheetsOptions": (
                    {"range": "my_sheet", "skipLeadingRows": 1},
                    {"range": "my_other_sheet", "skipLeadingRows": 1},
                ),
                "sourceUris": (
                    ["https://docs.google.com/spreadsheets/d/AbCdEf12345/"],
                    ["https://docs.google.com/spreadsheets/d/AbCdEf78910/"],
                ),
            },
            get_external_config_non_schema_updates(config_old, config_new),
        )

        self.assertTrue(external_config_has_non_schema_updates(config_old, config_new))
