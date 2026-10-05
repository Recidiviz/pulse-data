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
"""Tests for recidiviz/tools/analyst/source_table_upload_utils.py"""
import unittest
from unittest.mock import MagicMock, call, patch

import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery.enums import StandardSqlTypeNames as BigQueryFieldType

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.source_tables.source_table_config import SourceTableConfig
from recidiviz.tools.analyst.source_table_upload_utils import (
    append_df_to_source_table_in_all_projects,
    convert_df_column_to_type_if_necessary,
    validate_and_convert_df_columns_to_schema,
)


class ConvertDfColumnToTypeIfNecessaryTest(unittest.TestCase):
    """Tests for convert_df_column_to_type_if_necessary()."""

    def _convert_to_string(self, values: pd.Series) -> list:
        df = pd.DataFrame({"col": values})
        convert_df_column_to_type_if_necessary(
            df, column_name="col", field_type=BigQueryFieldType.STRING
        )
        return df["col"].tolist()

    def test_string_keeps_none_as_null(self) -> None:
        result = self._convert_to_string(pd.Series(["proj-1", None], dtype=object))
        self.assertEqual(result[0], "proj-1")
        self.assertIs(result[1], pd.NA)

    def test_string_keeps_nan_as_null(self) -> None:
        result = self._convert_to_string(pd.Series(["proj-1", np.nan], dtype=object))
        self.assertEqual(result[0], "proj-1")
        self.assertIs(result[1], pd.NA)

    def test_string_keeps_nullable_int_na_as_null(self) -> None:
        result = self._convert_to_string(pd.Series([123, None], dtype="Int64"))
        self.assertEqual(result[0], "123")
        self.assertIs(result[1], pd.NA)

    def test_string_converts_ints(self) -> None:
        self.assertEqual(self._convert_to_string(pd.Series([123, 456])), ["123", "456"])

    def test_string_leaves_all_string_column_unchanged(self) -> None:
        self.assertEqual(
            self._convert_to_string(pd.Series(["TREATED", ""], dtype=object)),
            ["TREATED", ""],
        )

    def test_date_converts_none_to_nat(self) -> None:
        df = pd.DataFrame({"col": ["2026-10-01", None]})
        convert_df_column_to_type_if_necessary(
            df, column_name="col", field_type=BigQueryFieldType.DATE
        )
        self.assertEqual(df["col"][0], pd.Timestamp("2026-10-01"))
        self.assertTrue(pd.isna(df["col"][1]))

    def test_datetime_converts_strings(self) -> None:
        df = pd.DataFrame({"col": ["2026-10-01T08:47:00", None]})
        convert_df_column_to_type_if_necessary(
            df, column_name="col", field_type=BigQueryFieldType.DATETIME
        )
        self.assertEqual(df["col"][0], pd.Timestamp("2026-10-01 08:47"))
        self.assertTrue(pd.isna(df["col"][1]))

    def test_raises_on_unsupported_type(self) -> None:
        df = pd.DataFrame({"col": [1.5]})
        with self.assertRaisesRegex(
            ValueError, r"^Column \[col\] cannot be converted to type"
        ):
            convert_df_column_to_type_if_necessary(
                df, column_name="col", field_type=BigQueryFieldType.FLOAT64
            )


class AppendDfToSourceTableInAllProjectsTest(unittest.TestCase):
    """Tests for append_df_to_source_table_in_all_projects()."""

    @patch(
        "recidiviz.tools.analyst.source_table_upload_utils.DATA_PLATFORM_GCP_PROJECTS",
        ["test-staging", "test-prod"],
    )
    @patch.object(pd.DataFrame, "to_gbq")
    def test_appends_to_every_project(self, mock_to_gbq: MagicMock) -> None:
        schema_fields = [bigquery.SchemaField("col", "STRING", mode="NULLABLE")]
        table_config = SourceTableConfig(
            address=BigQueryAddress(dataset_id="my_dataset", table_id="my_table"),
            description="Test table",
            schema_fields=schema_fields,
        )

        append_df_to_source_table_in_all_projects(
            pd.DataFrame({"col": ["a"]}), table_config=table_config
        )

        expected_kwargs = {
            "destination_table": "my_dataset.my_table",
            "table_schema": [schema_fields[0].to_api_repr()],
            "if_exists": "append",
        }
        self.assertEqual(
            mock_to_gbq.call_args_list,
            [
                call(project_id="test-staging", **expected_kwargs),
                call(project_id="test-prod", **expected_kwargs),
            ],
        )


class ValidateAndConvertDfColumnsToSchemaTest(unittest.TestCase):
    """Tests for validate_and_convert_df_columns_to_schema()."""

    def setUp(self) -> None:
        self.table_config = SourceTableConfig(
            address=BigQueryAddress(dataset_id="my_dataset", table_id="my_table"),
            description="Test table",
            schema_fields=[bigquery.SchemaField("dates", "DATE", mode="REPEATED")],
        )

    def test_leaves_repeated_list_values_unchanged(self) -> None:
        df = pd.DataFrame({"dates": [["2026-10-01"], []]})
        validate_and_convert_df_columns_to_schema(df, self.table_config)
        self.assertEqual(df["dates"].tolist(), [["2026-10-01"], []])

    def test_raises_on_repeated_non_list_value(self) -> None:
        df = pd.DataFrame({"dates": [["2026-10-01"], "2026-10-02"]})
        with self.assertRaisesRegex(
            ValueError, r"^Column \[dates\] is REPEATED, so every value must be a list"
        ):
            validate_and_convert_df_columns_to_schema(df, self.table_config)
