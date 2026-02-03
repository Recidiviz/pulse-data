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
"""Tests capabilities of the BigQuery emulator."""
import datetime
from datetime import date

import pandas as pd
import pandas_gbq
from google.api_core.exceptions import InternalServerError
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.test_setup_utils import BQ_EMULATOR_PROJECT_ID

_DATASET_1 = "dataset_1"
_TABLE_1 = "table_1"


class TestBigQueryEmulator(BigQueryEmulatorTestCase):
    """Tests capabilities of the BigQuery emulator."""

    def test_no_tables(self) -> None:
        """Run a simple query that does not query any tables."""
        query = """
SELECT *
FROM UNNEST([
   STRUCT(1 AS a, 2 AS b), STRUCT(3 AS a, 4 AS b)
]);
"""
        self.run_query_test(
            query,
            expected_result=[
                {"a": 1, "b": 2},
                {"a": 3, "b": 4},
            ],
        )

    def test_select_except(self) -> None:
        """Run a simple SELECT query with an EXCEPT clause."""
        query = """
SELECT * EXCEPT(b)
FROM UNNEST([
   STRUCT(1 AS a, 2 AS b), STRUCT(3 AS a, 4 AS b)
]);
"""
        self.run_query_test(
            query,
            expected_result=[
                {"a": 1},
                {"a": 3},
            ],
        )

    def test_select_qualify(self) -> None:
        """Run a simple query that has a QUALIFY clause."""

        query = """
SELECT *
FROM UNNEST([
   STRUCT(1 AS a, 2 AS b), STRUCT(3 AS a, 4 AS b)
])
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (ORDER BY b DESC) = 1;
"""

        self.run_query_test(
            query,
            expected_result=[{"a": 3, "b": 4}],
        )

    def test_query_empty_table(self) -> None:
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "a",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "b",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
            ],
        )

        self.run_query_test(
            f"SELECT a, b FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`;",
            expected_result=[],
        )

    def test_query_simple_table(self) -> None:
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "a",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "b",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
            ],
        )
        self.load_rows_into_table(
            address,
            data=[{"a": 1, "b": "foo"}, {"a": 3, "b": None}],
        )

        self.run_query_test(
            f"SELECT a, b FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`;",
            expected_result=[{"a": 1, "b": "foo"}, {"a": 3, "b": None}],
        )

    def test_delete_and_recreate_table(self) -> None:
        """Test that confirms https://github.com/goccy/bigquery-emulator/issues/16 has
        been resolved."""
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        schema = [
            bigquery.SchemaField(
                "a",
                field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                mode="REQUIRED",
            ),
        ]
        self.create_mock_table(address, schema=schema)
        self.bq_client.delete_table(address=address)

        # Should not crash
        self.create_mock_table(address, schema=schema)

    def test_delete_and_recreate_table_different_schema(self) -> None:
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        schema_1 = [
            bigquery.SchemaField(
                "a",
                field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                mode="REQUIRED",
            ),
        ]
        self.create_mock_table(address, schema=schema_1)

        self.run_query_test(
            f"SELECT a FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`",
            expected_result=[],
        )

        self.bq_client.delete_table(address=address)

        schema_2 = [
            bigquery.SchemaField(
                "b",
                field_type=bigquery.enums.SqlTypeNames.STRING.value,
                mode="REQUIRED",
            ),
        ]
        # Should not crash
        self.create_mock_table(address, schema=schema_2)

        # Should be a valid query now
        self.run_query_test(
            f"SELECT b FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`",
            expected_result=[],
        )

    def test_create_two_tables_same_name_different_dataset(self) -> None:
        """Test that confirms https://github.com/goccy/bigquery-emulator/issues/18 has
        been resolved."""
        address_1 = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        schema_1 = [
            bigquery.SchemaField(
                "a",
                field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                mode="REQUIRED",
            ),
        ]
        address_2 = BigQueryAddress(dataset_id="dataset_5", table_id=_TABLE_1)
        schema_2 = [
            bigquery.SchemaField(
                "b",
                field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                mode="REQUIRED",
            ),
        ]

        self.create_mock_table(address_1, schema_1)
        self.create_mock_table(address_2, schema_2)

        table_1 = self.bq_client.get_table(address_1)

        self.assertEqual(address_1.dataset_id, table_1.dataset_id)
        self.assertEqual(address_1.table_id, table_1.table_id)
        self.assertEqual(schema_1, table_1.schema)
        table_2 = self.bq_client.get_table(address_2)
        self.assertEqual(address_2.dataset_id, table_2.dataset_id)
        self.assertEqual(address_2.table_id, table_2.table_id)
        self.assertEqual(schema_2, table_2.schema)

        self.run_query_test(
            f"SELECT a FROM `{self.project_id}.{address_1.dataset_id}.{address_1.table_id}`",
            expected_result=[],
        )
        self.run_query_test(
            f"SELECT b FROM `{self.project_id}.{address_2.dataset_id}.{address_2.table_id}`",
            expected_result=[],
        )

        self.bq_client.stream_into_table(address_1, rows=[{"a": 1}])
        self.bq_client.stream_into_table(address_2, rows=[{"b": 2}])

        self.run_query_test(
            f"SELECT a FROM `{self.project_id}.{address_1.dataset_id}.{address_1.table_id}`",
            expected_result=[{"a": 1}],
        )
        self.run_query_test(
            f"SELECT b FROM `{self.project_id}.{address_2.dataset_id}.{address_2.table_id}`",
            expected_result=[{"b": 2}],
        )

    def test_query_min_max_integers(self) -> None:
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "a",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "b",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="NULLABLE",
                ),
            ],
        )
        self.load_rows_into_table(
            address,
            data=[{"a": 1, "b": 2}, {"a": 3, "b": 4}],
        )

        self.run_query_test(
            f"SELECT MIN(a) AS min_a, MAX(b) AS max_b "
            f"FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`;",
            expected_result=[{"min_a": 1, "max_b": 4}],
        )

    def test_query_min_max_dates(self) -> None:
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "a",
                    field_type=bigquery.enums.SqlTypeNames.DATE.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "b",
                    field_type=bigquery.enums.SqlTypeNames.DATE.value,
                    mode="NULLABLE",
                ),
            ],
        )
        self.load_rows_into_table(
            address,
            data=[
                {"a": "2022-01-01", "b": "2022-02-02"},
                {"a": "2022-03-03", "b": "2022-04-04"},
            ],
        )

        self.run_query_test(
            f"SELECT MIN(a) AS min_a, MAX(b) AS max_b "
            f"FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`;",
            expected_result=[{"min_a": date(2022, 1, 1), "max_b": date(2022, 4, 4)}],
        )

    def test_query_min_max_dates_with_partition(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/31."""
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "a",
                    field_type=bigquery.enums.SqlTypeNames.DATE.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "b",
                    field_type=bigquery.enums.SqlTypeNames.DATE.value,
                    mode="NULLABLE",
                ),
            ],
        )
        self.load_rows_into_table(
            address,
            data=[
                {"a": "2022-01-01", "b": "2022-03-03"},
                {"a": "2022-02-02", "b": "2022-03-03"},
                {"a": "2022-02-02", "b": "2022-04-04"},
            ],
        )

        self.run_query_test(
            f"SELECT MIN(a) OVER (PARTITION BY b) AS min_a, MAX(b) OVER (PARTITION BY a) AS max_b "
            f"FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`;",
            expected_result=[
                {
                    "min_a": datetime.date(2022, 1, 1),
                    "max_b": datetime.date(2022, 3, 3),
                },
                {
                    "min_a": datetime.date(2022, 1, 1),
                    "max_b": datetime.date(2022, 4, 4),
                },
                {
                    "min_a": datetime.date(2022, 2, 2),
                    "max_b": datetime.date(2022, 4, 4),
                },
            ],
        )

    def test_query_min_with_parition(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/19."""
        self.run_query_test(
            """SELECT MIN(a) OVER (PARTITION BY b) AS min_a
FROM UNNEST([STRUCT(1 AS a, 2 AS b)]);""",
            expected_result=[{"min_a": 1}],
        )

    def test_query_max_with_parition(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/19."""
        self.run_query_test(
            """SELECT MAX(a) OVER (PARTITION BY b) AS max_a
FROM UNNEST([STRUCT(1 AS a, 2 AS b)]);""",
            expected_result=[{"max_a": 1}],
        )

    def test_query_count_with_parition(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/19."""
        self.run_query_test(
            """SELECT COUNT(a) OVER (PARTITION BY b) AS count_a
FROM UNNEST([STRUCT(1 AS a, 2 AS b)]);""",
            expected_result=[{"count_a": 1}],
        )

    def test_query_sum_with_parition(self) -> None:
        self.run_query_test(
            """SELECT SUM(a) OVER (PARTITION BY b) AS sum_a
FROM UNNEST([STRUCT(1 AS a, 2 AS b)]);""",
            expected_result=[{"sum_a": 1}],
        )

    def test_query_avg_with_parition(self) -> None:
        self.run_query_test(
            """SELECT AVG(a) OVER (PARTITION BY b) AS avg_a
FROM UNNEST([STRUCT(1 AS a, 2 AS b)]);""",
            expected_result=[{"avg_a": 1.0}],
        )

    def test_array_type(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/20."""
        query = "SELECT [1, 2, 3] as a;"
        self.run_query_test(
            query,
            expected_result=[{"a": [1, 2, 3]}],
        )

    def test_safe_parse_date_valid(self) -> None:
        self.run_query_test(
            """SELECT SAFE.PARSE_DATE("%m/%d/%Y", "12/25/2008") as a;""",
            expected_result=[{"a": date(2008, 12, 25)}],
        )

    def test_safe_parse_date_invalid(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/149."""
        self.run_query_test(
            """SELECT SAFE.PARSE_DATE("%m/%d/%Y", "2008-12-25") as a;""",
            expected_result=[{"a": None}],
        )

    def test_safe_parse_date_on_julian_date(self) -> None:
        """Tests resolution of goccy/go-zetasqlite#196"""
        self.run_query_test(
            """SELECT SAFE.PARSE_DATE('%y%j', '85001') AS a;""",
            expected_result=[{"a": date(1985, 1, 1)}],
        )

    def test_array_to_json(self) -> None:
        # Tests resolution to https://github.com/goccy/bigquery-emulator/issues/24.
        query = "SELECT TO_JSON([1, 2, 3]) as a;"
        self.run_query_test(
            query,
            expected_result=[{"a": [1, 2, 3]}],
        )

    def test_to_json(self) -> None:
        query = """SELECT TO_JSON(
  STRUCT("foo" AS a, 1 AS b)
) AS result;"""

        self.run_query_test(
            query,
            expected_result=[{"result": {"a": "foo", "b": 1}}],
        )

    def test_to_json_nested(self) -> None:
        query = """SELECT TO_JSON(
  STRUCT("foo" AS a, TO_JSON(STRUCT("bar" AS c)) AS b)
) AS result;"""

        self.run_query_test(
            query,
            expected_result=[{"result": {"a": "foo", "b": {"c": "bar"}}}],
        )

    def test_to_json_nested_cte(self) -> None:
        query = """WITH inner_json AS (
  SELECT TO_JSON(STRUCT("bar" AS c)) AS b
)
SELECT TO_JSON(STRUCT("foo" as a, b)) AS result
FROM inner_json;"""

        self.run_query_test(
            query,
            expected_result=[{"result": {"a": "foo", "b": {"c": "bar"}}}],
        )

    def test_to_json_nested_cte_column_rename(self) -> None:
        query = """WITH inner_json AS (
  SELECT TO_JSON(STRUCT("bar" AS c)) AS b
)
SELECT TO_JSON(STRUCT("foo" AS a, b AS b_2)) AS result
FROM inner_json;"""

        self.run_query_test(
            query,
            expected_result=[{"result": {"a": "foo", "b_2": {"c": "bar"}}}],
        )

    def test_to_json_nested_cte_numbers(self) -> None:
        query = """WITH inner_json AS (
    SELECT TO_JSON(STRUCT(1 AS c)) AS b
)
SELECT TO_JSON(STRUCT(2 as a, b)) AS result
FROM inner_json;"""

        self.run_query_test(
            query,
            expected_result=[{"result": {"a": 2, "b": {"c": 1}}}],
        )

    def test_to_json_nested_outer_array(self) -> None:
        query = """WITH inner_json AS (
    SELECT TO_JSON(STRUCT(1 AS c)) AS b
)
SELECT
TO_JSON([
    TO_JSON(STRUCT('foo' AS a, b))
]) AS result
FROM inner_json;"""

        self.run_query_test(
            query,
            expected_result=[{"result": [{"a": "foo", "b": {"c": 1}}]}],
        )

    def test_nested_json_array_agg(self) -> None:
        query = """WITH inner_table AS (
  SELECT * 
  FROM UNNEST([
    STRUCT(
      "foo" AS a, TO_JSON(STRUCT(1 AS b)) AS c
    )
  ])
)
SELECT TO_JSON(ARRAY_AGG(
    TO_JSON(STRUCT(a, c))
    ORDER BY a
)) AS result
FROM inner_table;"""
        self.run_query_test(
            query,
            expected_result=[{"result": [{"a": "foo", "c": {"b": 1}}]}],
        )

    def test_array_agg(self) -> None:
        query = """
SELECT b, ARRAY_AGG(a) AS a_list
FROM UNNEST([
   STRUCT(1 AS a, 2 AS b),
   STRUCT(3 AS a, 2 AS b)
])
GROUP BY b;
"""
        self.run_query_test(
            query,
            expected_result=[{"a_list": [1, 3], "b": 2}],
        )

    def test_array_agg_ignore_nulls_no_nulls(self) -> None:
        query = """
SELECT b, ARRAY_AGG(a IGNORE NULLS) AS a_list
FROM UNNEST([
   STRUCT(1 AS a, 2 AS b),
   STRUCT(3 AS a, 2 AS b)
])
GROUP BY b;
"""
        self.run_query_test(
            query,
            expected_result=[{"a_list": [1, 3], "b": 2}],
        )

    # TODO(https://github.com/goccy/bigquery-emulator/issues/34): File task for this
    def test_array_agg_ignore_nulls(self) -> None:
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "a",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "b",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="NULLABLE",
                ),
            ],
        )
        self.load_rows_into_table(
            address,
            data=[{"a": None, "b": 2}, {"a": 3, "b": 2}],
        )
        query = f"""
SELECT b, ARRAY_AGG(a IGNORE NULLS) AS a_list
FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`
GROUP BY b;
"""
        self.run_query_test(
            query,
            expected_result=[{"a_list": [3], "b": 2}],
        )

    def test_json_load_rows_into_table(self) -> None:
        """
        Tests resolution of https://github.com/goccy/bigquery-emulator/issues/286 loading a mock table with a JSON
        column and using JSON_TYPE, JSON_QUERY, and JSON_VALUE query functions to inspect the JSON elements.
        """
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address=address,
            schema=[
                bigquery.SchemaField(
                    "json_column",
                    field_type=bigquery.enums.StandardSqlTypeNames.JSON.value,
                    mode="NULLABLE",
                ),
            ],
        )
        self.load_rows_into_table(
            address=address,
            data=[
                {"json_column": {"a": "2024-01-01", "b": 1.2}},
            ],
        )
        test_query = f"""
        SELECT
            JSON_TYPE(json_column) AS json_column_type,
            JSON_TYPE(JSON_QUERY(json_column, "$.a")) AS a_column_type,
            JSON_TYPE(JSON_QUERY(json_column, "$.b")) AS b_column_type,
            SAFE_CAST(JSON_VALUE(json_column, "$.a") AS DATE) AS a,
            SAFE_CAST(JSON_VALUE(json_column, "$.b") AS FLOAT64) AS b,
        FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`
        """
        self.run_query_test(
            test_query,
            expected_result=[
                {
                    "json_column_type": "object",
                    "a_column_type": "string",
                    "b_column_type": "number",
                    "a": date(2024, 1, 1),
                    "b": 1.2,
                },
            ],
        )

    def test_invalid_data_load_fails(self) -> None:
        """Tests that an action causing a 500 failure actually causes
        our system to fail, rather than an infinite retry.
        """
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "a",
                    field_type=bigquery.enums.SqlTypeNames.DATE.value,
                    mode="REQUIRED",
                ),
            ],
        )
        with self.assertRaisesRegex(
            RuntimeError, "failed to convert 202-06-06 to time.Time type"
        ):
            self.load_rows_into_table(address, data=[{"a": "202-06-06"}])

    def test_array_agg_with_nulls(self) -> None:
        """Tests fix for https://github.com/goccy/bigquery-emulator/issues/33"""
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "a",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "b",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="NULLABLE",
                ),
            ],
        )
        self.load_rows_into_table(
            address,
            data=[{"a": None, "b": 2}, {"a": 3, "b": 2}],
        )
        query = f"""
SELECT b, ARRAY_AGG(a) AS a_list
FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`
GROUP BY b;
"""
        with self.assertRaisesRegex(
            Exception, r"ARRAY_AGG: input value must be not null"
        ):
            self.run_query_test(
                query,
                expected_result=[{"a_list": [3], "b": 2}],
            )

    def test_null_in_unnest(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/30."""
        query = """
SELECT a
FROM UNNEST([
   STRUCT(NULL AS a)
]);
"""
        self.run_query_test(
            query,
            expected_result=[{"a": None}],
        )

    def test_date_in_unnest(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/30."""
        query = """
SELECT a
FROM UNNEST([
  STRUCT(DATE(2022, 1, 1) AS a)
]);
"""
        self.run_query_test(
            query,
            expected_result=[{"a": datetime.date(2022, 1, 1)}],
        )

    def test_cast_datetime_as_string(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/175."""
        self.run_query_test(
            """SELECT CAST(DATETIME(1987, 1, 25, 0, 0, 0) AS STRING)""",
            expected_result=[{"$col1": "1987-01-25 00:00:00"}],
        )

    def test_cast_datetime_as_string_with_format(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/175."""
        # TODO(goccy/bigquery-emulator#175): Change expected result to "SUNDAY, JANUARY 25 1987 AT 12:00:00" when fixed.
        self.run_query_test(
            """SELECT CAST(DATETIME(1987, 1, 25, 0, 0, 0) AS STRING FORMAT 'DAY"," MONTH DD YYYY "AT" HH":"MI":"SS')""",
            expected_result=[{"$col1": "1987-01-25 00:00:00"}],
        )

    def test_integer_type_alias(self) -> None:
        self.run_query_test(
            "SELECT CAST(null AS INT64)", expected_result=[{"$col1": None}]
        )
        # TODO(#33060) This should not raise an error.
        with self.assertRaises(InternalServerError):
            self.run_query_test(
                "SELECT CAST(null AS INTEGER)", expected_result=[{"$col1": None}]
            )

    def test_drop_view(self) -> None:
        """Ensures we can run DROP VIEW statements on the emulator."""
        client = self.bq_client.client
        # We need a created dataset and table before we define a view.
        client.create_dataset("example_dataset")
        client.create_table(
            bigquery.Table(
                f"{BQ_EMULATOR_PROJECT_ID}.example_dataset.example_table",
                [
                    bigquery.SchemaField("string_field", "STRING"),
                ],
            )
        )
        _view_definition = bigquery.Table(
            f"{BQ_EMULATOR_PROJECT_ID}.example_dataset.example_view"
        )
        _view_definition.view_query = (
            f"SELECT * FROM `{BQ_EMULATOR_PROJECT_ID}.example_dataset.example_table`"
        )
        example_view = client.create_table(_view_definition)
        client.delete_table(example_view)

    def test_json_string_column(self) -> None:
        query = """
            SELECT 
                TO_JSON_STRING(
                  ARRAY_AGG(
                    STRUCT< str_col string, date_col date, int_col int64 >
                    ('strings', date("2022-01-01"), 42) 
                  )
                )
            FROM unnest([1])
        """
        self.run_query_test(
            query,
            expected_result=[
                {
                    "$col1": '[{"str_col":"strings","date_col":"2022-01-01","int_col":42}]'
                }
            ],
        )

    def test_view_schema(self) -> None:
        """Ensures views return their schema as part of the creation response."""
        client = self.bq_client.client
        # We need a created dataset and table before we define a view.
        client.create_dataset("example_dataset")
        client.create_table(
            bigquery.Table(
                f"{BQ_EMULATOR_PROJECT_ID}.example_dataset.example_table",
                [
                    bigquery.SchemaField("string_field", "STRING"),
                ],
            )
        )
        _view_definition = bigquery.Table(
            f"{BQ_EMULATOR_PROJECT_ID}.example_dataset.example_view"
        )
        _view_definition.view_query = (
            f"SELECT * FROM `{BQ_EMULATOR_PROJECT_ID}.example_dataset.example_table`"
        )
        example_view = client.create_table(_view_definition)
        self.assertEqual(
            example_view.schema,
            [SchemaField("string_field", "STRING", "NULLABLE")],
        )

    def test_query_from_pandas_call(self) -> None:

        # Query against the emulator, (no tables)
        df = pandas_gbq.read_gbq("SELECT 1 AS one", project_id=self.project_id)
        assert df.shape == (1, 1)
        assert df.one.iloc[0] == 1

        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "a",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "b",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
            ],
        )

        # Query against empty table
        df = pandas_gbq.read_gbq(
            f"SELECT a, b FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`;"
        )
        assert df.empty

        # Load data to table and query
        self.load_rows_into_table(
            address,
            data=[{"a": 1, "b": "foo"}, {"a": 3, "b": None}],
        )
        df = pandas_gbq.read_gbq(
            f"SELECT a, b FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`;"
        )
        assert df.a.to_list() == [1, 3]
        assert df.b.to_list() == ["foo", None]

        # Gut check with emulator client
        self.run_query_test(
            f"SELECT a, b FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`;",
            expected_result=[{"a": 1, "b": "foo"}, {"a": 3, "b": None}],
        )

        # A similar mocking approach didn't quite work. More work to be done.
        with self.assertRaisesRegex(
            RuntimeError,
            "Writing to the emulator from pandas is not currently supported.",
        ):
            more_data = pd.DataFrame(
                [{"a": 42, "b": "bar"}, {"a": 43, "b": "baz"}],
                columns=["a", "b"],
            )
            pandas_gbq.to_gbq(
                more_data,
                destination_table="{address.dataset_id}.{address.table_id}",
                project_id=self.project_id,
                if_exists="append",
            )

    def test_timestamp_min_max(self) -> None:
        """Tests resolution of https://github.com/goccy/go-zetasqlite/issues/132
        and https://github.com/goccy/bigquery-emulator/issues/262"""
        self.run_query_test(
            """SELECT TIMESTAMP '0001-01-01 00:00:00.000000+00', TIMESTAMP '9999-12-31 23:59:59.999999+00'""",
            expected_result=[
                {
                    "$col1": datetime.datetime(
                        1, 1, 1, 0, 0, tzinfo=datetime.timezone.utc
                    ),
                    "$col2": datetime.datetime(
                        9999, 12, 31, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc
                    ),
                }
            ],
        )

    def test_information_schema_schemata(self) -> None:
        """Tests querying INFORMATION_SCHEMA.SCHEMATA to list datasets."""
        # TODO(https://github.com/goccy/bigquery-emulator/issues/48): Update this test
        #  once INFORMATION_SCHEMA is supported in the emulator.
        client = self.bq_client.client

        # Create some datasets
        client.create_dataset("test_dataset_1")
        client.create_dataset("test_dataset_2")
        client.create_dataset("another_dataset")

        # Query INFORMATION_SCHEMA.SCHEMATA to list all datasets
        query = f"SELECT schema_name FROM `{self.project_id}`.INFORMATION_SCHEMA.SCHEMATA ORDER BY schema_name"

        # This is what SHOULD work once the emulator supports INFORMATION_SCHEMA:
        # results = list(client.query(query).result())
        # dataset_names = [row.schema_name for row in results]
        # self.assertIn("test_dataset_1", dataset_names)
        # self.assertIn("test_dataset_2", dataset_names)
        # self.assertIn("another_dataset", dataset_names)

        # But currently the emulator does not support INFORMATION_SCHEMA
        with self.assertRaisesRegex(
            InternalServerError,
            r"Table not found:.*INFORMATION_SCHEMA\.SCHEMATA",
        ):
            list(client.query(query).result())
