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
"""Tests for BigQueryResultsContentsHandle."""

import unittest
from typing import Any, Dict, Union

import pandas as pd
import pytest
from sqlalchemy.sql import sqltypes

from recidiviz.big_query.big_query_results_contents_handle import (
    BigQueryResultsContentsHandle,
)
from recidiviz.tests.big_query.fakes.fake_big_query_client import FakeBigQueryClient
from recidiviz.tests.big_query.fakes.fake_big_query_database import FakeBigQueryDatabase
from recidiviz.tests.big_query.fakes.fake_table_schema import MockTableSchema
from recidiviz.tools.postgres import local_postgres_helpers


@pytest.mark.uses_db
class BigQueryResultsContentsHandleTest(unittest.TestCase):
    """Tests for BigQueryResultsContentsHandle."""

    temp_db_dir: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.project_id = "recidiviz-456"
        self.fake_bq_db = FakeBigQueryDatabase()
        self.fake_bq_client = FakeBigQueryClient(
            project_id=self.project_id, database=self.fake_bq_db
        )

        self.fake_bq_db.create_mock_bq_table(
            dataset_id="my_dataset",
            table_id="my_table",
            mock_schema=MockTableSchema(
                data_types={"foo": sqltypes.String(255), "bar": sqltypes.Integer()}
            ),
            mock_data=pd.DataFrame(
                pd.DataFrame(
                    [
                        ["00001", 2010],
                        ["00002", 2020],
                        ["00003", 2030],
                    ],
                    columns=["foo", "bar"],
                ),
            ),
        )

    def tearDown(self) -> None:
        self.fake_bq_db.teardown_databases()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_simple_empty(self) -> None:
        self.fake_bq_db.create_mock_bq_table(
            dataset_id="my_dataset",
            table_id="my_empty_table",
            mock_schema=MockTableSchema({"foo": sqltypes.Integer()}),
            mock_data=pd.DataFrame([]),
        )

        handle: BigQueryResultsContentsHandle[
            Dict[str, Any]
        ] = BigQueryResultsContentsHandle(
            self.fake_bq_client.run_query_async(
                "SELECT * FROM `recidiviz-456.my_dataset.my_empty_table`;"
            )
        )
        results = list(handle.get_contents_iterator())
        self.assertEqual([], results)

    def test_simple(self) -> None:
        handle: BigQueryResultsContentsHandle[
            Dict[str, Any]
        ] = BigQueryResultsContentsHandle(
            self.fake_bq_client.run_query_async(
                "SELECT * FROM `recidiviz-456.my_dataset.my_table` ORDER BY foo;"
            )
        )
        results = list(handle.get_contents_iterator())
        self.assertEqual(
            [
                {"bar": 2010, "foo": "00001"},
                {"bar": 2020, "foo": "00002"},
                {"bar": 2030, "foo": "00003"},
            ],
            results,
        )

    def test_iterate_twice(self) -> None:
        handle: BigQueryResultsContentsHandle[
            Dict[str, Any]
        ] = BigQueryResultsContentsHandle(
            self.fake_bq_client.run_query_async(
                "SELECT * FROM `recidiviz-456.my_dataset.my_table` ORDER BY foo;"
            )
        )

        expected_results = [
            {"bar": 2010, "foo": "00001"},
            {"bar": 2020, "foo": "00002"},
            {"bar": 2030, "foo": "00003"},
        ]

        results = list(handle.get_contents_iterator())
        self.assertEqual(expected_results, results)

        results = list(handle.get_contents_iterator())
        self.assertEqual(expected_results, results)

    def test_iterate_one_at_a_time(self) -> None:
        handle: BigQueryResultsContentsHandle[
            Dict[str, Any]
        ] = BigQueryResultsContentsHandle(
            self.fake_bq_client.run_query_async(
                "SELECT * FROM `recidiviz-456.my_dataset.my_table` ORDER BY foo;"
            )
        )
        iterator = handle.get_contents_iterator()

        self.assertEqual({"bar": 2010, "foo": "00001"}, next(iterator))
        self.assertEqual({"bar": 2020, "foo": "00002"}, next(iterator))
        self.assertEqual({"bar": 2030, "foo": "00003"}, next(iterator))
        with self.assertRaises(StopIteration):
            _ = next(iterator)

    def test_partial_iteration(self) -> None:
        handle: BigQueryResultsContentsHandle[
            Dict[str, Any]
        ] = BigQueryResultsContentsHandle(
            self.fake_bq_client.run_query_async(
                "SELECT * FROM `recidiviz-456.my_dataset.my_table` ORDER BY foo;"
            )
        )
        iterator = handle.get_contents_iterator()

        self.assertEqual({"bar": 2010, "foo": "00001"}, next(iterator))

        expected_results = [
            {"bar": 2010, "foo": "00001"},
            {"bar": 2020, "foo": "00002"},
            {"bar": 2030, "foo": "00003"},
        ]

        results = list(handle.get_contents_iterator())
        self.assertEqual(expected_results, results)

    def test_iterate_with_value_converter(self) -> None:
        def flip_types(field_name: str, value: Any) -> Union[str, int]:
            if field_name == "foo":
                return int(value)
            if field_name == "bar":
                return str(value)
            raise ValueError(
                f"Unexpected field name [{field_name}] for value [{value}]."
            )

        handle = BigQueryResultsContentsHandle(
            self.fake_bq_client.run_query_async(
                "SELECT * FROM `recidiviz-456.my_dataset.my_table` ORDER BY foo;"
            ),
            value_converter=flip_types,
        )

        expected_results = [
            {"bar": "2010", "foo": 1},
            {"bar": "2020", "foo": 2},
            {"bar": "2030", "foo": 3},
        ]

        results = list(handle.get_contents_iterator())
        self.assertEqual(expected_results, results)
