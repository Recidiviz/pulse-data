#   Recidiviz - a data platform for criminal justice reform
#   Copyright (C) 2022 Recidiviz, Inc.
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <https://www.gnu.org/licenses/>.
#   =============================================================================
#
"""Tests for generate_raw_data_fixtures_from_bq.py"""
import unittest

from recidiviz.tools.ingest.testing.generate_raw_data_fixtures_from_bq import (
    build_query_for_raw_table,
)


class GenerateRawDataFixturesFromBqTest(unittest.TestCase):
    """Tests for generate_raw_data_fixtures_from_bq.py"""

    def test_build_query_for_raw_table_single_column_and_value(self) -> None:
        query = build_query_for_raw_table(
            table="raw_data_table",
            person_external_ids=["123"],
            person_external_id_columns=["External_Id_Col"],
        )

        self.assertEqual(
            query,
            "SELECT * FROM raw_data_table WHERE External_Id_Col IN ('123');",
        )

    def test_build_query_for_raw_table_single_column_multiple_values(self) -> None:
        query = build_query_for_raw_table(
            table="raw_data_table",
            person_external_ids=["123", "456"],
            person_external_id_columns=["External_Id_Col"],
        )

        self.assertEqual(
            query,
            "SELECT * FROM raw_data_table WHERE External_Id_Col IN ('123', '456');",
        )

    def test_build_query_for_raw_table_multiple_cols_and_values(self) -> None:
        query = build_query_for_raw_table(
            table="raw_data_table",
            person_external_ids=["123", "456"],
            person_external_id_columns=[
                "External_Id_Col",
                "External_Id_Col_2",
                "External_Id_Col_3",
            ],
        )

        self.assertEqual(
            query,
            "SELECT * FROM raw_data_table WHERE External_Id_Col IN ('123', '456') OR External_Id_Col_2 IN "
            "('123', '456') OR External_Id_Col_3 IN ('123', '456');",
        )

    def test_build_query_for_raw_table_no_ids(self) -> None:
        query = build_query_for_raw_table(
            table="raw_data_table",
            person_external_ids=[],
            person_external_id_columns=[],
        )

        self.assertEqual(query, "SELECT * FROM raw_data_table ;")
