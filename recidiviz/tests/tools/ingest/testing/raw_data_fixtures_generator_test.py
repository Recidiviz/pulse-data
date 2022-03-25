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
"""Tests for raw_data_fixtures_generator.py"""
import unittest
from typing import List
from unittest import mock

from recidiviz.tools.ingest.testing.generate_raw_data_fixtures_from_bq import (
    RawDataFixturesGenerator,
)


class RawDataFixturesGeneratorTest(unittest.TestCase):
    """Tests for raw_data_fixtures_generator.py"""

    def setUp(self) -> None:
        self.get_region_patcher = mock.patch(
            "recidiviz.tools.ingest.testing.raw_data_fixtures_generator.get_region"
        ).start()
        self.bq_patcher = mock.patch(
            "recidiviz.tools.ingest.testing.raw_data_fixtures_generator.BigQueryClientImpl"
        ).start()
        self.mock_get_view_builder = mock.MagicMock(
            return_value=mock.MagicMock(build=lambda x: x, return_value=[])
        )
        self.view_builder_patcher = mock.patch(
            "recidiviz.tools.ingest.testing.raw_data_fixtures_generator.DirectIngestPreProcessedIngestViewCollector",
            get_view_builder_by_view_name=self.mock_get_view_builder,
        ).start()
        self.view_collector = self.view_builder_patcher()
        self.project_id: str = "recidiviz-test"
        self.region_code: str = "US_XX"
        self.ingest_view_tag: str = "supervision_periods"
        self.output_filename: str = "test_output"
        self.person_external_ids: List[str] = []
        self.person_external_id_columns: List[str] = []
        self.columns_to_randomize: List[str] = []

    def tearDown(self) -> None:
        self.bq_patcher.stop()
        self.view_builder_patcher.stop()
        self.get_region_patcher.stop()

    def _build_raw_data_fixtures_generator(
        self,
        person_external_ids: List[str],
    ) -> RawDataFixturesGenerator:
        return RawDataFixturesGenerator(
            project_id=self.project_id,
            region_code=self.region_code,
            ingest_view_tag=self.ingest_view_tag,
            output_filename=self.output_filename,
            person_external_ids=person_external_ids,
            person_external_id_columns=self.person_external_id_columns,
            columns_to_randomize=self.columns_to_randomize,
            file_tags_to_load_in_full=[],
            randomized_values_map={},
        )

    def test_build_query_for_raw_table_single_column_and_value(self) -> None:
        fixtures_generator = self._build_raw_data_fixtures_generator(
            person_external_ids=["123"]
        )
        query = fixtures_generator.build_query_for_raw_table(
            table="raw_data_table",
            person_external_id_columns=["External_Id_Col"],
        )

        self.assertEqual(
            query,
            "SELECT * FROM raw_data_table WHERE External_Id_Col IN ('123');",
        )

    def test_build_query_for_raw_table_single_column_multiple_values(self) -> None:
        fixtures_generator = self._build_raw_data_fixtures_generator(
            person_external_ids=["123", "456"]
        )
        query = fixtures_generator.build_query_for_raw_table(
            table="raw_data_table",
            person_external_id_columns=["External_Id_Col"],
        )

        self.assertEqual(
            query,
            "SELECT * FROM raw_data_table WHERE External_Id_Col IN ('123', '456');",
        )

    def test_build_query_for_raw_table_multiple_cols_and_values(self) -> None:
        fixtures_generator = self._build_raw_data_fixtures_generator(
            person_external_ids=["123", "456"]
        )
        query = fixtures_generator.build_query_for_raw_table(
            table="raw_data_table",
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
        fixtures_generator = self._build_raw_data_fixtures_generator(
            person_external_ids=[]
        )
        query = fixtures_generator.build_query_for_raw_table(
            table="raw_data_table",
            person_external_id_columns=[],
        )

        self.assertEqual(query, "SELECT * FROM raw_data_table ;")
