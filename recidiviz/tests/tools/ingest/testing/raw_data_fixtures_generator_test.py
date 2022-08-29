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

import attr

from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    RawDataClassification,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestRawDataTableUnnormalizedLatestRowsView,
)
from recidiviz.tools.ingest.testing.generate_raw_data_fixtures_from_bq import (
    RawDataFixturesGenerator,
)


class RawDataFixturesGeneratorTest(unittest.TestCase):
    """Tests for raw_data_fixtures_generator.py"""

    def setUp(self) -> None:
        self.get_region_patcher = mock.patch(
            "recidiviz.tools.ingest.testing.raw_data_fixtures_generator.get_direct_ingest_region"
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
        self.raw_table_config = DirectIngestRawFileConfig(
            file_tag="raw_data_table",
            file_path="some path",
            file_description="some description",
            data_classification=RawDataClassification.SOURCE,
            primary_key_cols=["Primary_Key_Col"],
            columns=[
                RawTableColumnInfo(
                    name="Primary_Key_Col",
                    description="primary key description",
                    is_pii=False,
                    is_datetime=False,
                ),
                RawTableColumnInfo(
                    name="External_Id_Col",
                    description="description",
                    is_pii=False,
                    is_datetime=False,
                ),
                RawTableColumnInfo(
                    name="External_Id_Col_2",
                    description="description 2",
                    is_pii=False,
                    is_datetime=False,
                ),
                RawTableColumnInfo(
                    name="External_Id_Col_3",
                    description="description 3",
                    is_pii=False,
                    is_datetime=False,
                ),
            ],
            supplemental_order_by_clause="ORDER BY Primary_Key_Col",
            encoding="UTF-8",
            separator="|",
            ignore_quotes=True,
            always_historical_export=False,
            import_chunk_size_rows=2500,
            infer_columns_from_config=False,
            primary_key_str="primary",
            custom_line_terminator="\n",
        )
        self.raw_table_view = DirectIngestRawDataTableUnnormalizedLatestRowsView(
            project_id=self.project_id,
            region_code=self.region_code,
            raw_file_config=self.raw_table_config,
            should_deploy_predicate=(lambda: False),
        )

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
            datetime_format="%m/%d/%y",
            randomized_values_map={},
        )

    def test_build_query_for_raw_table_single_column_and_value(self) -> None:
        fixtures_generator = self._build_raw_data_fixtures_generator(
            person_external_ids=["123"]
        )
        query = fixtures_generator.build_query_for_raw_table(
            raw_table_view=self.raw_table_view,
            person_external_id_columns=["External_Id_Col"],
        )
        expected_query = """
WITH rows_with_recency_rank AS (
    SELECT
        Primary_Key_Col, External_Id_Col, External_Id_Col_2, External_Id_Col_3,
        ROW_NUMBER() OVER (PARTITION BY primary
                           ORDER BY update_datetime DESC, ORDER BY Primary_Key_Col) AS recency_rank
    FROM
        `recidiviz-test.us_xx_raw_data.raw_data_table`
)
SELECT * EXCEPT (recency_rank)
FROM rows_with_recency_rank
WHERE recency_rank = 1
AND External_Id_Col IN ('123');"""
        self.assertEqual(
            query,
            expected_query,
        )

    def test_build_query_for_raw_table_single_column_multiple_values(self) -> None:
        fixtures_generator = self._build_raw_data_fixtures_generator(
            person_external_ids=["123", "456"]
        )
        query = fixtures_generator.build_query_for_raw_table(
            raw_table_view=self.raw_table_view,
            person_external_id_columns=["External_Id_Col"],
        )
        expected_query = """
WITH rows_with_recency_rank AS (
    SELECT
        Primary_Key_Col, External_Id_Col, External_Id_Col_2, External_Id_Col_3,
        ROW_NUMBER() OVER (PARTITION BY primary
                           ORDER BY update_datetime DESC, ORDER BY Primary_Key_Col) AS recency_rank
    FROM
        `recidiviz-test.us_xx_raw_data.raw_data_table`
)
SELECT * EXCEPT (recency_rank)
FROM rows_with_recency_rank
WHERE recency_rank = 1
AND External_Id_Col IN ('123', '456');"""

        self.assertEqual(query, expected_query)

    def test_build_query_for_raw_table_multiple_cols_and_values(self) -> None:
        fixtures_generator = self._build_raw_data_fixtures_generator(
            person_external_ids=["123", "456"]
        )
        query = fixtures_generator.build_query_for_raw_table(
            raw_table_view=self.raw_table_view,
            person_external_id_columns=[
                "External_Id_Col",
                "External_Id_Col_2",
                "External_Id_Col_3",
            ],
        )
        expected_query = """
WITH rows_with_recency_rank AS (
    SELECT
        Primary_Key_Col, External_Id_Col, External_Id_Col_2, External_Id_Col_3,
        ROW_NUMBER() OVER (PARTITION BY primary
                           ORDER BY update_datetime DESC, ORDER BY Primary_Key_Col) AS recency_rank
    FROM
        `recidiviz-test.us_xx_raw_data.raw_data_table`
)
SELECT * EXCEPT (recency_rank)
FROM rows_with_recency_rank
WHERE recency_rank = 1
AND External_Id_Col IN ('123', '456') OR External_Id_Col_2 IN ('123', '456') OR External_Id_Col_3 IN ('123', '456');"""
        self.maxDiff = None
        self.assertEqual(query, expected_query)

    def test_build_query_for_raw_table_no_ids(self) -> None:
        fixtures_generator = self._build_raw_data_fixtures_generator(
            person_external_ids=[]
        )
        query = fixtures_generator.build_query_for_raw_table(
            raw_table_view=self.raw_table_view,
            person_external_id_columns=[],
        )
        expected_query = """
WITH rows_with_recency_rank AS (
    SELECT
        Primary_Key_Col, External_Id_Col, External_Id_Col_2, External_Id_Col_3,
        ROW_NUMBER() OVER (PARTITION BY primary
                           ORDER BY update_datetime DESC, ORDER BY Primary_Key_Col) AS recency_rank
    FROM
        `recidiviz-test.us_xx_raw_data.raw_data_table`
)
SELECT * EXCEPT (recency_rank)
FROM rows_with_recency_rank
WHERE recency_rank = 1
;"""

        self.assertEqual(query, expected_query)

    def test_build_query_for_raw_table_historical_export(self) -> None:
        raw_table_config = attr.evolve(
            self.raw_table_config, always_historical_export=True
        )
        self.raw_table_view = DirectIngestRawDataTableUnnormalizedLatestRowsView(
            project_id=self.project_id,
            region_code=self.region_code,
            raw_file_config=raw_table_config,
            should_deploy_predicate=(lambda: False),
        )
        fixtures_generator = self._build_raw_data_fixtures_generator(
            person_external_ids=["123"]
        )
        query = fixtures_generator.build_query_for_raw_table(
            raw_table_view=self.raw_table_view,
            person_external_id_columns=["External_Id_Col"],
        )
        expected_query = """
WITH max_update_datetime AS (
    SELECT
        MAX(update_datetime) AS update_datetime
    FROM
        `recidiviz-test.us_xx_raw_data.raw_data_table`
),
max_file_id AS (
    SELECT
        MAX(file_id) AS file_id
    FROM
        `recidiviz-test.us_xx_raw_data.raw_data_table`
    WHERE
        update_datetime = (SELECT update_datetime FROM max_update_datetime)
),
rows_with_recency_rank AS (
    SELECT
        Primary_Key_Col, External_Id_Col, External_Id_Col_2, External_Id_Col_3,
        ROW_NUMBER() OVER (PARTITION BY primary
                           ORDER BY update_datetime DESC, ORDER BY Primary_Key_Col) AS recency_rank
    FROM
        `recidiviz-test.us_xx_raw_data.raw_data_table`
    WHERE
        file_id = (SELECT file_id FROM max_file_id)
)
SELECT * EXCEPT (recency_rank)
FROM rows_with_recency_rank
WHERE recency_rank = 1
AND External_Id_Col IN ('123');"""
        self.assertEqual(query, expected_query)
