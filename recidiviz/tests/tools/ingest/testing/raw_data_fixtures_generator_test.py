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
from mock import Mock

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
    RawDataClassification,
    RawDataExportLookbackWindow,
    RawDataFileUpdateCadence,
    RawTableColumnFieldType,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewRawFileDependency,
)
from recidiviz.tools.ingest.testing.generate_raw_data_fixtures_from_bq import (
    RawDataFixturesGenerator,
)


@mock.patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-test"))
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
            "recidiviz.tools.ingest.testing.raw_data_fixtures_generator.DirectIngestViewQueryBuilderCollector",
            get_view_builder_by_view_name=self.mock_get_view_builder,
        ).start()
        self.view_collector = self.view_builder_patcher()
        self.project_id: str = "recidiviz-test"
        self.region_code: str = "US_XX"
        self.ingest_view_tag: str = "supervision_periods"
        self.output_filename: str = "test_output"
        self.root_entity_external_ids: List[str] = []
        self.root_entity_external_id_columns: List[str] = []
        self.columns_to_randomize: List[str] = []

        file_tag = "raw_data_table"
        raw_table_config = DirectIngestRawFileConfig(
            state_code=StateCode(self.region_code.upper()),
            file_tag=file_tag,
            file_path="some path",
            file_description="some description",
            data_classification=RawDataClassification.SOURCE,
            primary_key_cols=["Primary_Key_Col"],
            columns=[
                RawTableColumnInfo(
                    name="Primary_Key_Col",
                    state_code=StateCode.US_XX,
                    file_tag=file_tag,
                    description="primary key description",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.STRING,
                ),
                RawTableColumnInfo(
                    name="External_Id_Col",
                    state_code=StateCode.US_XX,
                    file_tag=file_tag,
                    description="description",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.STRING,
                ),
                RawTableColumnInfo(
                    name="External_Id_Col_2",
                    state_code=StateCode.US_XX,
                    file_tag=file_tag,
                    description="description 2",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.STRING,
                ),
                RawTableColumnInfo(
                    name="External_Id_Col_3",
                    state_code=StateCode.US_XX,
                    file_tag=file_tag,
                    description="description 3",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.STRING,
                ),
            ],
            supplemental_order_by_clause="ORDER BY Primary_Key_Col",
            encoding="UTF-8",
            separator="|",
            ignore_quotes=True,
            export_lookback_window=RawDataExportLookbackWindow.UNKNOWN_INCREMENTAL_LOOKBACK,
            no_valid_primary_keys=False,
            infer_columns_from_config=False,
            custom_line_terminator="\n",
            table_relationships=[],
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
            is_code_file=False,
        )
        region_raw_table_config = DirectIngestRegionRawFileConfig(
            region_code=self.region_code,
            raw_file_configs={raw_table_config.file_tag: raw_table_config},
        )

        self.standard_raw_table_dependency_config = (
            DirectIngestViewRawFileDependency.from_raw_table_dependency_arg_name(
                region_raw_table_config=region_raw_table_config,
                raw_table_dependency_arg_name=raw_table_config.file_tag,
            )
        )
        self.all_rows_raw_table_dependency_config = (
            DirectIngestViewRawFileDependency.from_raw_table_dependency_arg_name(
                region_raw_table_config=region_raw_table_config,
                raw_table_dependency_arg_name=f"{raw_table_config.file_tag}@ALL",
            )
        )

    def tearDown(self) -> None:
        self.bq_patcher.stop()
        self.view_builder_patcher.stop()
        self.get_region_patcher.stop()

    def _build_raw_data_fixtures_generator(
        self,
        root_entity_external_ids: List[str],
    ) -> RawDataFixturesGenerator:
        return RawDataFixturesGenerator(
            project_id=self.project_id,
            region_code=self.region_code,
            ingest_view_tag=self.ingest_view_tag,
            output_filename=self.output_filename,
            root_entity_external_ids=root_entity_external_ids,
            root_entity_external_id_columns=self.root_entity_external_id_columns,
            columns_to_randomize=self.columns_to_randomize,
            file_tags_to_load_in_full=[],
            datetime_format="%m/%d/%y",
            randomized_values_map={},
        )

    def test_build_query_for_raw_table_single_column_and_value(self) -> None:
        fixtures_generator = self._build_raw_data_fixtures_generator(
            root_entity_external_ids=["123"]
        )
        query = fixtures_generator.build_query_for_raw_table(
            raw_table_dependency_config=self.standard_raw_table_dependency_config,
            root_entity_external_id_columns=["External_Id_Col"],
        )
        expected_query = """
WITH filtered_rows AS (
    SELECT
        * EXCEPT (recency_rank)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY Primary_Key_Col
                               ORDER BY update_datetime DESC, ORDER BY Primary_Key_Col) AS recency_rank
        FROM
            `recidiviz-test.us_xx_raw_data.raw_data_table`
        
    ) a
    WHERE
        recency_rank = 1
        AND is_deleted = False
)
SELECT Primary_Key_Col, External_Id_Col, External_Id_Col_2, External_Id_Col_3
FROM filtered_rows
WHERE External_Id_Col IN ('123');"""
        self.assertEqual(
            expected_query,
            query,
        )

    def test_build_query_for_raw_table_single_column_multiple_values(self) -> None:
        fixtures_generator = self._build_raw_data_fixtures_generator(
            root_entity_external_ids=["123", "456"]
        )
        query = fixtures_generator.build_query_for_raw_table(
            raw_table_dependency_config=self.standard_raw_table_dependency_config,
            root_entity_external_id_columns=["External_Id_Col"],
        )
        expected_query = """
WITH filtered_rows AS (
    SELECT
        * EXCEPT (recency_rank)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY Primary_Key_Col
                               ORDER BY update_datetime DESC, ORDER BY Primary_Key_Col) AS recency_rank
        FROM
            `recidiviz-test.us_xx_raw_data.raw_data_table`
        
    ) a
    WHERE
        recency_rank = 1
        AND is_deleted = False
)
SELECT Primary_Key_Col, External_Id_Col, External_Id_Col_2, External_Id_Col_3
FROM filtered_rows
WHERE External_Id_Col IN ('123', '456');"""

        self.assertEqual(expected_query, query)

    def test_build_query_for_raw_table_multiple_cols_and_values(self) -> None:
        fixtures_generator = self._build_raw_data_fixtures_generator(
            root_entity_external_ids=["123", "456"]
        )
        query = fixtures_generator.build_query_for_raw_table(
            raw_table_dependency_config=self.standard_raw_table_dependency_config,
            root_entity_external_id_columns=[
                "External_Id_Col",
                "External_Id_Col_2",
                "External_Id_Col_3",
            ],
        )
        expected_query = """
WITH filtered_rows AS (
    SELECT
        * EXCEPT (recency_rank)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY Primary_Key_Col
                               ORDER BY update_datetime DESC, ORDER BY Primary_Key_Col) AS recency_rank
        FROM
            `recidiviz-test.us_xx_raw_data.raw_data_table`
        
    ) a
    WHERE
        recency_rank = 1
        AND is_deleted = False
)
SELECT Primary_Key_Col, External_Id_Col, External_Id_Col_2, External_Id_Col_3
FROM filtered_rows
WHERE External_Id_Col IN ('123', '456') OR External_Id_Col_2 IN ('123', '456') OR External_Id_Col_3 IN ('123', '456');"""
        self.maxDiff = None
        self.assertEqual(expected_query, query)

    def test_build_query_for_raw_table_no_ids(self) -> None:
        fixtures_generator = self._build_raw_data_fixtures_generator(
            root_entity_external_ids=[]
        )
        query = fixtures_generator.build_query_for_raw_table(
            raw_table_dependency_config=self.standard_raw_table_dependency_config,
            root_entity_external_id_columns=[],
        )
        expected_query = """
WITH filtered_rows AS (
    SELECT
        * EXCEPT (recency_rank)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY Primary_Key_Col
                               ORDER BY update_datetime DESC, ORDER BY Primary_Key_Col) AS recency_rank
        FROM
            `recidiviz-test.us_xx_raw_data.raw_data_table`
        
    ) a
    WHERE
        recency_rank = 1
        AND is_deleted = False
)
SELECT Primary_Key_Col, External_Id_Col, External_Id_Col_2, External_Id_Col_3
FROM filtered_rows
;"""

        self.assertEqual(expected_query, query)

    def test_build_query_for_raw_table_historical_export(self) -> None:
        self.standard_raw_table_dependency_config.raw_file_config = attr.evolve(
            self.standard_raw_table_dependency_config.raw_file_config,
            export_lookback_window=RawDataExportLookbackWindow.FULL_HISTORICAL_LOOKBACK,
        )
        fixtures_generator = self._build_raw_data_fixtures_generator(
            root_entity_external_ids=["123"]
        )
        query = fixtures_generator.build_query_for_raw_table(
            raw_table_dependency_config=self.standard_raw_table_dependency_config,
            root_entity_external_id_columns=["External_Id_Col"],
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
filtered_rows AS (
    SELECT *
    FROM
        `recidiviz-test.us_xx_raw_data.raw_data_table`
    WHERE
        file_id = (SELECT file_id FROM max_file_id)
)
SELECT Primary_Key_Col, External_Id_Col, External_Id_Col_2, External_Id_Col_3
FROM filtered_rows
WHERE External_Id_Col IN ('123');"""
        self.assertEqual(expected_query, query)

    def test_build_query_for_raw_table_all_rows_dependency(self) -> None:
        fixtures_generator = self._build_raw_data_fixtures_generator(
            root_entity_external_ids=["123"]
        )
        query = fixtures_generator.build_query_for_raw_table(
            raw_table_dependency_config=self.all_rows_raw_table_dependency_config,
            root_entity_external_id_columns=["External_Id_Col"],
        )
        expected_query = """
WITH filtered_rows AS (
    SELECT *
    FROM `recidiviz-test.us_xx_raw_data.raw_data_table`
    
)
SELECT Primary_Key_Col, External_Id_Col, External_Id_Col_2, External_Id_Col_3, update_datetime
FROM filtered_rows
WHERE External_Id_Col IN ('123');"""
        self.assertEqual(
            expected_query,
            query,
        )
