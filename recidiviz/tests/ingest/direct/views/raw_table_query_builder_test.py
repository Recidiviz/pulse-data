# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for RawTableQueryBuilder"""
# pylint: disable=anomalous-backslash-in-string
import datetime
import unittest

import attr

from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    RawDataClassification,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.raw_table_query_builder import RawTableQueryBuilder


class RawTableQueryBuilderTest(unittest.TestCase):
    """Tests for RawTableQueryBuilder"""

    def setUp(self) -> None:
        self.raw_file_config = DirectIngestRawFileConfig(
            file_tag="table_name",
            file_path="path/to/file.yaml",
            file_description="file description",
            data_classification=RawDataClassification.SOURCE,
            primary_key_cols=["col1"],
            columns=[
                RawTableColumnInfo(
                    name="col1",
                    is_datetime=False,
                    is_pii=False,
                    description="col1 description",
                ),
                RawTableColumnInfo(
                    name="col2",
                    is_datetime=True,
                    is_pii=False,
                    description="col2 description",
                ),
                RawTableColumnInfo(
                    name="col3",
                    is_datetime=True,
                    is_pii=False,
                    description="col3 description",
                    datetime_sql_parsers=[
                        "SAFE.PARSE_TIMESTAMP('%b %e %Y %H:%M:%S', REGEXP_REPLACE({col_name}, r'\:\d\d\d.*', ''))"
                    ],
                ),
                RawTableColumnInfo(
                    name="undocumented_column",
                    is_datetime=True,
                    is_pii=False,
                    description=None,
                ),
                RawTableColumnInfo(
                    name="undocumented_column_2",
                    is_datetime=False,
                    is_pii=False,
                    description=None,
                ),
            ],
            supplemental_order_by_clause="",
            encoding="any-encoding",
            separator="@",
            custom_line_terminator=None,
            ignore_quotes=False,
            always_historical_export=False,
            no_valid_primary_keys=False,
            import_chunk_size_rows=10,
            infer_columns_from_config=False,
        )
        self.query_builder = RawTableQueryBuilder(
            project_id="recidiviz-456",
            region_code="us_xx",
            raw_data_source_instance=DirectIngestInstance.PRIMARY,
        )

    def test_date_filter_query(self) -> None:
        query = self.query_builder.build_query(
            self.raw_file_config,
            address_overrides=None,
            normalized_column_values=True,
            raw_data_datetime_upper_bound=datetime.datetime(2000, 1, 2, 3, 4, 5),
        )

        expected_view_query = """
WITH filtered_rows AS (
    SELECT
        * EXCEPT (recency_rank)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY col1
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            `recidiviz-456.us_xx_raw_data.table_name`
        WHERE update_datetime <= DATETIME(2000, 1, 2, 3, 4, 5)
    ) a
    WHERE
        recency_rank = 1
)
SELECT col1, 
        COALESCE(
            CAST(SAFE_CAST(col2 AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%y', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%Y', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', col2) AS DATETIME) AS STRING),
            col2
        ) AS col2, 
        COALESCE(
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%b %e %Y %H:%M:%S', REGEXP_REPLACE(col3, r'\:\d\d\d.*', '')) AS DATETIME) AS STRING),
            col3
        ) AS col3
FROM filtered_rows
"""

        self.assertEqual(expected_view_query, query)

    def test_date_filter_historical_file_query(self) -> None:
        raw_file_config = attr.evolve(
            self.raw_file_config, always_historical_export=True
        )
        query = self.query_builder.build_query(
            raw_file_config,
            address_overrides=None,
            normalized_column_values=True,
            raw_data_datetime_upper_bound=datetime.datetime(2000, 1, 2, 3, 4, 5),
        )

        expected_view_query = """
WITH max_update_datetime AS (
    SELECT
        MAX(update_datetime) AS update_datetime
    FROM
        `recidiviz-456.us_xx_raw_data.table_name`
    WHERE update_datetime <= DATETIME(2000, 1, 2, 3, 4, 5)
),
max_file_id AS (
    SELECT
        MAX(file_id) AS file_id
    FROM
        `recidiviz-456.us_xx_raw_data.table_name`
    WHERE
        update_datetime = (SELECT update_datetime FROM max_update_datetime)
),
filtered_rows AS (
    SELECT *
    FROM
        `recidiviz-456.us_xx_raw_data.table_name`
    WHERE
        file_id = (SELECT file_id FROM max_file_id)
)
SELECT col1, 
        COALESCE(
            CAST(SAFE_CAST(col2 AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%y', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%Y', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', col2) AS DATETIME) AS STRING),
            col2
        ) AS col2, 
        COALESCE(
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%b %e %Y %H:%M:%S', REGEXP_REPLACE(col3, r'\:\d\d\d.*', '')) AS DATETIME) AS STRING),
            col3
        ) AS col3
FROM filtered_rows
"""

        self.assertEqual(expected_view_query, query)

    def test_no_date_filter_non_historical_file_query(self) -> None:
        query = self.query_builder.build_query(
            self.raw_file_config,
            address_overrides=None,
            normalized_column_values=True,
            raw_data_datetime_upper_bound=None,
        )

        expected_view_query = """
WITH filtered_rows AS (
    SELECT
        * EXCEPT (recency_rank)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY col1
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            `recidiviz-456.us_xx_raw_data.table_name`
        
    ) a
    WHERE
        recency_rank = 1
)
SELECT col1, 
        COALESCE(
            CAST(SAFE_CAST(col2 AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%y', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%Y', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', col2) AS DATETIME) AS STRING),
            col2
        ) AS col2, 
        COALESCE(
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%b %e %Y %H:%M:%S', REGEXP_REPLACE(col3, r'\:\d\d\d.*', '')) AS DATETIME) AS STRING),
            col3
        ) AS col3
FROM filtered_rows
"""

        self.assertEqual(expected_view_query, query)

    def test_no_date_filter_no_normalization_non_historical_file_query(
        self,
    ) -> None:
        query = self.query_builder.build_query(
            self.raw_file_config,
            address_overrides=None,
            normalized_column_values=False,
            raw_data_datetime_upper_bound=None,
        )

        expected_view_query = """
WITH filtered_rows AS (
    SELECT
        * EXCEPT (recency_rank)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY col1
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            `recidiviz-456.us_xx_raw_data.table_name`
        
    ) a
    WHERE
        recency_rank = 1
)
SELECT col1, col2, col3
FROM filtered_rows
"""

        self.assertEqual(expected_view_query, query)

    def test_no_documented_columns_query(
        self,
    ) -> None:
        raw_file_config = attr.evolve(self.raw_file_config, columns=[])

        with self.assertRaisesRegex(
            ValueError,
            r"Found no available \(documented\) columns for file \[table_name\]",
        ):
            _ = self.query_builder.build_query(
                raw_file_config,
                address_overrides=None,
                normalized_column_values=False,
                raw_data_datetime_upper_bound=None,
            )

    def test_no_primary_keys_query(
        self,
    ) -> None:
        raw_file_config = attr.evolve(self.raw_file_config, primary_key_cols=[])
        with self.assertRaisesRegex(
            ValueError,
            r"Found no defined primary key columns for file \[table_name\]",
        ):
            _ = self.query_builder.build_query(
                raw_file_config,
                address_overrides=None,
                normalized_column_values=False,
                raw_data_datetime_upper_bound=None,
            )
