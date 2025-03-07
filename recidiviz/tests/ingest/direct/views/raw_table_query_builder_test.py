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

import attr
import mock
from mock import patch

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_schema_builder import (
    RawDataTableBigQuerySchemaBuilder,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    ColumnUpdateInfo,
    ColumnUpdateOperation,
    DirectIngestRawFileConfig,
    RawDataClassification,
    RawDataExportLookbackWindow,
    RawDataFileUpdateCadence,
    RawTableColumnFieldType,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import FILE_ID_COL_NAME
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.raw_table_query_builder import RawTableQueryBuilder
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)


class RawTableQueryBuilderTest(BigQueryEmulatorTestCase):
    """Tests for RawTableQueryBuilder"""

    def setUp(self) -> None:
        super().setUp()
        self.state_code = StateCode.US_XX
        file_tag = "table_name"
        self.raw_file_config = DirectIngestRawFileConfig(
            state_code=self.state_code,
            file_tag=file_tag,
            file_path="path/to/file.yaml",
            file_description="file description",
            data_classification=RawDataClassification.SOURCE,
            primary_key_cols=["col1"],
            columns=[
                RawTableColumnInfo(
                    name="col1",
                    state_code=self.state_code,
                    file_tag=file_tag,
                    field_type=RawTableColumnFieldType.STRING,
                    is_pii=False,
                    description="col1 description",
                ),
                RawTableColumnInfo(
                    name="col2",
                    state_code=self.state_code,
                    file_tag=file_tag,
                    field_type=RawTableColumnFieldType.DATETIME,
                    is_pii=False,
                    description="col2 description",
                ),
                RawTableColumnInfo(
                    name="col3",
                    state_code=self.state_code,
                    file_tag=file_tag,
                    field_type=RawTableColumnFieldType.DATETIME,
                    is_pii=False,
                    description="col3 description",
                    datetime_sql_parsers=[
                        "SAFE.PARSE_DATETIME('%b %e %Y %H:%M:%S', REGEXP_REPLACE({col_name}, r'\:\d\d\d.*', ''))"
                    ],
                ),
                RawTableColumnInfo(
                    name="undocumented_column",
                    state_code=self.state_code,
                    file_tag=file_tag,
                    field_type=RawTableColumnFieldType.DATETIME,
                    is_pii=False,
                    description=None,
                ),
                RawTableColumnInfo(
                    name="undocumented_column_2",
                    state_code=self.state_code,
                    file_tag=file_tag,
                    field_type=RawTableColumnFieldType.STRING,
                    is_pii=False,
                    description=None,
                ),
                RawTableColumnInfo(
                    name="col6",
                    state_code=self.state_code,
                    file_tag=file_tag,
                    field_type=RawTableColumnFieldType.DATETIME,
                    is_pii=False,
                    description="deleted column should have no effect",
                    update_history=[
                        ColumnUpdateInfo(
                            update_type=ColumnUpdateOperation.DELETION,
                            update_datetime=datetime.datetime(
                                2000, 1, 1, tzinfo=datetime.timezone.utc
                            ),
                        )
                    ],
                ),
            ],
            supplemental_order_by_clause="",
            encoding="any-encoding",
            separator="@",
            custom_line_terminator=None,
            ignore_quotes=False,
            export_lookback_window=RawDataExportLookbackWindow.UNKNOWN_INCREMENTAL_LOOKBACK,
            no_valid_primary_keys=False,
            import_chunk_size_rows=10,
            infer_columns_from_config=False,
            table_relationships=[],
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
        )
        self.query_builder = RawTableQueryBuilder(
            project_id=self.project_id,
            region_code=self.state_code.value.lower(),
            raw_data_source_instance=DirectIngestInstance.PRIMARY,
        )

    def load_empty_raw_table(self, raw_file_config: DirectIngestRawFileConfig) -> None:
        dataset_id = "us_xx_raw_data"
        self.bq_client.create_dataset_if_necessary(dataset_id)
        schema_fields = RawDataTableBigQuerySchemaBuilder.build_bq_schema_for_config(
            raw_file_config=raw_file_config,
        )
        self.bq_client.create_table_with_schema(
            address=BigQueryAddress(
                dataset_id=dataset_id,
                table_id=raw_file_config.file_tag,
            ),
            schema_fields=schema_fields,
            clustering_fields=[FILE_ID_COL_NAME],
        )

    def test_date_and_latest_filter_query(self) -> None:
        self.load_empty_raw_table(self.raw_file_config)
        query = self.query_builder.build_query(
            self.raw_file_config,
            parent_address_overrides=None,
            parent_address_formatter_provider=None,
            normalized_column_values=True,
            raw_data_datetime_upper_bound=datetime.datetime(2000, 1, 2, 3, 4, 5, 6),
            filter_to_latest=True,
            filter_to_only_documented_columns=True,
        )
        # Make sure query is valid SQL by running it
        self.bq_client.run_query_async(query_str=query, use_query_cache=False).result()

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
            `recidiviz-bq-emulator-project.us_xx_raw_data.table_name`
        WHERE update_datetime <= DATETIME "2000-01-02T03:04:05.000006"
    ) a
    WHERE
        recency_rank = 1
        AND is_deleted = False
)
SELECT col1, 
        COALESCE(
            CAST(SAFE_CAST(col2 AS DATETIME) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%y', col2) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%Y', col2) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M', col2) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%Y %H:%M:%S', col2) AS STRING),
            col2
        ) AS col2, 
        COALESCE(
            CAST(SAFE.PARSE_DATETIME('%b %e %Y %H:%M:%S', REGEXP_REPLACE(col3, r'\:\d\d\d.*', '')) AS STRING),
            col3
        ) AS col3
FROM filtered_rows
"""

        self.assertEqual(expected_view_query, query)

    def test_only_date_filter_query(self) -> None:
        self.load_empty_raw_table(self.raw_file_config)
        query = self.query_builder.build_query(
            self.raw_file_config,
            parent_address_overrides=None,
            parent_address_formatter_provider=None,
            normalized_column_values=True,
            raw_data_datetime_upper_bound=datetime.datetime(2000, 1, 2, 3, 4, 5, 6),
            filter_to_latest=False,
            filter_to_only_documented_columns=True,
        )
        # Make sure query is valid SQL by running it
        self.bq_client.run_query_async(query_str=query, use_query_cache=False).result()

        expected_view_query = """
WITH filtered_rows AS (
    SELECT *
    FROM `recidiviz-bq-emulator-project.us_xx_raw_data.table_name`
    WHERE update_datetime <= DATETIME "2000-01-02T03:04:05.000006"
)
SELECT col1, 
        COALESCE(
            CAST(SAFE_CAST(col2 AS DATETIME) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%y', col2) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%Y', col2) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M', col2) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%Y %H:%M:%S', col2) AS STRING),
            col2
        ) AS col2, 
        COALESCE(
            CAST(SAFE.PARSE_DATETIME('%b %e %Y %H:%M:%S', REGEXP_REPLACE(col3, r'\:\d\d\d.*', '')) AS STRING),
            col3
        ) AS col3, update_datetime
FROM filtered_rows
"""

        self.assertEqual(expected_view_query, query)

    def test_date_and_latest_filter_historical_file_query_cannot_prune(self) -> None:
        raw_file_config = attr.evolve(
            self.raw_file_config,
            export_lookback_window=RawDataExportLookbackWindow.FULL_HISTORICAL_LOOKBACK,
        )
        self.load_empty_raw_table(raw_file_config)
        query = self.query_builder.build_query(
            raw_file_config,
            parent_address_overrides=None,
            parent_address_formatter_provider=None,
            normalized_column_values=True,
            raw_data_datetime_upper_bound=datetime.datetime(2000, 1, 2, 3, 4, 5, 6),
            filter_to_latest=True,
            filter_to_only_documented_columns=True,
        )
        # Make sure query is valid SQL by running it
        self.bq_client.run_query_async(query_str=query, use_query_cache=False).result()

        expected_view_query = """
WITH max_update_datetime AS (
    SELECT
        MAX(update_datetime) AS update_datetime
    FROM
        `recidiviz-bq-emulator-project.us_xx_raw_data.table_name`
    WHERE update_datetime <= DATETIME "2000-01-02T03:04:05.000006"
),
max_file_id AS (
    SELECT
        MAX(file_id) AS file_id
    FROM
        `recidiviz-bq-emulator-project.us_xx_raw_data.table_name`
    WHERE
        update_datetime = (SELECT update_datetime FROM max_update_datetime)
),
filtered_rows AS (
    SELECT *
    FROM
        `recidiviz-bq-emulator-project.us_xx_raw_data.table_name`
    WHERE
        file_id = (SELECT file_id FROM max_file_id)
)
SELECT col1, 
        COALESCE(
            CAST(SAFE_CAST(col2 AS DATETIME) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%y', col2) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%Y', col2) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M', col2) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%Y %H:%M:%S', col2) AS STRING),
            col2
        ) AS col2, 
        COALESCE(
            CAST(SAFE.PARSE_DATETIME('%b %e %Y %H:%M:%S', REGEXP_REPLACE(col3, r'\:\d\d\d.*', '')) AS STRING),
            col3
        ) AS col3
FROM filtered_rows
"""

        self.assertEqual(expected_view_query, query)

    def test_no_filters_no_normalization_query(self) -> None:
        raw_file_config = attr.evolve(
            self.raw_file_config,
            export_lookback_window=RawDataExportLookbackWindow.FULL_HISTORICAL_LOOKBACK,
        )
        self.load_empty_raw_table(raw_file_config)
        query = self.query_builder.build_query(
            raw_file_config,
            parent_address_overrides=None,
            parent_address_formatter_provider=None,
            normalized_column_values=False,
            raw_data_datetime_upper_bound=None,
            filter_to_latest=False,
            filter_to_only_documented_columns=True,
        )
        # Make sure query is valid SQL by running it
        self.bq_client.run_query_async(query_str=query, use_query_cache=False).result()

        expected_view_query = """
WITH filtered_rows AS (
    SELECT *
    FROM `recidiviz-bq-emulator-project.us_xx_raw_data.table_name`
    
)
SELECT col1, col2, col3, update_datetime
FROM filtered_rows
"""

        self.assertEqual(expected_view_query, query)

    def test_no_date_filter_non_historical_file_query(self) -> None:
        self.load_empty_raw_table(self.raw_file_config)
        query = self.query_builder.build_query(
            self.raw_file_config,
            parent_address_overrides=None,
            parent_address_formatter_provider=None,
            normalized_column_values=True,
            raw_data_datetime_upper_bound=None,
            filter_to_latest=True,
            filter_to_only_documented_columns=True,
        )
        # Make sure query is valid SQL by running it
        self.bq_client.run_query_async(query_str=query, use_query_cache=False).result()

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
            `recidiviz-bq-emulator-project.us_xx_raw_data.table_name`
        
    ) a
    WHERE
        recency_rank = 1
        AND is_deleted = False
)
SELECT col1, 
        COALESCE(
            CAST(SAFE_CAST(col2 AS DATETIME) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%y', col2) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%Y', col2) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M', col2) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%Y %H:%M:%S', col2) AS STRING),
            col2
        ) AS col2, 
        COALESCE(
            CAST(SAFE.PARSE_DATETIME('%b %e %Y %H:%M:%S', REGEXP_REPLACE(col3, r'\:\d\d\d.*', '')) AS STRING),
            col3
        ) AS col3
FROM filtered_rows
"""

        self.assertEqual(expected_view_query, query)

    def test_no_date_filter_no_normalization_non_historical_file_query(
        self,
    ) -> None:
        self.load_empty_raw_table(self.raw_file_config)
        query = self.query_builder.build_query(
            self.raw_file_config,
            parent_address_overrides=None,
            parent_address_formatter_provider=None,
            normalized_column_values=False,
            raw_data_datetime_upper_bound=None,
            filter_to_latest=True,
            filter_to_only_documented_columns=True,
        )
        # Make sure query is valid SQL by running it
        self.bq_client.run_query_async(query_str=query, use_query_cache=False).result()

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
            `recidiviz-bq-emulator-project.us_xx_raw_data.table_name`
        
    ) a
    WHERE
        recency_rank = 1
        AND is_deleted = False
)
SELECT col1, col2, col3
FROM filtered_rows
"""

        self.assertEqual(expected_view_query, query)

    def test_no_documented_columns_query(
        self,
    ) -> None:
        raw_file_config = attr.evolve(
            self.raw_file_config, columns=[], primary_key_cols=[]
        )

        with self.assertRaisesRegex(
            ValueError,
            r"Found no available \(documented\) columns for file \[table_name\]",
        ):
            _ = self.query_builder.build_query(
                raw_file_config,
                parent_address_overrides=None,
                parent_address_formatter_provider=None,
                normalized_column_values=False,
                raw_data_datetime_upper_bound=None,
                filter_to_latest=True,
                filter_to_only_documented_columns=True,
            )

        with self.assertRaisesRegex(
            ValueError,
            r"Found no available \(documented\) columns for file \[table_name\]",
        ):
            _ = self.query_builder.build_query(
                raw_file_config,
                parent_address_overrides=None,
                parent_address_formatter_provider=None,
                normalized_column_values=False,
                raw_data_datetime_upper_bound=None,
                filter_to_latest=False,
                filter_to_only_documented_columns=True,
            )

    def test_no_valid_primary_keys_nonempty(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Incorrect primary key setup found for file_tag=table_name: `no_valid_primary_keys`=True and "
            r"`primary_key_cols` is not empty: \['col1'\]",
        ):
            _ = attr.evolve(self.raw_file_config, no_valid_primary_keys=True)

    @patch(
        "recidiviz.ingest.direct.views.raw_table_query_builder.raw_data_pruning_enabled_in_state_and_instance"
    )
    def test_always_historical_can_prune(self, mock_is_enabled: mock.MagicMock) -> None:
        mock_is_enabled.return_value = True
        raw_file_config = attr.evolve(
            self.raw_file_config,
            export_lookback_window=RawDataExportLookbackWindow.FULL_HISTORICAL_LOOKBACK,
        )
        self.load_empty_raw_table(raw_file_config)
        query = self.query_builder.build_query(
            raw_file_config,
            parent_address_overrides=None,
            parent_address_formatter_provider=None,
            normalized_column_values=True,
            raw_data_datetime_upper_bound=None,
            filter_to_latest=True,
            filter_to_only_documented_columns=True,
        )
        # Make sure query is valid SQL by running it
        self.bq_client.run_query_async(query_str=query, use_query_cache=False).result()

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
            `recidiviz-bq-emulator-project.us_xx_raw_data.table_name`
        
    ) a
    WHERE
        recency_rank = 1
        AND is_deleted = False
)
SELECT col1, 
        COALESCE(
            CAST(SAFE_CAST(col2 AS DATETIME) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%y', col2) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%Y', col2) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M', col2) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%Y %H:%M:%S', col2) AS STRING),
            col2
        ) AS col2, 
        COALESCE(
            CAST(SAFE.PARSE_DATETIME('%b %e %Y %H:%M:%S', REGEXP_REPLACE(col3, r'\:\d\d\d.*', '')) AS STRING),
            col3
        ) AS col3
FROM filtered_rows
"""

        self.assertEqual(expected_view_query, query)

    @patch(
        "recidiviz.ingest.direct.views.raw_table_query_builder.raw_data_pruning_enabled_in_state_and_instance"
    )
    def test_always_historical_can_prune_with_undocumented_columns(
        self, mock_is_enabled: mock.MagicMock
    ) -> None:
        mock_is_enabled.return_value = True
        raw_file_config = attr.evolve(
            self.raw_file_config,
            export_lookback_window=RawDataExportLookbackWindow.FULL_HISTORICAL_LOOKBACK,
        )
        self.load_empty_raw_table(raw_file_config)
        query = self.query_builder.build_query(
            raw_file_config,
            parent_address_overrides=None,
            parent_address_formatter_provider=None,
            normalized_column_values=True,
            raw_data_datetime_upper_bound=None,
            filter_to_latest=True,
            filter_to_only_documented_columns=False,
        )
        # Make sure query is valid SQL by running it
        self.bq_client.run_query_async(query_str=query, use_query_cache=False).result()

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
            `recidiviz-bq-emulator-project.us_xx_raw_data.table_name`
        
    ) a
    WHERE
        recency_rank = 1
        AND is_deleted = False
)
SELECT col1, 
        COALESCE(
            CAST(SAFE_CAST(col2 AS DATETIME) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%y', col2) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%Y', col2) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M', col2) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%Y %H:%M:%S', col2) AS STRING),
            col2
        ) AS col2, 
        COALESCE(
            CAST(SAFE.PARSE_DATETIME('%b %e %Y %H:%M:%S', REGEXP_REPLACE(col3, r'\:\d\d\d.*', '')) AS STRING),
            col3
        ) AS col3, 
        COALESCE(
            CAST(SAFE_CAST(undocumented_column AS DATETIME) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%y', undocumented_column) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%Y', undocumented_column) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M', undocumented_column) AS STRING),
            CAST(SAFE.PARSE_DATETIME('%m/%d/%Y %H:%M:%S', undocumented_column) AS STRING),
            undocumented_column
        ) AS undocumented_column, undocumented_column_2
FROM filtered_rows
"""

        self.assertEqual(expected_view_query, query)

    def test_only_datetime_documented_columns(self) -> None:
        raw_file_config = attr.evolve(
            self.raw_file_config,
            primary_key_cols=["undocumented_column"],
            columns=[
                RawTableColumnInfo(
                    name="datetime_col",
                    state_code=self.state_code,
                    file_tag=self.raw_file_config.file_tag,
                    field_type=RawTableColumnFieldType.DATETIME,
                    is_pii=False,
                    description="col3 description",
                    datetime_sql_parsers=[
                        "SAFE.PARSE_DATETIME('%b %e %Y %H:%M:%S', REGEXP_REPLACE({col_name}, r'\:\d\d\d.*', ''))"
                    ],
                ),
                RawTableColumnInfo(
                    name="undocumented_column",
                    state_code=self.state_code,
                    file_tag=self.raw_file_config.file_tag,
                    field_type=RawTableColumnFieldType.DATETIME,
                    is_pii=False,
                    description=None,
                ),
                RawTableColumnInfo(
                    name="undocumented_column_2",
                    state_code=self.state_code,
                    file_tag=self.raw_file_config.file_tag,
                    field_type=RawTableColumnFieldType.STRING,
                    is_pii=False,
                    description=None,
                ),
            ],
        )
        self.load_empty_raw_table(raw_file_config)
        query = self.query_builder.build_query(
            raw_file_config,
            parent_address_overrides=None,
            parent_address_formatter_provider=None,
            normalized_column_values=True,
            raw_data_datetime_upper_bound=None,
            filter_to_latest=True,
            filter_to_only_documented_columns=True,
        )
        # Make sure query is valid SQL by running it
        self.bq_client.run_query_async(query_str=query, use_query_cache=False).result()

        expected_view_query = """
WITH filtered_rows AS (
    SELECT
        * EXCEPT (recency_rank)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY undocumented_column
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            `recidiviz-bq-emulator-project.us_xx_raw_data.table_name`
        
    ) a
    WHERE
        recency_rank = 1
        AND is_deleted = False
)
SELECT 
        COALESCE(
            CAST(SAFE.PARSE_DATETIME('%b %e %Y %H:%M:%S', REGEXP_REPLACE(datetime_col, r'\:\d\d\d.*', '')) AS STRING),
            datetime_col
        ) AS datetime_col
FROM filtered_rows
"""

        self.assertEqual(expected_view_query, query)

    def test_null_values(self) -> None:
        raw_file_config = attr.evolve(
            self.raw_file_config,
            primary_key_cols=["undocumented_column"],
            columns=[
                RawTableColumnInfo(
                    name="datetime_col",
                    state_code=self.state_code,
                    file_tag=self.raw_file_config.file_tag,
                    field_type=RawTableColumnFieldType.DATETIME,
                    is_pii=False,
                    description="description",
                    datetime_sql_parsers=[
                        "SAFE.PARSE_DATETIME('%b %e %Y %H:%M:%S', REGEXP_REPLACE({col_name}, r'\:\d\d\d.*', ''))"
                    ],
                    null_values=["0000"],
                ),
                RawTableColumnInfo(
                    name="documented_column",
                    state_code=self.state_code,
                    file_tag=self.raw_file_config.file_tag,
                    field_type=RawTableColumnFieldType.STRING,
                    is_pii=False,
                    description="description",
                    null_values=["00", "N/A"],
                ),
                RawTableColumnInfo(
                    name="undocumented_column",
                    state_code=self.state_code,
                    file_tag=self.raw_file_config.file_tag,
                    field_type=RawTableColumnFieldType.DATETIME,
                    is_pii=False,
                    description=None,
                    null_values=[],
                ),
            ],
        )
        self.load_empty_raw_table(raw_file_config)
        query = self.query_builder.build_query(
            raw_file_config,
            parent_address_overrides=None,
            parent_address_formatter_provider=None,
            normalized_column_values=True,
            raw_data_datetime_upper_bound=None,
            filter_to_latest=True,
            filter_to_only_documented_columns=True,
        )
        # Make sure query is valid SQL by running it
        self.bq_client.run_query_async(query_str=query, use_query_cache=False).result()

        expected_view_query = """
WITH filtered_rows AS (
    SELECT
        * EXCEPT (recency_rank)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY undocumented_column
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            `recidiviz-bq-emulator-project.us_xx_raw_data.table_name`
        
    ) a
    WHERE
        recency_rank = 1
        AND is_deleted = False
)
SELECT 
    CASE
        WHEN datetime_col IN ('0000') THEN NULL
        ELSE 
        COALESCE(
            CAST(SAFE.PARSE_DATETIME('%b %e %Y %H:%M:%S', REGEXP_REPLACE(datetime_col, r'\:\d\d\d.*', '')) AS STRING),
            datetime_col
        )
    END AS datetime_col, 
    CASE
        WHEN documented_column IN ('00', 'N/A') THEN NULL
        ELSE documented_column
    END AS documented_column
FROM filtered_rows
"""
        self.assertEqual(expected_view_query, query)
