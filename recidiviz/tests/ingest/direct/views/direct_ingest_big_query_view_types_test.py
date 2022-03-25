# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for types defined in direct_ingest_big_query_view_types_test.py"""
import unittest

import attr
from mock import patch

from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    RAW_DATA_LATEST_HISTORICAL_FILE_VIEW_QUERY_TEMPLATE,
    RAW_DATA_LATEST_VIEW_QUERY_TEMPLATE,
    RAW_DATA_UP_TO_DATE_HISTORICAL_FILE_VIEW_QUERY_TEMPLATE,
    RAW_DATA_UP_TO_DATE_VIEW_QUERY_TEMPLATE,
    UPDATE_DATETIME_PARAM_NAME,
    DestinationTableType,
    DirectIngestPreProcessedIngestView,
    DirectIngestRawDataTableLatestView,
    DirectIngestRawDataTableUpToDateView,
    RawTableViewType,
)
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module
from recidiviz.utils.string import StrictStringFormatter


class DirectIngestBigQueryViewTypesTest(unittest.TestCase):
    """Tests for types defined in direct_ingest_big_query_view_types_test.py"""

    PROJECT_ID = "recidiviz-456"

    DEFAULT_LATEST_CONFIG = DirectIngestPreProcessedIngestView.QueryStructureConfig(
        raw_table_view_type=RawTableViewType.LATEST
    )

    DEFAULT_PARAMETERIZED_CONFIG = (
        DirectIngestPreProcessedIngestView.QueryStructureConfig(
            raw_table_view_type=RawTableViewType.PARAMETERIZED
        )
    )

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.PROJECT_ID

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_raw_latest_view(self) -> None:
        view = DirectIngestRawDataTableLatestView(
            region_code="us_xx",
            raw_file_config=DirectIngestRawFileConfig(
                file_tag="table_name",
                file_path="path/to/file.yaml",
                file_description="file description",
                primary_key_cols=["col1", "col2"],
                columns=[
                    RawTableColumnInfo(
                        name="col1", is_datetime=False, description="col1 description"
                    ),
                    RawTableColumnInfo(
                        name="col2", is_datetime=False, description="col2 description"
                    ),
                ],
                supplemental_order_by_clause="CAST(seq_num AS INT64)",
                encoding="any-encoding",
                separator="@",
                custom_line_terminator=None,
                ignore_quotes=False,
                always_historical_export=False,
                import_chunk_size_rows=10,
            ),
            dataset_overrides=None,
        )

        self.assertEqual(self.PROJECT_ID, view.project)
        self.assertEqual("us_xx_raw_data_up_to_date_views", view.dataset_id)
        self.assertEqual("table_name_latest", view.table_id)
        self.assertEqual("table_name_latest", view.view_id)

        expected_view_query = StrictStringFormatter().format(
            RAW_DATA_LATEST_VIEW_QUERY_TEMPLATE,
            project_id=self.PROJECT_ID,
            raw_table_primary_key_str="col1, col2",
            raw_table_dataset_id="us_xx_raw_data",
            raw_table_name="table_name",
            columns_clause="col1, col2",
            normalized_columns="*",
            supplemental_order_by_clause=", CAST(seq_num AS INT64)",
        )

        self.assertEqual(expected_view_query, view.view_query)
        self.assertEqual(
            "SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.table_name_latest`",
            view.select_query,
        )

    def test_raw_latest_historical_file_view(self) -> None:
        view = DirectIngestRawDataTableLatestView(
            region_code="us_xx",
            raw_file_config=DirectIngestRawFileConfig(
                file_tag="table_name",
                file_path="path/to/file.yaml",
                file_description="file description",
                primary_key_cols=["col1", "col2"],
                columns=[
                    RawTableColumnInfo(
                        name="col1", is_datetime=False, description="col1 description"
                    ),
                    RawTableColumnInfo(
                        name="col2", is_datetime=False, description="col2 description"
                    ),
                ],
                supplemental_order_by_clause="CAST(seq_num AS INT64)",
                encoding="any-encoding",
                separator="@",
                custom_line_terminator=None,
                ignore_quotes=False,
                always_historical_export=True,
                import_chunk_size_rows=10,
            ),
            dataset_overrides=None,
        )

        self.assertEqual(self.PROJECT_ID, view.project)
        self.assertEqual("us_xx_raw_data_up_to_date_views", view.dataset_id)
        self.assertEqual("table_name_latest", view.table_id)
        self.assertEqual("table_name_latest", view.view_id)

        expected_view_query = StrictStringFormatter().format(
            RAW_DATA_LATEST_HISTORICAL_FILE_VIEW_QUERY_TEMPLATE,
            project_id=self.PROJECT_ID,
            raw_table_primary_key_str="col1, col2",
            raw_table_dataset_id="us_xx_raw_data",
            raw_table_name="table_name",
            columns_clause="col1, col2",
            normalized_columns="*",
            supplemental_order_by_clause=", CAST(seq_num AS INT64)",
        )

        self.assertEqual(expected_view_query, view.view_query)
        self.assertEqual(
            "SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.table_name_latest`",
            view.select_query,
        )

    def test_raw_up_to_date_view(self) -> None:
        view = DirectIngestRawDataTableUpToDateView(
            region_code="us_xx",
            include_undocumented_columns=True,
            raw_file_config=DirectIngestRawFileConfig(
                file_tag="table_name",
                file_path="path/to/file.yaml",
                file_description="file description",
                primary_key_cols=["col1"],
                columns=[
                    RawTableColumnInfo(
                        name="col1", is_datetime=False, description="col1 description"
                    ),
                    RawTableColumnInfo(
                        name="col2", is_datetime=True, description="col2 description"
                    ),
                    RawTableColumnInfo(
                        name="undocumented_column", is_datetime=True, description=None
                    ),
                ],
                supplemental_order_by_clause="",
                encoding="any-encoding",
                separator="@",
                custom_line_terminator=None,
                ignore_quotes=False,
                always_historical_export=False,
                import_chunk_size_rows=10,
            ),
        )

        self.assertEqual(self.PROJECT_ID, view.project)
        self.assertEqual("us_xx_raw_data_up_to_date_views", view.dataset_id)
        self.assertEqual("table_name_by_update_date", view.table_id)
        self.assertEqual("table_name_by_update_date", view.view_id)

        expected_datetime_cols_clause = """
        COALESCE(
            CAST(SAFE_CAST(col2 AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%y', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%Y', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', col2) AS DATETIME) AS STRING),
            col2
        ) AS col2, 
        COALESCE(
            CAST(SAFE_CAST(undocumented_column AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%y', undocumented_column) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%Y', undocumented_column) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M', undocumented_column) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', undocumented_column) AS DATETIME) AS STRING),
            undocumented_column
        ) AS undocumented_column"""

        expected_view_query = StrictStringFormatter().format(
            RAW_DATA_UP_TO_DATE_VIEW_QUERY_TEMPLATE,
            project_id=self.PROJECT_ID,
            raw_table_primary_key_str="col1",
            raw_table_dataset_id="us_xx_raw_data",
            raw_table_name="table_name",
            columns_clause="col1, col2",
            normalized_columns=f"col1, update_datetime, {expected_datetime_cols_clause}",
            supplemental_order_by_clause="",
        )

        self.assertEqual(expected_view_query, view.view_query)
        self.assertEqual(
            "SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.table_name_by_update_date`",
            view.select_query,
        )

    def test_raw_up_to_date_historical_file_view(self) -> None:
        view = DirectIngestRawDataTableUpToDateView(
            region_code="us_xx",
            raw_file_config=DirectIngestRawFileConfig(
                file_tag="table_name",
                file_path="path/to/file.yaml",
                file_description="file description",
                primary_key_cols=["col1"],
                columns=[
                    RawTableColumnInfo(
                        name="col1", is_datetime=False, description="col1 description"
                    ),
                    RawTableColumnInfo(
                        name="col2", is_datetime=True, description="col2 description"
                    ),
                ],
                supplemental_order_by_clause="",
                encoding="any-encoding",
                separator="@",
                custom_line_terminator=None,
                ignore_quotes=False,
                always_historical_export=True,
                import_chunk_size_rows=10,
            ),
        )

        self.assertEqual(self.PROJECT_ID, view.project)
        self.assertEqual("us_xx_raw_data_up_to_date_views", view.dataset_id)
        self.assertEqual("table_name_by_update_date", view.table_id)
        self.assertEqual("table_name_by_update_date", view.view_id)

        expected_datetime_cols_clause = """
        COALESCE(
            CAST(SAFE_CAST(col2 AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%y', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%Y', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', col2) AS DATETIME) AS STRING),
            col2
        ) AS col2"""

        expected_view_query = StrictStringFormatter().format(
            RAW_DATA_UP_TO_DATE_HISTORICAL_FILE_VIEW_QUERY_TEMPLATE,
            project_id=self.PROJECT_ID,
            raw_table_primary_key_str="col1",
            raw_table_dataset_id="us_xx_raw_data",
            raw_table_name="table_name",
            columns_clause="col1, col2",
            normalized_columns=f"col1, update_datetime, {expected_datetime_cols_clause}",
            supplemental_order_by_clause="",
        )

        self.assertEqual(expected_view_query, view.view_query)
        self.assertEqual(
            "SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.table_name_by_update_date`",
            view.select_query,
        )

    def test_direct_ingest_preprocessed_view(self) -> None:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions_module,
        )

        view_query_template = """SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_second}
USING (col1);"""

        view = DirectIngestPreProcessedIngestView(
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_raw_table_config=region_config,
            order_by_cols="col1, col2",
            is_detect_row_deletion_view=False,
            primary_key_tables_for_entity_deletion=[],
        )

        self.assertEqual(
            ["file_tag_first", "file_tag_second"],
            [c.file_tag for c in view.raw_table_dependency_configs],
        )

        expected_view_query = """WITH
file_tag_first_generated_view AS (
    SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.file_tag_first_latest`
),
file_tag_second_generated_view AS (
    SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.file_tag_second_latest`
)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN file_tag_second_generated_view
USING (col1)
ORDER BY col1, col2;"""

        self.assertEqual(
            expected_view_query,
            view.expanded_view_query(config=self.DEFAULT_LATEST_CONFIG),
        )

        expected_parameterized_view_query = """WITH
file_tag_first_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_first`
        WHERE
            update_datetime <= @my_update_timestamp_param_name
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_1a, col_name_1b,
            ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
),
file_tag_second_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_second`
        WHERE
            update_datetime <= @my_update_timestamp_param_name
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_2a,
            ROW_NUMBER() OVER (PARTITION BY col_name_2a
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN file_tag_second_generated_view
USING (col1)
ORDER BY col1, col2;"""

        self.assertEqual(
            expected_parameterized_view_query,
            view.expanded_view_query(
                config=DirectIngestPreProcessedIngestView.QueryStructureConfig(
                    raw_table_view_type=RawTableViewType.PARAMETERIZED,
                    param_name_override="my_update_timestamp_param_name",
                )
            ),
        )

    def test_direct_ingest_preprocessed_view_detect_row_deletion_no_historical_table(
        self,
    ) -> None:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions_module,
        )

        view_query_template = """SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_second}
USING (col1);"""

        with self.assertRaisesRegex(
            ValueError,
            "^Ingest view ingest_view_tag is marked as `is_detect_row_deletion_view` and has table file_tag_second "
            "specified in `primary_key_tables_for_entity_deletion`; however the raw data file is not marked as always "
            "being exported as historically.",
        ):
            DirectIngestPreProcessedIngestView(
                ingest_view_name="ingest_view_tag",
                view_query_template=view_query_template,
                region_raw_table_config=region_config,
                order_by_cols="col1, col2",
                is_detect_row_deletion_view=True,
                primary_key_tables_for_entity_deletion=["file_tag_second"],
            )

    def test_direct_ingest_preprocessed_view_no_raw_file_config_columns_defined(
        self,
    ) -> None:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_ww",
            region_module=fake_regions_module,
        )

        view_query_template = """SELECT * FROM {tagColumnsMissing};"""

        with self.assertRaisesRegex(
            ValueError,
            r"^Found empty set of columns in raw table config \[tagColumnsMissing\]"
            r" in region \[us_ww\].$",
        ):
            DirectIngestPreProcessedIngestView(
                ingest_view_name="ingest_view_tag",
                view_query_template=view_query_template,
                region_raw_table_config=region_config,
                order_by_cols=None,
                is_detect_row_deletion_view=False,
                primary_key_tables_for_entity_deletion=[],
            )

    def test_direct_ingest_preprocessed_view_detect_row_deletion_no_pk_tables_specified(
        self,
    ) -> None:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions_module,
        )

        view_query_template = """SELECT * FROM {file_tag_first}
        LEFT OUTER JOIN {tagFullHistoricalExport}
        USING (col1);"""

        with self.assertRaisesRegex(
            ValueError,
            "^Ingest view ingest_view_tag was marked as `is_detect_row_deletion_view`; however no "
            "`primary_key_tables_for_entity_deletion` were defined.",
        ):
            DirectIngestPreProcessedIngestView(
                ingest_view_name="ingest_view_tag",
                view_query_template=view_query_template,
                region_raw_table_config=region_config,
                order_by_cols="col1, col2",
                is_detect_row_deletion_view=True,
                primary_key_tables_for_entity_deletion=[],
            )

    def test_direct_ingest_preprocessed_view_detect_row_deletion_unknown_pk_table_specified(
        self,
    ) -> None:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions_module,
        )

        view_query_template = """SELECT * FROM {file_tag_first}
        LEFT OUTER JOIN {tagFullHistoricalExport}
        USING (col1);"""

        with self.assertRaisesRegex(
            ValueError,
            "^Ingest view ingest_view_tag has specified unknown in "
            "`primary_key_tables_for_entity_deletion`, but that "
            "raw file tag was not found as a dependency.",
        ):
            DirectIngestPreProcessedIngestView(
                ingest_view_name="ingest_view_tag",
                view_query_template=view_query_template,
                region_raw_table_config=region_config,
                order_by_cols="col1, col2",
                is_detect_row_deletion_view=True,
                primary_key_tables_for_entity_deletion=[
                    "tagFullHistoricalExport",
                    "unknown",
                ],
            )

    def test_direct_ingest_preprocessed_view_detect_row_deletion(self) -> None:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions_module,
        )

        view_query_template = """SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {tagFullHistoricalExport}
USING (col1);"""

        view = DirectIngestPreProcessedIngestView(
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_raw_table_config=region_config,
            order_by_cols="col1, col2",
            is_detect_row_deletion_view=True,
            primary_key_tables_for_entity_deletion=["tagFullHistoricalExport"],
        )

        self.assertEqual(
            ["file_tag_first", "tagFullHistoricalExport"],
            [c.file_tag for c in view.raw_table_dependency_configs],
        )

        expected_view_query = """WITH
file_tag_first_generated_view AS (
    SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.file_tag_first_latest`
),
tagFullHistoricalExport_generated_view AS (
    SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.tagFullHistoricalExport_latest`
)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN tagFullHistoricalExport_generated_view
USING (col1)
ORDER BY col1, col2;"""

        self.assertEqual(
            expected_view_query,
            view.expanded_view_query(config=self.DEFAULT_LATEST_CONFIG),
        )

        expected_parameterized_view_query = """WITH
file_tag_first_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_first`
        WHERE
            update_datetime <= @my_update_timestamp_param_name
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_1a, col_name_1b,
            ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
),
tagFullHistoricalExport_generated_view AS (
    WITH max_update_datetime AS (
        SELECT
            MAX(update_datetime) AS update_datetime
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            update_datetime <= @my_update_timestamp_param_name
    ),
    max_file_id AS (
        SELECT
            MAX(file_id) AS file_id
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            update_datetime = (SELECT update_datetime FROM max_update_datetime)
    ),
    normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            file_id = (SELECT file_id FROM max_file_id)
    ),
    rows_with_recency_rank AS (
        SELECT
            COL_1,
            ROW_NUMBER() OVER (PARTITION BY COL_1
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )
    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN tagFullHistoricalExport_generated_view
USING (col1)
ORDER BY col1, col2;"""

        self.assertEqual(
            expected_parameterized_view_query,
            view.expanded_view_query(
                config=DirectIngestPreProcessedIngestView.QueryStructureConfig(
                    raw_table_view_type=RawTableViewType.PARAMETERIZED,
                    param_name_override="my_update_timestamp_param_name",
                )
            ),
        )

    def test_direct_ingest_preprocessed_view_with_reference_table(self) -> None:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions_module,
        )

        view_query_template = """SELECT * FROM {file_tag_first}
LEFT OUTER JOIN `{{project_id}}.reference_tables.my_table`
USING (col1);"""

        view = DirectIngestPreProcessedIngestView(
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_raw_table_config=region_config,
            order_by_cols="col1, col2",
            is_detect_row_deletion_view=False,
            primary_key_tables_for_entity_deletion=[],
        )

        self.assertEqual(
            ["file_tag_first"], [c.file_tag for c in view.raw_table_dependency_configs]
        )

        expected_view_query = """WITH
file_tag_first_generated_view AS (
    SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.file_tag_first_latest`
)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN `recidiviz-456.reference_tables.my_table`
USING (col1)
ORDER BY col1, col2;"""

        self.assertEqual(
            expected_view_query,
            view.expanded_view_query(config=self.DEFAULT_LATEST_CONFIG),
        )

        expected_date_parameterized_view_query = """WITH
file_tag_first_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_first`
        WHERE
            update_datetime <= @my_param
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_1a, col_name_1b,
            ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN `recidiviz-456.reference_tables.my_table`
USING (col1)
ORDER BY col1, col2;"""

        self.assertEqual(
            expected_date_parameterized_view_query,
            view.expanded_view_query(
                config=DirectIngestPreProcessedIngestView.QueryStructureConfig(
                    raw_table_view_type=RawTableViewType.PARAMETERIZED,
                    param_name_override="my_param",
                )
            ),
        )

    def test_direct_ingest_preprocessed_view_same_table_multiple_places(self) -> None:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions_module,
        )

        view_query_template = """SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_first}
USING (col1);"""

        view = DirectIngestPreProcessedIngestView(
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_raw_table_config=region_config,
            order_by_cols="col1, col2",
            is_detect_row_deletion_view=False,
            primary_key_tables_for_entity_deletion=[],
        )

        self.assertEqual(
            ["file_tag_first"], [c.file_tag for c in view.raw_table_dependency_configs]
        )

        expected_view_query = """WITH
file_tag_first_generated_view AS (
    SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.file_tag_first_latest`
)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN file_tag_first_generated_view
USING (col1)
ORDER BY col1, col2;"""

        self.assertEqual(
            expected_view_query,
            view.expanded_view_query(config=self.DEFAULT_LATEST_CONFIG),
        )

    def test_direct_ingest_preprocessed_view_with_subqueries(self) -> None:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions_module,
        )

        view_query_template = """WITH
foo AS (SELECT * FROM bar)
SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_second}
USING (col1);"""

        view = DirectIngestPreProcessedIngestView(
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_raw_table_config=region_config,
            order_by_cols="col1, col2",
            is_detect_row_deletion_view=False,
            primary_key_tables_for_entity_deletion=[],
        )

        self.assertEqual(
            ["file_tag_first", "file_tag_second"],
            [c.file_tag for c in view.raw_table_dependency_configs],
        )

        expected_view_query = """WITH
file_tag_first_generated_view AS (
    SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.file_tag_first_latest`
),
file_tag_second_generated_view AS (
    SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.file_tag_second_latest`
),
foo AS (SELECT * FROM bar)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN file_tag_second_generated_view
USING (col1)
ORDER BY col1, col2;"""

        self.assertEqual(
            expected_view_query,
            view.expanded_view_query(config=self.DEFAULT_LATEST_CONFIG),
        )

        expected_parameterized_view_query = """WITH
file_tag_first_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_first`
        WHERE
            update_datetime <= @update_timestamp
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_1a, col_name_1b,
            ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
),
file_tag_second_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_second`
        WHERE
            update_datetime <= @update_timestamp
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_2a,
            ROW_NUMBER() OVER (PARTITION BY col_name_2a
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
),
foo AS (SELECT * FROM bar)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN file_tag_second_generated_view
USING (col1)
ORDER BY col1, col2;"""

        self.assertEqual(
            expected_parameterized_view_query,
            view.expanded_view_query(config=self.DEFAULT_PARAMETERIZED_CONFIG),
        )

        # Also check that appending whitespace before the WITH prefix produces the same results
        view_query_template = "\n " + view_query_template

        view = DirectIngestPreProcessedIngestView(
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_raw_table_config=region_config,
            order_by_cols="col1, col2",
            is_detect_row_deletion_view=False,
            primary_key_tables_for_entity_deletion=[],
        )

        self.assertEqual(
            ["file_tag_first", "file_tag_second"],
            [c.file_tag for c in view.raw_table_dependency_configs],
        )

        self.assertEqual(
            expected_view_query,
            view.expanded_view_query(config=self.DEFAULT_LATEST_CONFIG),
        )
        self.assertEqual(
            expected_parameterized_view_query,
            view.expanded_view_query(config=self.DEFAULT_PARAMETERIZED_CONFIG),
        )

    def test_direct_ingest_preprocessed_view_throws_for_unexpected_tag(self) -> None:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions_module,
        )

        view_query_template = """SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_not_in_config}
USING (col1);"""

        with self.assertRaises(ValueError):
            DirectIngestPreProcessedIngestView(
                ingest_view_name="ingest_view_tag",
                view_query_template=view_query_template,
                region_raw_table_config=region_config,
                order_by_cols=None,
                is_detect_row_deletion_view=False,
                primary_key_tables_for_entity_deletion=[],
            )

    def test_direct_ingest_preprocessed_view_materialized_raw_table_subqueries(
        self,
    ) -> None:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions_module,
        )

        view_query_template = """SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_second}
USING (col1);"""

        view = DirectIngestPreProcessedIngestView(
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_raw_table_config=region_config,
            order_by_cols="col1, col2",
            is_detect_row_deletion_view=False,
            primary_key_tables_for_entity_deletion=[],
            materialize_raw_data_table_views=True,
        )

        self.assertEqual(
            ["file_tag_first", "file_tag_second"],
            [c.file_tag for c in view.raw_table_dependency_configs],
        )

        expected_view_query = """CREATE TEMP TABLE file_tag_first_generated_view AS (
    SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.file_tag_first_latest`
);
CREATE TEMP TABLE file_tag_second_generated_view AS (
    SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.file_tag_second_latest`
);
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN file_tag_second_generated_view
USING (col1)
ORDER BY col1, col2;"""

        self.assertEqual(
            expected_view_query,
            view.expanded_view_query(
                config=DirectIngestPreProcessedIngestView.QueryStructureConfig(
                    raw_table_view_type=RawTableViewType.LATEST,
                )
            ),
        )

        expected_parameterized_view_query = """CREATE TEMP TABLE file_tag_first_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_first`
        WHERE
            update_datetime <= @my_update_timestamp_param_name
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_1a, col_name_1b,
            ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
);
CREATE TEMP TABLE file_tag_second_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_second`
        WHERE
            update_datetime <= @my_update_timestamp_param_name
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_2a,
            ROW_NUMBER() OVER (PARTITION BY col_name_2a
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
);
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN file_tag_second_generated_view
USING (col1)
ORDER BY col1, col2;"""

        self.assertEqual(
            expected_parameterized_view_query,
            view.expanded_view_query(
                config=DirectIngestPreProcessedIngestView.QueryStructureConfig(
                    raw_table_view_type=RawTableViewType.PARAMETERIZED,
                    param_name_override="my_update_timestamp_param_name",
                )
            ),
        )

    def test_direct_ingest_preprocessed_view_other_materialized_subquery_fails(
        self,
    ) -> None:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions_module,
        )

        view_query_template = """
CREATE TEMP TABLE my_subquery AS (SELECT * FROM {file_tag_first});
SELECT * FROM my_subquery;"""

        with self.assertRaisesRegex(
            ValueError,
            "^Found CREATE TEMP TABLE clause in this query - ingest views cannot contain CREATE clauses.$",
        ):
            _ = DirectIngestPreProcessedIngestView(
                ingest_view_name="ingest_view_tag",
                view_query_template=view_query_template,
                region_raw_table_config=region_config,
                order_by_cols="col1, col2",
                is_detect_row_deletion_view=False,
                primary_key_tables_for_entity_deletion=[],
            )

    def test_direct_ingest_preprocessed_view_materialized_raw_table_views(self) -> None:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions_module,
        )

        view_query_template = """WITH
foo AS (SELECT * FROM bar)
SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_second}
USING (col1);"""

        view = DirectIngestPreProcessedIngestView(
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_raw_table_config=region_config,
            order_by_cols="col1, col2",
            is_detect_row_deletion_view=False,
            primary_key_tables_for_entity_deletion=[],
            materialize_raw_data_table_views=True,
        )

        self.assertEqual(
            ["file_tag_first", "file_tag_second"],
            [c.file_tag for c in view.raw_table_dependency_configs],
        )

        expected_view_query = """CREATE TEMP TABLE file_tag_first_generated_view AS (
    SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.file_tag_first_latest`
);
CREATE TEMP TABLE file_tag_second_generated_view AS (
    SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.file_tag_second_latest`
);
WITH
foo AS (SELECT * FROM bar)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN file_tag_second_generated_view
USING (col1)
ORDER BY col1, col2;"""

        self.assertEqual(
            expected_view_query,
            view.expanded_view_query(config=self.DEFAULT_LATEST_CONFIG),
        )

        expected_parameterized_view_query = """CREATE TEMP TABLE file_tag_first_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_first`
        WHERE
            update_datetime <= @update_timestamp
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_1a, col_name_1b,
            ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
);
CREATE TEMP TABLE file_tag_second_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_second`
        WHERE
            update_datetime <= @update_timestamp
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_2a,
            ROW_NUMBER() OVER (PARTITION BY col_name_2a
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
);
WITH
foo AS (SELECT * FROM bar)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN file_tag_second_generated_view
USING (col1)
ORDER BY col1, col2;"""

        self.assertEqual(
            expected_parameterized_view_query,
            view.expanded_view_query(config=self.DEFAULT_PARAMETERIZED_CONFIG),
        )

        # Also check that appending whitespace before the WITH prefix produces the same results
        view_query_template = "\n " + view_query_template

        view = DirectIngestPreProcessedIngestView(
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_raw_table_config=region_config,
            order_by_cols="col1, col2",
            is_detect_row_deletion_view=False,
            primary_key_tables_for_entity_deletion=[],
            materialize_raw_data_table_views=True,
        )

        self.assertEqual(
            ["file_tag_first", "file_tag_second"],
            [c.file_tag for c in view.raw_table_dependency_configs],
        )

        self.assertEqual(
            expected_view_query,
            view.expanded_view_query(config=self.DEFAULT_LATEST_CONFIG),
        )
        self.assertEqual(
            expected_parameterized_view_query,
            view.expanded_view_query(config=self.DEFAULT_PARAMETERIZED_CONFIG),
        )

    def test_direct_ingest_preprocessed_view_materialized_raw_table_views_temp_output_table(
        self,
    ) -> None:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions_module,
        )

        view_query_template = """WITH
foo AS (SELECT * FROM bar)
SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_second}
USING (col1);"""

        view = DirectIngestPreProcessedIngestView(
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_raw_table_config=region_config,
            order_by_cols="col1, col2",
            is_detect_row_deletion_view=False,
            primary_key_tables_for_entity_deletion=[],
            materialize_raw_data_table_views=True,
        )

        self.assertEqual(
            ["file_tag_first", "file_tag_second"],
            [c.file_tag for c in view.raw_table_dependency_configs],
        )

        expected_view_query = """CREATE TEMP TABLE file_tag_first_generated_view AS (
    SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.file_tag_first_latest`
);
CREATE TEMP TABLE file_tag_second_generated_view AS (
    SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.file_tag_second_latest`
);
CREATE TEMP TABLE my_destination_table AS (

WITH
foo AS (SELECT * FROM bar)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN file_tag_second_generated_view
USING (col1)
ORDER BY col1, col2

);"""

        latest_config = attr.evolve(
            self.DEFAULT_LATEST_CONFIG,
            destination_table_id="my_destination_table",
            destination_table_type=DestinationTableType.TEMPORARY,
        )
        self.assertEqual(
            expected_view_query, view.expanded_view_query(config=latest_config)
        )

        expected_parameterized_view_query = """CREATE TEMP TABLE file_tag_first_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_first`
        WHERE
            update_datetime <= @update_timestamp
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_1a, col_name_1b,
            ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
);
CREATE TEMP TABLE file_tag_second_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_second`
        WHERE
            update_datetime <= @update_timestamp
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_2a,
            ROW_NUMBER() OVER (PARTITION BY col_name_2a
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
);
CREATE TEMP TABLE my_destination_table AS (

WITH
foo AS (SELECT * FROM bar)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN file_tag_second_generated_view
USING (col1)
ORDER BY col1, col2

);"""

        parametrized_config = attr.evolve(
            self.DEFAULT_PARAMETERIZED_CONFIG,
            destination_table_id="my_destination_table",
            destination_table_type=DestinationTableType.TEMPORARY,
        )
        self.assertEqual(
            expected_parameterized_view_query,
            view.expanded_view_query(config=parametrized_config),
        )

    def test_direct_ingest_preprocessed_view_materialized_raw_table_views_permanent_expiring_output_table(
        self,
    ) -> None:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions_module,
        )

        view_query_template = """WITH
foo AS (SELECT * FROM bar)
SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_second}
USING (col1);"""

        view = DirectIngestPreProcessedIngestView(
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_raw_table_config=region_config,
            order_by_cols="col1, col2",
            is_detect_row_deletion_view=False,
            primary_key_tables_for_entity_deletion=[],
        )

        self.assertEqual(
            ["file_tag_first", "file_tag_second"],
            [c.file_tag for c in view.raw_table_dependency_configs],
        )

        expected_view_query = """DROP TABLE IF EXISTS `recidiviz-456.my_destination_dataset.my_destination_table`;
CREATE TABLE `recidiviz-456.my_destination_dataset.my_destination_table`
OPTIONS(
  -- Data in this table will be deleted after 24 hours
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS (

WITH
file_tag_first_generated_view AS (
    SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.file_tag_first_latest`
),
file_tag_second_generated_view AS (
    SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.file_tag_second_latest`
),
foo AS (SELECT * FROM bar)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN file_tag_second_generated_view
USING (col1)
ORDER BY col1, col2

);"""

        latest_config = attr.evolve(
            self.DEFAULT_LATEST_CONFIG,
            destination_dataset_id="my_destination_dataset",
            destination_table_id="my_destination_table",
            destination_table_type=DestinationTableType.PERMANENT_EXPIRING,
        )
        self.assertEqual(
            expected_view_query, view.expanded_view_query(config=latest_config)
        )

        expected_parameterized_view_query = """DROP TABLE IF EXISTS `recidiviz-456.my_destination_dataset.my_destination_table`;
CREATE TABLE `recidiviz-456.my_destination_dataset.my_destination_table`
OPTIONS(
  -- Data in this table will be deleted after 24 hours
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS (

WITH
file_tag_first_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_first`
        WHERE
            update_datetime <= @update_timestamp
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_1a, col_name_1b,
            ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
),
file_tag_second_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_second`
        WHERE
            update_datetime <= @update_timestamp
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_2a,
            ROW_NUMBER() OVER (PARTITION BY col_name_2a
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
),
foo AS (SELECT * FROM bar)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN file_tag_second_generated_view
USING (col1)
ORDER BY col1, col2

);"""

        parametrized_config = attr.evolve(
            self.DEFAULT_PARAMETERIZED_CONFIG,
            destination_dataset_id="my_destination_dataset",
            destination_table_id="my_destination_table",
            destination_table_type=DestinationTableType.PERMANENT_EXPIRING,
        )
        self.assertEqual(
            expected_parameterized_view_query,
            view.expanded_view_query(config=parametrized_config),
        )

    def test_direct_ingest_preprocessed_view_with_update_datetime(self) -> None:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions_module,
        )

        view_query_template = f"""SELECT * FROM {{file_tag_first}}
        WHERE col1 <= @{UPDATE_DATETIME_PARAM_NAME}"""

        view = DirectIngestPreProcessedIngestView(
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_raw_table_config=region_config,
            order_by_cols="col1, col2",
            is_detect_row_deletion_view=False,
            primary_key_tables_for_entity_deletion=[],
        )

        expected_view_query = """WITH
file_tag_first_generated_view AS (
    SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.file_tag_first_latest`
)
SELECT * FROM file_tag_first_generated_view
        WHERE col1 <= CURRENT_DATE('US/Eastern')
ORDER BY col1, col2;"""

        self.assertEqual(
            expected_view_query,
            view.expanded_view_query(config=self.DEFAULT_LATEST_CONFIG),
        )

        expected_parameterized_view_query = f"""WITH
file_tag_first_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_first`
        WHERE
            update_datetime <= @{UPDATE_DATETIME_PARAM_NAME}
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_1a, col_name_1b,
            ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
)
SELECT * FROM file_tag_first_generated_view
        WHERE col1 <= @{UPDATE_DATETIME_PARAM_NAME}
ORDER BY col1, col2;"""

        self.assertEqual(
            expected_parameterized_view_query,
            view.expanded_view_query(
                config=DirectIngestPreProcessedIngestView.QueryStructureConfig(
                    raw_table_view_type=RawTableViewType.PARAMETERIZED
                )
            ),
        )

    def test_direct_ingest_preprocessed_view_with_current_date(self) -> None:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions_module,
        )

        for current_date_fn in [
            "CURRENT_DATE('US/Eastern')",
            # Split up to avoid the lint check for this function used without a timezone
            "CURRENT_DATE(" + ")",
            "current_date(" + ")",
        ]:

            view_query_template = f"""SELECT * FROM {{file_tag_first}}
            WHERE col1 <= {current_date_fn}"""
            with self.assertRaisesRegex(
                ValueError,
                "Found CURRENT_DATE function in this query - ingest views cannot contain "
                "CURRENT_DATE functions. Consider using @update_timestamp instead.",
            ):
                DirectIngestPreProcessedIngestView(
                    ingest_view_name="ingest_view_tag",
                    view_query_template=view_query_template,
                    region_raw_table_config=region_config,
                    order_by_cols="col1, col2",
                    is_detect_row_deletion_view=False,
                    primary_key_tables_for_entity_deletion=[],
                )

    def test_query_structure_config_destination_table_type_dataset_id_validations(
        self,
    ) -> None:
        has_destination_dataset_types = {DestinationTableType.PERMANENT_EXPIRING}

        # Must have dataset id
        for _ in RawTableViewType:
            for destination_table_type in has_destination_dataset_types:
                with self.assertRaisesRegex(
                    ValueError,
                    r"^Found null destination_dataset_id \[None\] with destination_table_type "
                    rf"\[{destination_table_type.name}\]$",
                ):
                    _ = DirectIngestPreProcessedIngestView.QueryStructureConfig(
                        raw_table_view_type=RawTableViewType.LATEST,
                        destination_table_type=destination_table_type,
                    )

        has_no_destination_dataset_types = set(DestinationTableType).difference(
            has_destination_dataset_types
        )
        # Should not have dataset id
        for _ in RawTableViewType:
            for destination_table_type in has_no_destination_dataset_types:
                with self.assertRaisesRegex(
                    ValueError,
                    r"^Found nonnull destination_dataset_id \[some_dataset\] with destination_table_type "
                    rf"\[{destination_table_type.name}\]$",
                ):
                    _ = DirectIngestPreProcessedIngestView.QueryStructureConfig(
                        raw_table_view_type=RawTableViewType.LATEST,
                        destination_dataset_id="some_dataset",
                        destination_table_type=destination_table_type,
                    )

    def test_query_structure_config_destination_table_type_table_id_validations(
        self,
    ) -> None:
        # Must have table id
        for _ in RawTableViewType:
            with self.assertRaisesRegex(
                ValueError,
                r"^Found null destination_table_id \[None\] with destination_table_type \[PERMANENT_EXPIRING\]$",
            ):
                _ = DirectIngestPreProcessedIngestView.QueryStructureConfig(
                    raw_table_view_type=RawTableViewType.LATEST,
                    destination_table_type=DestinationTableType.PERMANENT_EXPIRING,
                    destination_dataset_id="some_dataset",
                )

            with self.assertRaisesRegex(
                ValueError,
                r"^Found null destination_table_id \[None\] with destination_table_type \[TEMPORARY\]$",
            ):
                _ = DirectIngestPreProcessedIngestView.QueryStructureConfig(
                    raw_table_view_type=RawTableViewType.LATEST,
                    destination_table_type=DestinationTableType.TEMPORARY,
                )

        # Should not have table id
        for _ in RawTableViewType:
            with self.assertRaisesRegex(
                ValueError,
                r"^Found nonnull destination_table_id \[some_table\] with destination_table_type \[NONE\]$",
            ):
                _ = DirectIngestPreProcessedIngestView.QueryStructureConfig(
                    raw_table_view_type=RawTableViewType.LATEST,
                    destination_table_id="some_table",
                    destination_table_type=DestinationTableType.NONE,
                )
