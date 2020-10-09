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

from mock import patch

from recidiviz.ingest.direct.controllers.direct_ingest_big_query_view_types import \
    DirectIngestRawDataTableLatestView, RAW_DATA_LATEST_VIEW_QUERY_TEMPLATE, \
    DirectIngestRawDataTableUpToDateView, RAW_DATA_UP_TO_DATE_VIEW_QUERY_TEMPLATE, DirectIngestPreProcessedIngestView, \
    RAW_DATA_LATEST_HISTORICAL_FILE_VIEW_QUERY_TEMPLATE, RAW_DATA_UP_TO_DATE_HISTORICAL_FILE_VIEW_QUERY_TEMPLATE
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import DirectIngestRawFileConfig, \
    DirectIngestRegionRawFileConfig
from recidiviz.tests.ingest import fixtures


class DirectIngestBigQueryViewTypesTest(unittest.TestCase):
    """Tests for types defined in direct_ingest_big_query_view_types_test.py"""

    PROJECT_ID = 'recidiviz-456'

    def setUp(self) -> None:
        self.metadata_patcher = patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.PROJECT_ID

    def tearDown(self):
        self.metadata_patcher.stop()

    def test_raw_latest_view(self):
        view = DirectIngestRawDataTableLatestView(
            region_code='us_xx',
            raw_file_config=DirectIngestRawFileConfig(
                file_tag='table_name',
                primary_key_cols=['col1', 'col2'],
                datetime_cols=[],
                supplemental_order_by_clause='CAST(seq_num AS INT64)',
                encoding='any-encoding',
                separator='@',
                ignore_quotes=False,
                always_historical_export=False,
            )
        )

        self.assertEqual(self.PROJECT_ID, view.project)
        self.assertEqual('us_xx_raw_data_up_to_date_views', view.dataset_id)
        self.assertEqual('table_name_latest', view.table_id)
        self.assertEqual('table_name_latest', view.view_id)

        expected_view_query = RAW_DATA_LATEST_VIEW_QUERY_TEMPLATE.format(
            project_id=self.PROJECT_ID,
            raw_table_primary_key_str='col1, col2',
            raw_table_dataset_id='us_xx_raw_data',
            raw_table_name='table_name',
            except_clause='EXCEPT (file_id, update_datetime)',
            datetime_cols_clause='',
            supplemental_order_by_clause=', CAST(seq_num AS INT64)'
        )

        self.assertEqual(expected_view_query, view.view_query)
        self.assertEqual('SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.table_name_latest`',
                         view.select_query)

    def test_raw_latest_historical_file_view(self):
        view = DirectIngestRawDataTableLatestView(
            region_code='us_xx',
            raw_file_config=DirectIngestRawFileConfig(
                file_tag='table_name',
                primary_key_cols=['col1', 'col2'],
                datetime_cols=[],
                supplemental_order_by_clause='CAST(seq_num AS INT64)',
                encoding='any-encoding',
                separator='@',
                ignore_quotes=False,
                always_historical_export=True,
            )
        )

        self.assertEqual(self.PROJECT_ID, view.project)
        self.assertEqual('us_xx_raw_data_up_to_date_views', view.dataset_id)
        self.assertEqual('table_name_latest', view.table_id)
        self.assertEqual('table_name_latest', view.view_id)

        expected_view_query = RAW_DATA_LATEST_HISTORICAL_FILE_VIEW_QUERY_TEMPLATE.format(
            project_id=self.PROJECT_ID,
            raw_table_primary_key_str='col1, col2',
            raw_table_dataset_id='us_xx_raw_data',
            raw_table_name='table_name',
            except_clause='EXCEPT (file_id, update_datetime)',
            datetime_cols_clause='',
            supplemental_order_by_clause=', CAST(seq_num AS INT64)'
        )

        self.assertEqual(expected_view_query, view.view_query)
        self.assertEqual('SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.table_name_latest`',
                         view.select_query)

    def test_raw_up_to_date_view(self):
        view = DirectIngestRawDataTableUpToDateView(
            region_code='us_xx',
            raw_file_config=DirectIngestRawFileConfig(
                file_tag='table_name',
                primary_key_cols=['col1'],
                datetime_cols=['col2'],
                supplemental_order_by_clause='',
                encoding='any-encoding',
                separator='@',
                ignore_quotes=False,
                always_historical_export=False,
            )
        )

        self.assertEqual(self.PROJECT_ID, view.project)
        self.assertEqual('us_xx_raw_data_up_to_date_views', view.dataset_id)
        self.assertEqual('table_name_by_update_date', view.table_id)
        self.assertEqual('table_name_by_update_date', view.view_id)

        expected_datetime_cols_clause = """
        COALESCE(
            CAST(SAFE_CAST(col2 AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%y', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%Y', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', col2) AS DATETIME) AS STRING),
            col2
        ) AS col2,"""

        expected_view_query = RAW_DATA_UP_TO_DATE_VIEW_QUERY_TEMPLATE.format(
            project_id=self.PROJECT_ID,
            raw_table_primary_key_str='col1',
            raw_table_dataset_id='us_xx_raw_data',
            raw_table_name='table_name',
            except_clause='EXCEPT (col2, file_id, update_datetime)',
            datetime_cols_clause=expected_datetime_cols_clause,
            supplemental_order_by_clause=''
        )

        self.assertEqual(expected_view_query, view.view_query)
        self.assertEqual('SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.table_name_by_update_date`',
                         view.select_query)

    def test_raw_up_to_date_historical_file_view(self):
        view = DirectIngestRawDataTableUpToDateView(
            region_code='us_xx',
            raw_file_config=DirectIngestRawFileConfig(
                file_tag='table_name',
                primary_key_cols=['col1'],
                datetime_cols=['col2'],
                supplemental_order_by_clause='',
                encoding='any-encoding',
                separator='@',
                ignore_quotes=False,
                always_historical_export=True,
            )
        )

        self.assertEqual(self.PROJECT_ID, view.project)
        self.assertEqual('us_xx_raw_data_up_to_date_views', view.dataset_id)
        self.assertEqual('table_name_by_update_date', view.table_id)
        self.assertEqual('table_name_by_update_date', view.view_id)

        expected_datetime_cols_clause = """
        COALESCE(
            CAST(SAFE_CAST(col2 AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%y', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%Y', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M', col2) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', col2) AS DATETIME) AS STRING),
            col2
        ) AS col2,"""

        expected_view_query = RAW_DATA_UP_TO_DATE_HISTORICAL_FILE_VIEW_QUERY_TEMPLATE.format(
            project_id=self.PROJECT_ID,
            raw_table_primary_key_str='col1',
            raw_table_dataset_id='us_xx_raw_data',
            raw_table_name='table_name',
            except_clause='EXCEPT (col2, file_id, update_datetime)',
            datetime_cols_clause=expected_datetime_cols_clause,
            supplemental_order_by_clause=''
        )

        self.assertEqual(expected_view_query, view.view_query)
        self.assertEqual('SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.table_name_by_update_date`',
                         view.select_query)

    def test_direct_ingest_preprocessed_view(self):
        region_config = DirectIngestRegionRawFileConfig(
            region_code='us_xx',
            yaml_config_file_path=fixtures.as_filepath('us_xx_raw_data_files.yaml'),
        )

        view_query_template = """SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_second}
USING (col1);"""

        view = DirectIngestPreProcessedIngestView(
            ingest_view_name='ingest_view_tag',
            view_query_template=view_query_template,
            region_raw_table_config=region_config,
            order_by_cols='col1, col2',
            is_detect_row_deletion_view=False,
            primary_key_tables_for_entity_deletion=[],
        )

        self.assertEqual(['file_tag_first', 'file_tag_second'],
                         [c.file_tag for c in view.raw_table_dependency_configs])

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

        self.assertEqual(expected_view_query, view.view_query)

        expected_parametrized_view_query = """WITH
file_tag_first_generated_view AS (
    WITH rows_with_recency_rank AS (
        SELECT 
            * EXCEPT (file_id, update_datetime), 
            ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM 
            `recidiviz-456.us_xx_raw_data.file_tag_first`
        WHERE 
            update_datetime <= @my_update_timestamp_param_name
    )

    SELECT * 
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
),
file_tag_second_generated_view AS (
    WITH rows_with_recency_rank AS (
        SELECT 
            * EXCEPT (file_id, update_datetime), 
            ROW_NUMBER() OVER (PARTITION BY col_name_2a
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM 
            `recidiviz-456.us_xx_raw_data.file_tag_second`
        WHERE 
            update_datetime <= @my_update_timestamp_param_name
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

        self.assertEqual(expected_parametrized_view_query,
                         view.date_parametrized_view_query('my_update_timestamp_param_name'))

    def test_direct_ingest_preprocessed_view_detect_row_deletion_no_historical_table(self):
        region_config = DirectIngestRegionRawFileConfig(
            region_code='us_xx',
            yaml_config_file_path=fixtures.as_filepath('us_xx_raw_data_files.yaml'),
        )

        view_query_template = """SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_second}
USING (col1);"""

        with self.assertRaises(ValueError):
            DirectIngestPreProcessedIngestView(
                ingest_view_name='ingest_view_tag',
                view_query_template=view_query_template,
                region_raw_table_config=region_config,
                order_by_cols='col1, col2',
                is_detect_row_deletion_view=True,
                primary_key_tables_for_entity_deletion=['file_tag_second'],
            )

    def test_direct_ingest_preprocessed_view_detect_row_deletion_no_pk_tables_specified(self):
        region_config = DirectIngestRegionRawFileConfig(
            region_code='us_xx',
            yaml_config_file_path=fixtures.as_filepath('us_xx_raw_data_files.yaml'),
        )

        view_query_template = """SELECT * FROM {file_tag_first}
        LEFT OUTER JOIN {tagFullHistoricalExport}
        USING (col1);"""

        with self.assertRaises(ValueError):
            DirectIngestPreProcessedIngestView(
                ingest_view_name='ingest_view_tag',
                view_query_template=view_query_template,
                region_raw_table_config=region_config,
                order_by_cols='col1, col2',
                is_detect_row_deletion_view=True,
                primary_key_tables_for_entity_deletion=[],
            )

    def test_direct_ingest_preprocessed_view_detect_row_deletion_unknown_pk_table_specified(self):
        region_config = DirectIngestRegionRawFileConfig(
            region_code='us_xx',
            yaml_config_file_path=fixtures.as_filepath('us_xx_raw_data_files.yaml'),
        )

        view_query_template = """SELECT * FROM {file_tag_first}
        LEFT OUTER JOIN {tagFullHistoricalExport}
        USING (col1);"""

        with self.assertRaises(ValueError):
            DirectIngestPreProcessedIngestView(
                ingest_view_name='ingest_view_tag',
                view_query_template=view_query_template,
                region_raw_table_config=region_config,
                order_by_cols='col1, col2',
                is_detect_row_deletion_view=True,
                primary_key_tables_for_entity_deletion=['tagFullHistoricalExport', 'unknown'],
            )

    def test_direct_ingest_preprocessed_view_detect_row_deletion(self):
        region_config = DirectIngestRegionRawFileConfig(
            region_code='us_xx',
            yaml_config_file_path=fixtures.as_filepath('us_xx_raw_data_files.yaml'),
        )

        view_query_template = """SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {tagFullHistoricalExport}
USING (col1);"""

        view = DirectIngestPreProcessedIngestView(
            ingest_view_name='ingest_view_tag',
            view_query_template=view_query_template,
            region_raw_table_config=region_config,
            order_by_cols='col1, col2',
            is_detect_row_deletion_view=True,
            primary_key_tables_for_entity_deletion=['tagFullHistoricalExport'],
        )

        self.assertEqual(['file_tag_first', 'tagFullHistoricalExport'],
                         [c.file_tag for c in view.raw_table_dependency_configs])

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

        self.assertEqual(expected_view_query, view.view_query)

        expected_parametrized_view_query = """WITH
file_tag_first_generated_view AS (
    WITH rows_with_recency_rank AS (
        SELECT 
            * EXCEPT (file_id, update_datetime), 
            ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM 
            `recidiviz-456.us_xx_raw_data.file_tag_first`
        WHERE 
            update_datetime <= @my_update_timestamp_param_name
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
    rows_with_recency_rank AS (
        SELECT 
            * EXCEPT (file_id, update_datetime), 
            ROW_NUMBER() OVER (PARTITION BY COL_1
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM 
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE 
            file_id = (SELECT file_id FROM max_file_id)
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

        self.assertEqual(expected_parametrized_view_query,
                         view.date_parametrized_view_query('my_update_timestamp_param_name'))

    def test_direct_ingest_preprocessed_view_with_reference_table(self):
        region_config = DirectIngestRegionRawFileConfig(
            region_code='us_xx',
            yaml_config_file_path=fixtures.as_filepath('us_xx_raw_data_files.yaml'),
        )

        view_query_template = """SELECT * FROM {file_tag_first}
LEFT OUTER JOIN `{{project_id}}.reference_tables.my_table`
USING (col1);"""

        view = DirectIngestPreProcessedIngestView(
            ingest_view_name='ingest_view_tag',
            view_query_template=view_query_template,
            region_raw_table_config=region_config,
            order_by_cols='col1, col2',
            is_detect_row_deletion_view=False,
            primary_key_tables_for_entity_deletion=[],
        )

        self.assertEqual(['file_tag_first'],
                         [c.file_tag for c in view.raw_table_dependency_configs])

        expected_view_query = """WITH
file_tag_first_generated_view AS (
    SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.file_tag_first_latest`
)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN `recidiviz-456.reference_tables.my_table`
USING (col1) 
ORDER BY col1, col2;"""

        self.assertEqual(expected_view_query, view.view_query)

        expected_date_parametrized_view_query = """WITH
file_tag_first_generated_view AS (
    WITH rows_with_recency_rank AS (
        SELECT 
            * EXCEPT (file_id, update_datetime), 
            ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM 
            `recidiviz-456.us_xx_raw_data.file_tag_first`
        WHERE 
            update_datetime <= @my_param
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

        self.assertEqual(expected_date_parametrized_view_query, view.date_parametrized_view_query('my_param'))

    def test_direct_ingest_preprocessed_view_same_table_multiple_places(self):
        region_config = DirectIngestRegionRawFileConfig(
            region_code='us_xx',
            yaml_config_file_path=fixtures.as_filepath('us_xx_raw_data_files.yaml'),
        )

        view_query_template = """SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_first}
USING (col1);"""

        view = DirectIngestPreProcessedIngestView(
            ingest_view_name='ingest_view_tag',
            view_query_template=view_query_template,
            region_raw_table_config=region_config,
            order_by_cols='col1, col2',
            is_detect_row_deletion_view=False,
            primary_key_tables_for_entity_deletion=[],
        )

        self.assertEqual(['file_tag_first'],
                         [c.file_tag for c in view.raw_table_dependency_configs])

        expected_view_query = """WITH
file_tag_first_generated_view AS (
    SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.file_tag_first_latest`
)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN file_tag_first_generated_view
USING (col1) 
ORDER BY col1, col2;"""

        self.assertEqual(expected_view_query, view.view_query)

    def test_direct_ingest_preprocessed_view_with_subqueries(self):
        region_config = DirectIngestRegionRawFileConfig(
            region_code='us_xx',
            yaml_config_file_path=fixtures.as_filepath('us_xx_raw_data_files.yaml'),
        )

        view_query_template = """WITH
foo AS (SELECT * FROM bar)
SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_second}
USING (col1);"""

        view = DirectIngestPreProcessedIngestView(
            ingest_view_name='ingest_view_tag',
            view_query_template=view_query_template,
            region_raw_table_config=region_config,
            order_by_cols='col1, col2',
            is_detect_row_deletion_view=False,
            primary_key_tables_for_entity_deletion=[],
        )

        self.assertEqual(['file_tag_first', 'file_tag_second'],
                         [c.file_tag for c in view.raw_table_dependency_configs])

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

        self.assertEqual(expected_view_query, view.view_query)

        expected_parametrized_view_query = """WITH
file_tag_first_generated_view AS (
    WITH rows_with_recency_rank AS (
        SELECT 
            * EXCEPT (file_id, update_datetime), 
            ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM 
            `recidiviz-456.us_xx_raw_data.file_tag_first`
        WHERE 
            update_datetime <= @update_timestamp
    )

    SELECT * 
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
),
file_tag_second_generated_view AS (
    WITH rows_with_recency_rank AS (
        SELECT 
            * EXCEPT (file_id, update_datetime), 
            ROW_NUMBER() OVER (PARTITION BY col_name_2a
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM 
            `recidiviz-456.us_xx_raw_data.file_tag_second`
        WHERE 
            update_datetime <= @update_timestamp
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

        self.assertEqual(expected_parametrized_view_query, view.date_parametrized_view_query())

        # Also check that appending whitespace before the WITH prefix produces the same results
        view_query_template = '\n ' + view_query_template

        view = DirectIngestPreProcessedIngestView(
            ingest_view_name='ingest_view_tag',
            view_query_template=view_query_template,
            region_raw_table_config=region_config,
            order_by_cols='col1, col2',
            is_detect_row_deletion_view=False,
            primary_key_tables_for_entity_deletion=[],
        )

        self.assertEqual(['file_tag_first', 'file_tag_second'],
                         [c.file_tag for c in view.raw_table_dependency_configs])

        self.assertEqual(expected_view_query, view.view_query)
        self.assertEqual(expected_parametrized_view_query, view.date_parametrized_view_query())

    def test_direct_ingest_preprocessed_view_throws_for_unexpected_tag(self):
        region_config = DirectIngestRegionRawFileConfig(
            region_code='us_xx',
            yaml_config_file_path=fixtures.as_filepath('us_xx_raw_data_files.yaml'),
        )

        view_query_template = """SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_not_in_config}
USING (col1);"""

        with self.assertRaises(ValueError):
            DirectIngestPreProcessedIngestView(
                ingest_view_name='ingest_view_tag',
                view_query_template=view_query_template,
                region_raw_table_config=region_config,
                order_by_cols=None,
                is_detect_row_deletion_view=False,
                primary_key_tables_for_entity_deletion=[],
            )
