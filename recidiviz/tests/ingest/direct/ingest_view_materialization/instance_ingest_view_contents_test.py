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
"""Tests for InstanceIngestViewContents."""
import datetime
import unittest
from unittest import mock
from unittest.mock import call, create_autospec, patch

from freezegun import freeze_time
from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.ingest.direct.ingest_view_materialization.instance_ingest_view_contents import (
    IngestViewContentsSummary,
    InstanceIngestViewContentsImpl,
    ResultsBatchInfo,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


class InstanceIngestViewContentsTest(unittest.TestCase):
    """Tests for InstanceIngestViewContents."""

    def setUp(self) -> None:

        self.region_code = "us_xx"
        self.ingest_view_name = "my_ingest_view_name"
        self.ingest_view_name_2 = "some_other_view"

        self.project_id = "recidiviz-456"
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = self.project_id

        self.mock_bq_client = create_autospec(BigQueryClient)

        def fake_dataset_ref_for_id(dataset_id: str) -> bigquery.DatasetReference:
            return bigquery.DatasetReference(self.project_id, dataset_id)

        self.mock_bq_client.dataset_ref_for_id = fake_dataset_ref_for_id

    def tearDown(self) -> None:
        self.project_id_patcher.stop()

    @mock.patch("uuid.uuid4")
    def test_save_results(self, mock_uuid_fn: mock.MagicMock) -> None:
        self.mock_bq_client.table_exists.return_value = False
        mock_uuid = "f4eb1302-0034-437e-a344-8f7fd41d0f3e"
        mock_uuid_fn.return_value = mock_uuid

        mock_table = mock.MagicMock()
        mock_table.schema = [bigquery.SchemaField("fake_schema_field", "STRING")]
        self.mock_bq_client.get_table.return_value = mock_table

        save_results_date = datetime.datetime(2022, 2, 1, 0, 0, 0)
        input_query = "SELECT * FROM `recidiviz-456.my_dataset.foo`;"
        with freeze_time(save_results_date):
            ingest_view_contents = InstanceIngestViewContentsImpl(
                big_query_client=self.mock_bq_client,
                region_code=self.region_code,
                ingest_instance=DirectIngestInstance.PRIMARY,
                dataset_prefix=None,
            )
            ingest_view_contents.save_query_results(
                ingest_view_name=self.ingest_view_name,
                upper_bound_datetime_inclusive=datetime.datetime(2022, 1, 1, 0, 0, 0),
                lower_bound_datetime_exclusive=datetime.datetime(2021, 1, 1, 0, 0, 0),
                query_str=input_query,
                order_by_cols_str="some_col",
            )
        expected_temp_dataset = "us_xx_ingest_view_results_primary_temp_20220201"
        expected_final_dataset = "us_xx_ingest_view_results_primary"
        expected_temp_results_table = f"my_ingest_view_name_{mock_uuid}"
        expected_final_query = f"""
SELECT 
    *, 
    DATETIME(2022, 1, 1, 0, 0, 0) AS __upper_bound_datetime_inclusive,
    DATETIME(2021, 1, 1, 0, 0, 0) AS __lower_bound_datetime_exclusive,
    CURRENT_DATETIME('UTC') AS __materialization_time,
    CAST(NULL AS DATETIME) AS __processed_time,
    CAST(
      FLOOR(
        (
          ROW_NUMBER() OVER (ORDER BY some_col) - 1
        ) / 2500
      ) AS INT64
    ) AS __extract_and_merge_batch
FROM
    `recidiviz-456.{expected_temp_dataset}.{expected_temp_results_table}`;
"""
        expected_final_schema = [
            bigquery.SchemaField("fake_schema_field", "STRING", "NULLABLE"),
            bigquery.SchemaField(
                "__upper_bound_datetime_inclusive", "DATETIME", "REQUIRED"
            ),
            bigquery.SchemaField(
                "__lower_bound_datetime_exclusive",
                "DATETIME",
                "NULLABLE",
            ),
            bigquery.SchemaField("__materialization_time", "DATETIME", "REQUIRED"),
            bigquery.SchemaField("__processed_time", "DATETIME", "NULLABLE"),
            bigquery.SchemaField("__extract_and_merge_batch", "INTEGER", "REQUIRED"),
        ]
        self.assertEqual(
            [
                call.create_dataset_if_necessary(
                    DatasetReference(self.project_id, expected_temp_dataset),
                    default_table_expiration_ms=86400000,
                ),
                call.insert_into_table_from_query_async(
                    destination_dataset_id=expected_temp_dataset,
                    destination_table_id=expected_temp_results_table,
                    query=input_query,
                    allow_field_additions=False,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                ),
                call.insert_into_table_from_query_async().result(),
                call.create_dataset_if_necessary(
                    DatasetReference(self.project_id, expected_final_dataset),
                    default_table_expiration_ms=None,
                ),
                call.table_exists(
                    DatasetReference(self.project_id, expected_final_dataset),
                    self.ingest_view_name,
                ),
                call.get_table(
                    DatasetReference(self.project_id, expected_temp_dataset),
                    expected_temp_results_table,
                ),
                call.create_table_with_schema(
                    expected_final_dataset,
                    self.ingest_view_name,
                    schema_fields=expected_final_schema,
                    date_partition_field="__upper_bound_datetime_inclusive",
                ),
                call.insert_into_table_from_query_async(
                    destination_dataset_id=expected_final_dataset,
                    destination_table_id=self.ingest_view_name,
                    query=expected_final_query,
                    allow_field_additions=False,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                ),
                call.insert_into_table_from_query_async().result(),
            ],
            self.mock_bq_client.mock_calls,
        )

    def test_get_unprocessed_rows_for_batch(self) -> None:
        get_rows_date = datetime.datetime(2022, 2, 1, 0, 0, 0)
        with freeze_time(get_rows_date):
            ingest_view_contents = InstanceIngestViewContentsImpl(
                big_query_client=self.mock_bq_client,
                region_code=self.region_code,
                ingest_instance=DirectIngestInstance.PRIMARY,
                dataset_prefix=None,
            )
            _ = ingest_view_contents.get_unprocessed_rows_for_batch(
                ingest_view_name=self.ingest_view_name,
                upper_bound_datetime_inclusive=datetime.datetime(2022, 1, 1, 0, 0, 0),
                batch_number=2,
            )

        expected_query = """SELECT * EXCEPT(
  __upper_bound_datetime_inclusive,
  __lower_bound_datetime_exclusive,
  __materialization_time,
  __processed_time,
  __extract_and_merge_batch
)
FROM `recidiviz-456.us_xx_ingest_view_results_primary.my_ingest_view_name`
WHERE
  __upper_bound_datetime_inclusive = DATETIME(2022, 1, 1, 0, 0, 0)
  AND __extract_and_merge_batch = 2
  AND __processed_time IS NULL;"""

        self.assertEqual(
            [call.run_query_async(query_str=expected_query)],
            self.mock_bq_client.mock_calls,
        )

    def test_get_next_unprocessed_batch_info_by_view_no_results_tables(self) -> None:
        ingest_view_contents = InstanceIngestViewContentsImpl(
            big_query_client=self.mock_bq_client,
            region_code=self.region_code,
            ingest_instance=DirectIngestInstance.PRIMARY,
            dataset_prefix=None,
        )

        self.mock_bq_client.list_tables.return_value = []

        batch_info = ingest_view_contents.get_next_unprocessed_batch_info_by_view()

        self.assertEqual({}, batch_info)

        self.assertEqual(
            [call.list_tables("us_xx_ingest_view_results_primary")],
            self.mock_bq_client.mock_calls,
        )

    def test_get_next_unprocessed_batch_info_by_view_no_batches_returned(self) -> None:
        ingest_view_contents = InstanceIngestViewContentsImpl(
            big_query_client=self.mock_bq_client,
            region_code=self.region_code,
            ingest_instance=DirectIngestInstance.PRIMARY,
            dataset_prefix=None,
        )

        self.mock_bq_client.list_tables.return_value = [
            bigquery.table.TableListItem(
                {
                    "tableReference": {
                        "projectId": self.project_id,
                        "datasetId": "us_xx_ingest_view_results_primary",
                        "tableId": ingest_view_name,
                    }
                }
            )
            for ingest_view_name in [self.ingest_view_name, self.ingest_view_name_2]
        ]

        self.mock_bq_client.run_query_async.return_value = iter([])

        batch_info = ingest_view_contents.get_next_unprocessed_batch_info_by_view()

        self.assertEqual(
            {"my_ingest_view_name": None, "some_other_view": None}, batch_info
        )

        expected_query = """
SELECT *
FROM (
  SELECT
    'my_ingest_view_name' AS ingest_view_name,
    __upper_bound_datetime_inclusive,
    __extract_and_merge_batch,
    ROW_NUMBER() OVER (
      ORDER BY __upper_bound_datetime_inclusive, __extract_and_merge_batch
    ) AS priority
  FROM
    `recidiviz-456.us_xx_ingest_view_results_primary.my_ingest_view_name`
  WHERE
    __processed_time IS NULL
)
WHERE priority = 1

UNION ALL

SELECT *
FROM (
  SELECT
    'some_other_view' AS ingest_view_name,
    __upper_bound_datetime_inclusive,
    __extract_and_merge_batch,
    ROW_NUMBER() OVER (
      ORDER BY __upper_bound_datetime_inclusive, __extract_and_merge_batch
    ) AS priority
  FROM
    `recidiviz-456.us_xx_ingest_view_results_primary.some_other_view`
  WHERE
    __processed_time IS NULL
)
WHERE priority = 1
"""

        self.assertEqual(
            [
                call.list_tables("us_xx_ingest_view_results_primary"),
                call.run_query_async(query_str=expected_query),
            ],
            self.mock_bq_client.mock_calls,
        )

    def test_get_next_unprocessed_batch_info_by_view(self) -> None:
        ingest_view_contents = InstanceIngestViewContentsImpl(
            big_query_client=self.mock_bq_client,
            region_code=self.region_code,
            ingest_instance=DirectIngestInstance.PRIMARY,
            dataset_prefix=None,
        )

        batch_date_1 = datetime.datetime(2022, 2, 1, 0, 0, 0)
        batch_number_1 = 10

        batch_date_2 = datetime.datetime(2022, 2, 2, 0, 0, 0)
        batch_number_2 = 0

        self.mock_bq_client.list_tables.return_value = [
            bigquery.table.TableListItem(
                {
                    "tableReference": {
                        "projectId": self.project_id,
                        "datasetId": "us_xx_ingest_view_results_primary",
                        "tableId": ingest_view_name,
                    }
                }
            )
            for ingest_view_name in [self.ingest_view_name, self.ingest_view_name_2]
        ]

        self.mock_bq_client.run_query_async.return_value = iter(
            [
                bigquery.table.Row(
                    [self.ingest_view_name, batch_date_1, batch_number_1],
                    {
                        "ingest_view_name": 0,
                        "__upper_bound_datetime_inclusive": 1,
                        "__extract_and_merge_batch": 2,
                    },
                ),
                bigquery.table.Row(
                    [self.ingest_view_name_2, batch_date_2, batch_number_2],
                    {
                        "ingest_view_name": 0,
                        "__upper_bound_datetime_inclusive": 1,
                        "__extract_and_merge_batch": 2,
                    },
                ),
            ]
        )

        batch_info = ingest_view_contents.get_next_unprocessed_batch_info_by_view()

        self.assertEqual(
            {
                self.ingest_view_name: ResultsBatchInfo(
                    ingest_view_name=self.ingest_view_name,
                    upper_bound_datetime_inclusive=batch_date_1,
                    batch_number=batch_number_1,
                ),
                self.ingest_view_name_2: ResultsBatchInfo(
                    ingest_view_name=self.ingest_view_name_2,
                    upper_bound_datetime_inclusive=batch_date_2,
                    batch_number=batch_number_2,
                ),
            },
            batch_info,
        )

        expected_query = """
SELECT *
FROM (
  SELECT
    'my_ingest_view_name' AS ingest_view_name,
    __upper_bound_datetime_inclusive,
    __extract_and_merge_batch,
    ROW_NUMBER() OVER (
      ORDER BY __upper_bound_datetime_inclusive, __extract_and_merge_batch
    ) AS priority
  FROM
    `recidiviz-456.us_xx_ingest_view_results_primary.my_ingest_view_name`
  WHERE
    __processed_time IS NULL
)
WHERE priority = 1

UNION ALL

SELECT *
FROM (
  SELECT
    'some_other_view' AS ingest_view_name,
    __upper_bound_datetime_inclusive,
    __extract_and_merge_batch,
    ROW_NUMBER() OVER (
      ORDER BY __upper_bound_datetime_inclusive, __extract_and_merge_batch
    ) AS priority
  FROM
    `recidiviz-456.us_xx_ingest_view_results_primary.some_other_view`
  WHERE
    __processed_time IS NULL
)
WHERE priority = 1
"""

        self.assertEqual(
            [
                call.list_tables("us_xx_ingest_view_results_primary"),
                call.run_query_async(query_str=expected_query),
            ],
            self.mock_bq_client.mock_calls,
        )

    def test_get_next_unprocessed_batch_info_by_view_one_no_results_returned(
        self,
    ) -> None:
        ingest_view_contents = InstanceIngestViewContentsImpl(
            big_query_client=self.mock_bq_client,
            region_code=self.region_code,
            ingest_instance=DirectIngestInstance.PRIMARY,
            dataset_prefix=None,
        )

        batch_date = datetime.datetime(2022, 2, 1, 0, 0, 0)
        batch_number = 10

        self.mock_bq_client.list_tables.return_value = [
            bigquery.table.TableListItem(
                {
                    "tableReference": {
                        "projectId": self.project_id,
                        "datasetId": "us_xx_ingest_view_results_primary",
                        "tableId": ingest_view_name,
                    }
                }
            )
            for ingest_view_name in [self.ingest_view_name, self.ingest_view_name_2]
        ]

        self.mock_bq_client.run_query_async.return_value = iter(
            [
                bigquery.table.Row(
                    [self.ingest_view_name, batch_date, batch_number],
                    {
                        "ingest_view_name": 0,
                        "__upper_bound_datetime_inclusive": 1,
                        "__extract_and_merge_batch": 2,
                    },
                )
            ]
        )

        batch_info = ingest_view_contents.get_next_unprocessed_batch_info_by_view()

        self.assertEqual(
            {
                self.ingest_view_name: ResultsBatchInfo(
                    ingest_view_name=self.ingest_view_name,
                    upper_bound_datetime_inclusive=batch_date,
                    batch_number=10,
                ),
                self.ingest_view_name_2: None,
            },
            batch_info,
        )

        expected_query = """
SELECT *
FROM (
  SELECT
    'my_ingest_view_name' AS ingest_view_name,
    __upper_bound_datetime_inclusive,
    __extract_and_merge_batch,
    ROW_NUMBER() OVER (
      ORDER BY __upper_bound_datetime_inclusive, __extract_and_merge_batch
    ) AS priority
  FROM
    `recidiviz-456.us_xx_ingest_view_results_primary.my_ingest_view_name`
  WHERE
    __processed_time IS NULL
)
WHERE priority = 1

UNION ALL

SELECT *
FROM (
  SELECT
    'some_other_view' AS ingest_view_name,
    __upper_bound_datetime_inclusive,
    __extract_and_merge_batch,
    ROW_NUMBER() OVER (
      ORDER BY __upper_bound_datetime_inclusive, __extract_and_merge_batch
    ) AS priority
  FROM
    `recidiviz-456.us_xx_ingest_view_results_primary.some_other_view`
  WHERE
    __processed_time IS NULL
)
WHERE priority = 1
"""

        self.assertEqual(
            [
                call.list_tables("us_xx_ingest_view_results_primary"),
                call.run_query_async(query_str=expected_query),
            ],
            self.mock_bq_client.mock_calls,
        )

    def test_mark_rows_as_processed(self) -> None:
        mark_processed_date = datetime.datetime(2022, 2, 1, 0, 0, 0)
        with freeze_time(mark_processed_date):
            ingest_view_contents = InstanceIngestViewContentsImpl(
                big_query_client=self.mock_bq_client,
                region_code=self.region_code,
                ingest_instance=DirectIngestInstance.PRIMARY,
                dataset_prefix=None,
            )
            ingest_view_contents.mark_rows_as_processed(
                ingest_view_name=self.ingest_view_name,
                upper_bound_datetime_inclusive=datetime.datetime(2022, 1, 1, 0, 0, 0),
                batch_number=2,
            )

        expected_query = """
UPDATE `recidiviz-456.us_xx_ingest_view_results_primary.my_ingest_view_name`
SET __processed_time = DATETIME(2022, 2, 1, 0, 0, 0)
WHERE
  __upper_bound_datetime_inclusive = DATETIME(2022, 1, 1, 0, 0, 0)
  AND __extract_and_merge_batch = 2
  AND __processed_time IS NULL;
"""

        self.assertEqual(
            [
                call.run_query_async(query_str=expected_query),
                call.run_query_async().result(),
            ],
            self.mock_bq_client.mock_calls,
        )

    def test_get_view_contents_summary_table_does_not_exist(self) -> None:
        ingest_view_contents = InstanceIngestViewContentsImpl(
            big_query_client=self.mock_bq_client,
            region_code=self.region_code,
            ingest_instance=DirectIngestInstance.PRIMARY,
            dataset_prefix=None,
        )
        self.mock_bq_client.table_exists.return_value = False
        self.assertIsNone(
            ingest_view_contents.get_ingest_view_contents_summary(
                ingest_view_name=self.ingest_view_name
            )
        )

        self.mock_bq_client.run_query_async.assert_not_called()

    def test_get_view_contents_summary_table(self) -> None:
        ingest_view_contents = InstanceIngestViewContentsImpl(
            big_query_client=self.mock_bq_client,
            region_code=self.region_code,
            ingest_instance=DirectIngestInstance.PRIMARY,
            dataset_prefix=None,
        )
        self.mock_bq_client.table_exists.return_value = True
        self.mock_bq_client.run_query_async.return_value = iter(
            [
                bigquery.table.Row(
                    [0, None, 10, datetime.datetime(2022, 1, 1, 0, 0, 0, 0)],
                    {
                        "num_processed_rows": 0,
                        "processed_rows_max_datetime": 1,
                        "num_unprocessed_rows": 2,
                        "unprocessed_rows_min_datetime": 3,
                    },
                )
            ]
        )

        self.assertEqual(
            IngestViewContentsSummary(
                ingest_view_name=self.ingest_view_name,
                num_processed_rows=0,
                processed_rows_max_datetime=None,
                unprocessed_rows_min_datetime=datetime.datetime(2022, 1, 1, 0, 0, 0, 0),
                num_unprocessed_rows=10,
            ),
            ingest_view_contents.get_ingest_view_contents_summary(
                ingest_view_name=self.ingest_view_name
            ),
        )

        expected_query = """
SELECT
  COUNTIF(__processed_time IS NULL) AS num_unprocessed_rows,
  MIN(
    IF(__processed_time IS NULL, __upper_bound_datetime_inclusive, NULL)
  ) AS unprocessed_rows_min_datetime,
  COUNTIF(__processed_time IS NOT NULL) AS num_processed_rows,
  MAX(
    IF(__processed_time IS NOT NULL, __upper_bound_datetime_inclusive, NULL)
  ) AS processed_rows_max_datetime
FROM
  `recidiviz-456.us_xx_ingest_view_results_primary.my_ingest_view_name`
"""

        self.assertEqual(
            [
                call.table_exists(mock.ANY, self.ingest_view_name),
                call.run_query_async(query_str=expected_query),
            ],
            self.mock_bq_client.mock_calls,
        )

    def test_get_max_date_of_data_processed_before_datetime(self) -> None:
        datetime_utc = datetime.datetime(2022, 1, 1, 0, 0, 0, 0)
        self.mock_bq_client.list_tables.return_value = iter(
            [
                bigquery.table.TableListItem(
                    {
                        "tableReference": {
                            "projectId": self.project_id,
                            "datasetId": "us_xx_ingest_view_results_primary",
                            "tableId": ingest_view_name,
                        }
                    }
                )
                for ingest_view_name in [self.ingest_view_name, self.ingest_view_name_2]
            ]
        )
        self.mock_bq_client.run_query_async.return_value = iter(
            [
                bigquery.table.Row(
                    [self.ingest_view_name, datetime_utc],
                    {
                        "ingest_view_name": 0,
                        "__upper_bound_datetime_inclusive": 1,
                    },
                ),
                bigquery.table.Row(
                    [self.ingest_view_name_2, datetime_utc],
                    {
                        "ingest_view_name": 0,
                        "__upper_bound_datetime_inclusive": 1,
                    },
                ),
            ]
        )

        ingest_view_contents = InstanceIngestViewContentsImpl(
            big_query_client=self.mock_bq_client,
            region_code=self.region_code,
            ingest_instance=DirectIngestInstance.PRIMARY,
            dataset_prefix=None,
        )
        results = ingest_view_contents.get_max_date_of_data_processed_before_datetime(
            datetime_utc=datetime_utc,
        )

        self.assertEqual(
            {
                ingest_view_name: datetime_utc
                for ingest_view_name in [self.ingest_view_name, self.ingest_view_name_2]
            },
            results,
        )

        expected_query = f"""
SELECT "{self.ingest_view_name}" AS ingest_view_name, MAX(__upper_bound_datetime_inclusive) as __upper_bound_datetime_inclusive
FROM `recidiviz-456.us_xx_ingest_view_results_primary.{self.ingest_view_name}`
WHERE __processed_time IS NOT NULL AND __processed_time < DATETIME("{datetime_utc}")

UNION ALL

SELECT "{self.ingest_view_name_2}" AS ingest_view_name, MAX(__upper_bound_datetime_inclusive) as __upper_bound_datetime_inclusive
FROM `recidiviz-456.us_xx_ingest_view_results_primary.{self.ingest_view_name_2}`
WHERE __processed_time IS NOT NULL AND __processed_time < DATETIME("{datetime_utc}")
"""

        self.assertEqual(
            [
                call.list_tables("us_xx_ingest_view_results_primary"),
                call.run_query_async(query_str=expected_query),
            ],
            self.mock_bq_client.mock_calls,
        )

    def test_get_min_date_of_unprocessed_data(self) -> None:
        datetime_utc = datetime.datetime(2022, 1, 1, 0, 0, 0, 0)
        self.mock_bq_client.list_tables.return_value = iter(
            [
                bigquery.table.TableListItem(
                    {
                        "tableReference": {
                            "projectId": self.project_id,
                            "datasetId": "us_xx_ingest_view_results_primary",
                            "tableId": ingest_view_name,
                        }
                    }
                )
                for ingest_view_name in [self.ingest_view_name, self.ingest_view_name_2]
            ]
        )
        self.mock_bq_client.run_query_async.return_value = iter(
            [
                bigquery.table.Row(
                    [self.ingest_view_name, datetime_utc],
                    {
                        "ingest_view_name": 0,
                        "__upper_bound_datetime_inclusive": 1,
                    },
                ),
                bigquery.table.Row(
                    [self.ingest_view_name_2, datetime_utc],
                    {
                        "ingest_view_name": 0,
                        "__upper_bound_datetime_inclusive": 1,
                    },
                ),
            ]
        )

        ingest_view_contents = InstanceIngestViewContentsImpl(
            big_query_client=self.mock_bq_client,
            region_code=self.region_code,
            ingest_instance=DirectIngestInstance.PRIMARY,
            dataset_prefix=None,
        )
        results = ingest_view_contents.get_min_date_of_unprocessed_data()

        self.assertEqual(
            {
                ingest_view_name: datetime_utc
                for ingest_view_name in [self.ingest_view_name, self.ingest_view_name_2]
            },
            results,
        )

        expected_query = f"""
SELECT "{self.ingest_view_name}" AS ingest_view_name, MIN(__upper_bound_datetime_inclusive) as __upper_bound_datetime_inclusive
FROM `recidiviz-456.us_xx_ingest_view_results_primary.{self.ingest_view_name}`
WHERE __processed_time IS NULL

UNION ALL

SELECT "{self.ingest_view_name_2}" AS ingest_view_name, MIN(__upper_bound_datetime_inclusive) as __upper_bound_datetime_inclusive
FROM `recidiviz-456.us_xx_ingest_view_results_primary.{self.ingest_view_name_2}`
WHERE __processed_time IS NULL
"""

        self.assertEqual(
            [
                call.list_tables("us_xx_ingest_view_results_primary"),
                call.run_query_async(query_str=expected_query),
            ],
            self.mock_bq_client.mock_calls,
        )
