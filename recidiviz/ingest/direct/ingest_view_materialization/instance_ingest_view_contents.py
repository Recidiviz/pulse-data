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
"""Provides an interface for I/O to the ingest view results tables for a given
region's ingest instance.
"""
import abc
import datetime
import pprint
import uuid
from typing import Any, Dict, Iterable, Optional, Union

import attr
from google.cloud import bigquery
from more_itertools import one, peekable

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_results_contents_handle import (
    BigQueryResultsContentsHandle,
)
from recidiviz.big_query.view_update_manager import (
    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils import metadata
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import assert_type

_UPPER_BOUND_DATETIME_COL_NAME = "__upper_bound_datetime_inclusive"
_LOWER_BOUND_DATETIME_COL_NAME = "__lower_bound_datetime_exclusive"
_MATERIALIZATION_TIME_COL_NAME = "__materialization_time"
_PROCESSED_TIME_COL_NAME = "__processed_time"
_BATCH_NUMBER_COL_NAME = "__extract_and_merge_batch"

_ALL_METADATA_COLS_STR = ",\n  ".join(
    [
        _UPPER_BOUND_DATETIME_COL_NAME,
        _LOWER_BOUND_DATETIME_COL_NAME,
        _MATERIALIZATION_TIME_COL_NAME,
        _PROCESSED_TIME_COL_NAME,
        _BATCH_NUMBER_COL_NAME,
    ]
)

_INGEST_VIEW_RESULTS_BATCH_QUERY_TEMPLATE = f"""SELECT * EXCEPT(
  {_ALL_METADATA_COLS_STR}
)
FROM `{{project_id}}.{{results_dataset}}.{{results_table}}`
WHERE
  {_UPPER_BOUND_DATETIME_COL_NAME} = {{upper_bound_datetime_inclusive}}
  AND {_BATCH_NUMBER_COL_NAME} = {{batch_number}}
  AND {_PROCESSED_TIME_COL_NAME} IS NULL;"""


_MARK_PROCESSED_QUERY_TEMPLATE = f"""
UPDATE `{{project_id}}.{{results_dataset}}.{{results_table}}`
SET {_PROCESSED_TIME_COL_NAME} = {{processed_time}}
WHERE
  {_UPPER_BOUND_DATETIME_COL_NAME} = {{upper_bound_datetime_inclusive}}
  AND {_BATCH_NUMBER_COL_NAME} = {{batch_number}}
  AND {_PROCESSED_TIME_COL_NAME} IS NULL;
"""


_ADD_METADATA_COLS_TO_RESULTS_QUERY_TEMPLATE = f"""
SELECT 
    *, 
    {{upper_bound_datetime_inclusive}} AS {_UPPER_BOUND_DATETIME_COL_NAME},
    {{lower_bound_datetime_exclusive}} AS {_LOWER_BOUND_DATETIME_COL_NAME},
    CURRENT_DATETIME('UTC') AS {_MATERIALIZATION_TIME_COL_NAME},
    CAST(NULL AS DATETIME) AS {_PROCESSED_TIME_COL_NAME},
    CAST(
      FLOOR(
        (
          ROW_NUMBER() OVER (ORDER BY {{order_by_cols}}) - 1
        ) / {{batch_size}}
      ) AS INT64
    ) AS {_BATCH_NUMBER_COL_NAME}
FROM
    `{{project_id}}.{{temp_results_dataset}}.{{temp_results_table}}`;
"""


HIGHEST_PRIORITY_ROW_FOR_VIEW_TEMPLATE = f"""
SELECT *
FROM (
  SELECT
    '{{ingest_view_name}}' AS ingest_view_name,
    {_UPPER_BOUND_DATETIME_COL_NAME},
    {_BATCH_NUMBER_COL_NAME},
    ROW_NUMBER() OVER (
      ORDER BY {_UPPER_BOUND_DATETIME_COL_NAME}, {_BATCH_NUMBER_COL_NAME}
    ) AS priority
  FROM
    `{{project_id}}.{{results_dataset}}.{{results_table}}`
  WHERE
    {_PROCESSED_TIME_COL_NAME} IS NULL
)
WHERE priority = 1
"""

SUMMARY_FOR_VIEW_TEMPLATE = f"""
SELECT
  COUNTIF({_PROCESSED_TIME_COL_NAME} IS NULL) AS num_unprocessed_rows,
  MIN(
    IF({_PROCESSED_TIME_COL_NAME} IS NULL, {_UPPER_BOUND_DATETIME_COL_NAME}, NULL)
  ) AS unprocessed_rows_min_datetime,
  COUNTIF({_PROCESSED_TIME_COL_NAME} IS NOT NULL) AS num_processed_rows,
  MAX(
    IF({_PROCESSED_TIME_COL_NAME} IS NOT NULL, {_UPPER_BOUND_DATETIME_COL_NAME}, NULL)
  ) AS processed_rows_max_datetime
FROM
  `{{project_id}}.{{results_dataset}}.{{results_table}}`
"""

MAX_DATE_OF_DATA_PROCESSED_FOR_SCHEMA_TEMPLATE = f"""
SELECT "{{ingest_view_name}}" AS ingest_view_name, MAX({_UPPER_BOUND_DATETIME_COL_NAME}) as {_UPPER_BOUND_DATETIME_COL_NAME}
FROM `{{project_id}}.{{results_dataset}}.{{results_table}}`
WHERE {_PROCESSED_TIME_COL_NAME} IS NOT NULL AND {_PROCESSED_TIME_COL_NAME} < DATETIME("{{datetime_utc}}")
"""

MIN_DATE_OF_UNPROCESSED_DATA_FOR_SCHEMA_TEMPLATE = f"""
SELECT "{{ingest_view_name}}" AS ingest_view_name, MIN({_UPPER_BOUND_DATETIME_COL_NAME}) as {_UPPER_BOUND_DATETIME_COL_NAME}
FROM `{{project_id}}.{{results_dataset}}.{{results_table}}`
WHERE {_PROCESSED_TIME_COL_NAME} IS NULL
"""

_EXTRACT_AND_MERGE_DEFAULT_BATCH_SIZE = 2500


# TODO(#8905): This function makes the contents we read from ingest view results tables
#  backwards compatible with the content we used to read from ingest view files. It is
#  possible that this will not be necessary once all views have been migrated to v2
#  mappings and it's worth revisiting at that time.
def to_string_value_converter(
    field_name: str,
    value: Any,
) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (bool, int)):
        return str(value)
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()

    raise ValueError(
        f"Unexpected value type [{type(value)}] for field [{field_name}]: {value}"
    )


@attr.define(frozen=True, kw_only=True)
class ResultsBatchInfo:
    ingest_view_name: str
    upper_bound_datetime_inclusive: datetime.datetime
    batch_number: int


@attr.define(frozen=True, kw_only=True)
class IngestViewContentsSummary:
    ingest_view_name: str
    num_unprocessed_rows: int
    unprocessed_rows_min_datetime: Optional[datetime.datetime]
    num_processed_rows: int
    processed_rows_max_datetime: Optional[datetime.datetime]

    def __str__(self) -> str:
        return pprint.pformat(
            {
                field_name: getattr(self, field_name)
                for field_name in attr.fields_dict(self.__class__)
            },
            indent=2,
        )

    def as_api_dict(self) -> Dict[str, Union[Optional[str], int]]:
        """Serializes this class into a dictionary that can be transmitted via an API
        to the frontend.
        """
        return {
            "ingestViewName": self.ingest_view_name,
            "numUnprocessedRows": self.num_unprocessed_rows,
            "unprocessedRowsMinDatetime": (
                self.unprocessed_rows_min_datetime.isoformat()
                if self.unprocessed_rows_min_datetime
                else None
            ),
            "numProcessedRows": self.num_processed_rows,
            "processedRowsMaxDatetime": (
                self.processed_rows_max_datetime.isoformat()
                if self.processed_rows_max_datetime
                else None
            ),
        }


class InstanceIngestViewContents:
    """Provides an interface for I/O to the ingest view results tables for a given
    region's ingest instance.
    """

    @property
    @abc.abstractmethod
    def ingest_instance(self) -> DirectIngestInstance:
        """Instance the results belong to."""

    @abc.abstractmethod
    def results_dataset(self) -> str:
        """Returns the dataset of the results tables for this ingest instance."""

    @property
    @abc.abstractmethod
    def temp_results_dataset(self) -> str:
        """Returns the dataset where results are staged before they are copied to their
        final destination (returned by results_dataset())."""

    @abc.abstractmethod
    def save_query_results(
        self,
        *,
        ingest_view_name: str,
        upper_bound_datetime_inclusive: datetime.datetime,
        lower_bound_datetime_exclusive: Optional[datetime.datetime],
        query_str: str,
        order_by_cols_str: str,
        batch_size: int = _EXTRACT_AND_MERGE_DEFAULT_BATCH_SIZE,
    ) -> None:
        """Runs the provided ingest view query and saves the results to the appropriate
        BigQuery table, augmenting with relevant metadata. Will create the ingest view
        results dataset if it does not yet exist.
        """

    @abc.abstractmethod
    def get_unprocessed_rows_for_batch(
        self,
        *,
        ingest_view_name: str,
        upper_bound_datetime_inclusive: datetime.datetime,
        batch_number: int,
    ) -> BigQueryResultsContentsHandle:
        """Returns all ingest view result rows in a given back that have not yet been
        processed via the extract and merge step of ingest (i.e. committed to Postgres).
        """

    @abc.abstractmethod
    def get_next_unprocessed_batch_info_by_view(
        self,
    ) -> Dict[str, Optional[ResultsBatchInfo]]:
        """Returns info about the next unprocessed batch of ingest view results for the
        each ingest ingest view with generated results. This first looks for batches
        with the earliest file date, then chooses the lowest batch number among batches
        with the same date. For any given view, if there are result rows but all have
        been processed, then that view will have an entry in the result, but the value
        will be None.
        """

    @abc.abstractmethod
    def mark_rows_as_processed(
        self,
        *,
        ingest_view_name: str,
        upper_bound_datetime_inclusive: datetime.datetime,
        batch_number: int,
    ) -> None:
        """Marks all ingest view results rows in a given batch as processed."""

    @abc.abstractmethod
    def get_max_date_of_data_processed_before_datetime(
        self, datetime_utc: datetime.datetime
    ) -> Dict[str, Optional[datetime.datetime]]:
        """Returns a result with one row per ingest view and the max date result for that view."""

    @abc.abstractmethod
    def get_min_date_of_unprocessed_data(
        self,
    ) -> Dict[str, Optional[datetime.datetime]]:
        """Returns a dictionary with one entry per ingest view with the min date on any unprocessed results row."""


class InstanceIngestViewContentsImpl(InstanceIngestViewContents):
    """Production implementation of InstanceIngestViewContents."""

    _ADDITIONAL_METADATA_SCHEMA_FIELDS = [
        bigquery.SchemaField(
            _UPPER_BOUND_DATETIME_COL_NAME,
            field_type=bigquery.enums.SqlTypeNames.DATETIME.value,
            mode="REQUIRED",
        ),
        bigquery.SchemaField(
            _LOWER_BOUND_DATETIME_COL_NAME,
            field_type=bigquery.enums.SqlTypeNames.DATETIME.value,
            mode="NULLABLE",
        ),
        bigquery.SchemaField(
            _MATERIALIZATION_TIME_COL_NAME,
            field_type=bigquery.enums.SqlTypeNames.DATETIME.value,
            mode="REQUIRED",
        ),
        bigquery.SchemaField(
            _PROCESSED_TIME_COL_NAME,
            field_type=bigquery.enums.SqlTypeNames.DATETIME.value,
            mode="NULLABLE",
        ),
        bigquery.SchemaField(
            _BATCH_NUMBER_COL_NAME,
            field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
            mode="REQUIRED",
        ),
    ]

    def __init__(
        self,
        big_query_client: BigQueryClient,
        region_code: str,
        ingest_instance: DirectIngestInstance,
        dataset_prefix: Optional[str],
    ):
        self._big_query_client = big_query_client
        self._region_code_lower = region_code.lower()
        self._ingest_instance = ingest_instance
        self._dataset_prefix = dataset_prefix
        self._temp_results_dataset = self._build_temp_results_dataset()

    @property
    def ingest_instance(self) -> DirectIngestInstance:
        return self._ingest_instance

    def results_dataset(self) -> str:
        """Returns the dataset of the results tables for this ingest instance."""
        dataset_prefix = f"{self._dataset_prefix}_" if self._dataset_prefix else ""
        return (
            f"{dataset_prefix}{self._region_code_lower}_ingest_view_results_"
            f"{self.ingest_instance.value.lower()}"
        )

    @property
    def temp_results_dataset(self) -> str:
        return self._temp_results_dataset

    def _build_temp_results_dataset(self) -> str:
        date_ts = datetime.datetime.utcnow().strftime("%Y%m%d")
        return f"{self.results_dataset()}_temp_{date_ts}"

    def _datetime_clause(self, dt: datetime.datetime) -> str:
        """Returns a datetime formatted as a BigQuery DATETIME() function."""
        return f"DATETIME({dt.year}, {dt.month}, {dt.day}, {dt.hour}, {dt.minute}, {dt.second})"

    def save_query_results(
        self,
        *,
        ingest_view_name: str,
        upper_bound_datetime_inclusive: datetime.datetime,
        lower_bound_datetime_exclusive: Optional[datetime.datetime],
        query_str: str,
        order_by_cols_str: str,
        batch_size: int = _EXTRACT_AND_MERGE_DEFAULT_BATCH_SIZE,
    ) -> None:
        # First, load results into a temporary table
        intermediate_table_address = BigQueryAddress(
            dataset_id=self.temp_results_dataset,
            table_id=f"{ingest_view_name}_{str(uuid.uuid4())}",
        )
        self._big_query_client.create_dataset_if_necessary(
            self._big_query_client.dataset_ref_for_id(
                intermediate_table_address.dataset_id
            ),
            default_table_expiration_ms=TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
        )
        query_job = self._big_query_client.insert_into_table_from_query_async(
            destination_dataset_id=intermediate_table_address.dataset_id,
            destination_table_id=intermediate_table_address.table_id,
            query=query_str,
            allow_field_additions=False,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        query_job.result()

        # Use those results to create a final destination table with the same schema,
        # but with a few metadata columns added on.
        final_address = self._ingest_view_results_address(ingest_view_name)
        self._create_ingest_view_table_if_necessary(
            table_address=final_address,
            intermediate_table_address=intermediate_table_address,
        )

        # Insert the results into the final destination table, augmenting with metadata.
        final_query = StrictStringFormatter().format(
            _ADD_METADATA_COLS_TO_RESULTS_QUERY_TEMPLATE,
            project_id=metadata.project_id(),
            temp_results_dataset=intermediate_table_address.dataset_id,
            temp_results_table=intermediate_table_address.table_id,
            upper_bound_datetime_inclusive=self._datetime_clause(
                upper_bound_datetime_inclusive
            ),
            lower_bound_datetime_exclusive=(
                self._datetime_clause(lower_bound_datetime_exclusive)
                if lower_bound_datetime_exclusive
                else "CAST(NULL AS DATETIME)"
            ),
            batch_size=batch_size,
            order_by_cols=order_by_cols_str,
        )

        query_job = self._big_query_client.insert_into_table_from_query_async(
            destination_dataset_id=final_address.dataset_id,
            destination_table_id=final_address.table_id,
            query=final_query,
            allow_field_additions=False,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        query_job.result()

    def _ingest_view_results_address(self, ingest_view_name: str) -> BigQueryAddress:
        """Returns the address of the results table for this ingest view."""
        return BigQueryAddress(
            dataset_id=self.results_dataset(),
            table_id=ingest_view_name,
        )

    def _create_ingest_view_table_if_necessary(
        self,
        table_address: BigQueryAddress,
        intermediate_table_address: BigQueryAddress,
    ) -> None:
        self._big_query_client.create_dataset_if_necessary(
            self._big_query_client.dataset_ref_for_id(table_address.dataset_id),
            default_table_expiration_ms=(
                TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
                if self._dataset_prefix
                else None
            ),
        )

        if self._big_query_client.table_exists(
            self._big_query_client.dataset_ref_for_id(table_address.dataset_id),
            table_address.table_id,
        ):
            return

        intermediate_table = self._big_query_client.get_table(
            self._big_query_client.dataset_ref_for_id(
                intermediate_table_address.dataset_id
            ),
            intermediate_table_address.table_id,
        )
        final_table_schema = (
            intermediate_table.schema.copy() + self._ADDITIONAL_METADATA_SCHEMA_FIELDS
        )
        self._big_query_client.create_table_with_schema(
            table_address.dataset_id,
            table_address.table_id,
            schema_fields=final_table_schema,
            date_partition_field=_UPPER_BOUND_DATETIME_COL_NAME,
        )

    def get_unprocessed_rows_for_batch(
        self,
        *,
        ingest_view_name: str,
        upper_bound_datetime_inclusive: datetime.datetime,
        batch_number: int,
    ) -> BigQueryResultsContentsHandle:
        results_address = self._ingest_view_results_address(ingest_view_name)

        query_job = self._big_query_client.run_query_async(
            query_str=StrictStringFormatter().format(
                _INGEST_VIEW_RESULTS_BATCH_QUERY_TEMPLATE,
                project_id=metadata.project_id(),
                results_dataset=results_address.dataset_id,
                results_table=results_address.table_id,
                upper_bound_datetime_inclusive=self._datetime_clause(
                    upper_bound_datetime_inclusive
                ),
                batch_number=batch_number,
            )
        )

        return BigQueryResultsContentsHandle(
            query_job, value_converter=to_string_value_converter
        )

    def get_next_unprocessed_batch_info_by_view(
        self,
    ) -> Dict[str, Optional[ResultsBatchInfo]]:
        highest_priority_row_for_view_queries = []

        ingest_views_with_results = [
            results_table.table_id
            for results_table in self._big_query_client.list_tables(
                self.results_dataset()
            )
        ]
        if not ingest_views_with_results:
            return {}

        for ingest_view_name in ingest_views_with_results:
            results_address = self._ingest_view_results_address(ingest_view_name)
            highest_priority_row_for_view_query = StrictStringFormatter().format(
                HIGHEST_PRIORITY_ROW_FOR_VIEW_TEMPLATE,
                project_id=metadata.project_id(),
                results_dataset=results_address.dataset_id,
                results_table=results_address.table_id,
                ingest_view_name=ingest_view_name,
            )
            highest_priority_row_for_view_queries.append(
                highest_priority_row_for_view_query
            )
        all_views_query = "\nUNION ALL\n".join(highest_priority_row_for_view_queries)

        query_job = self._big_query_client.run_query_async(query_str=all_views_query)

        result: Dict[str, Optional[ResultsBatchInfo]] = {
            ingest_view_name: None for ingest_view_name in ingest_views_with_results
        }
        row_iterator: Iterable[Dict[str, Any]] = BigQueryResultsContentsHandle(
            query_job
        ).get_contents_iterator()

        for row in row_iterator:
            ingest_view_name = row["ingest_view_name"]
            result[ingest_view_name] = ResultsBatchInfo(
                ingest_view_name=ingest_view_name,
                upper_bound_datetime_inclusive=assert_type(
                    row[_UPPER_BOUND_DATETIME_COL_NAME], datetime.datetime
                ),
                batch_number=assert_type(row[_BATCH_NUMBER_COL_NAME], int),
            )

        return result

    def mark_rows_as_processed(
        self,
        *,
        ingest_view_name: str,
        upper_bound_datetime_inclusive: datetime.datetime,
        batch_number: int,
    ) -> None:
        results_address = self._ingest_view_results_address(ingest_view_name)
        query_str = StrictStringFormatter().format(
            _MARK_PROCESSED_QUERY_TEMPLATE,
            project_id=metadata.project_id(),
            results_dataset=results_address.dataset_id,
            results_table=results_address.table_id,
            upper_bound_datetime_inclusive=self._datetime_clause(
                upper_bound_datetime_inclusive
            ),
            processed_time=self._datetime_clause(datetime.datetime.utcnow()),
            batch_number=batch_number,
        )
        query_job = self._big_query_client.run_query_async(query_str=query_str)
        query_job.result()

    def get_ingest_view_contents_summary(
        self, ingest_view_name: str
    ) -> Optional[IngestViewContentsSummary]:
        """Returns a summary of processed / unprocessed rows for a given ingest view."""
        results_address = self._ingest_view_results_address(ingest_view_name)

        if not self._big_query_client.table_exists(
            self._big_query_client.dataset_ref_for_id(results_address.dataset_id),
            results_address.table_id,
        ):
            return None

        query_job = self._big_query_client.run_query_async(
            query_str=StrictStringFormatter().format(
                SUMMARY_FOR_VIEW_TEMPLATE,
                project_id=metadata.project_id(),
                results_dataset=results_address.dataset_id,
                results_table=results_address.table_id,
            )
        )
        rows: Iterable[Dict[str, Any]] = peekable(
            BigQueryResultsContentsHandle(query_job).get_contents_iterator()
        )
        row = one(rows)

        return IngestViewContentsSummary(
            ingest_view_name=ingest_view_name,
            num_unprocessed_rows=row["num_unprocessed_rows"],
            unprocessed_rows_min_datetime=row["unprocessed_rows_min_datetime"],
            num_processed_rows=row["num_processed_rows"],
            processed_rows_max_datetime=row["processed_rows_max_datetime"],
        )

    def get_max_date_of_data_processed_before_datetime(
        self, datetime_utc: datetime.datetime
    ) -> Dict[str, Optional[datetime.datetime]]:
        """Returns the most recent date of processed data for a given ingest view before the provided datetime."""
        max_date_of_data_processed_before_datetime_for_view_queries = []

        ingest_views_with_results = [
            results_table.table_id
            for results_table in self._big_query_client.list_tables(
                self.results_dataset()
            )
        ]
        if not ingest_views_with_results:
            return {}

        for ingest_view_name in ingest_views_with_results:
            results_address = self._ingest_view_results_address(ingest_view_name)
            max_date_of_data_processed_before_datetime_for_view_queries.append(
                StrictStringFormatter().format(
                    MAX_DATE_OF_DATA_PROCESSED_FOR_SCHEMA_TEMPLATE,
                    project_id=metadata.project_id(),
                    results_dataset=results_address.dataset_id,
                    results_table=results_address.table_id,
                    datetime_utc=datetime_utc,
                    ingest_view_name=ingest_view_name,
                )
            )

        all_views_query = "\nUNION ALL\n".join(
            max_date_of_data_processed_before_datetime_for_view_queries
        )

        query_job = self._big_query_client.run_query_async(query_str=all_views_query)

        result: Dict[str, Optional[datetime.datetime]] = {
            ingest_view_name: None for ingest_view_name in ingest_views_with_results
        }
        row_iterator: Iterable[Dict[str, Any]] = BigQueryResultsContentsHandle(
            query_job
        ).get_contents_iterator()

        for row in row_iterator:
            ingest_view_name = row["ingest_view_name"]
            result[ingest_view_name] = assert_type(
                row[_UPPER_BOUND_DATETIME_COL_NAME], datetime.datetime
            )

        return result

    def get_min_date_of_unprocessed_data(
        self,
    ) -> Dict[str, Optional[datetime.datetime]]:
        """Returns a dictionary with one entry per ingest view with the max date on any processed results row."""
        min_date_of_unprocessed_data_for_view_queries = []

        ingest_views_with_results = [
            results_table.table_id
            for results_table in self._big_query_client.list_tables(
                self.results_dataset()
            )
        ]
        if not ingest_views_with_results:
            return {}

        for ingest_view_name in ingest_views_with_results:
            results_address = self._ingest_view_results_address(ingest_view_name)
            min_date_of_unprocessed_data_for_view_queries.append(
                StrictStringFormatter().format(
                    MIN_DATE_OF_UNPROCESSED_DATA_FOR_SCHEMA_TEMPLATE,
                    project_id=metadata.project_id(),
                    results_dataset=results_address.dataset_id,
                    results_table=results_address.table_id,
                    ingest_view_name=ingest_view_name,
                )
            )

        all_views_query = "\nUNION ALL\n".join(
            min_date_of_unprocessed_data_for_view_queries
        )

        query_job = self._big_query_client.run_query_async(query_str=all_views_query)

        result: Dict[str, Optional[datetime.datetime]] = {
            ingest_view_name: None for ingest_view_name in ingest_views_with_results
        }
        row_iterator: Iterable[Dict[str, Any]] = BigQueryResultsContentsHandle(
            query_job
        ).get_contents_iterator()

        for row in row_iterator:
            ingest_view_name = row["ingest_view_name"]
            result[ingest_view_name] = row[_UPPER_BOUND_DATETIME_COL_NAME]

        return result


# Run this script if you are making changes to this file. It should produce the
# following output:
# Summary for [my_fake_view]: None
# ... logs from for inserting data
# Summary for [my_fake_view]:
# { 'ingest_view_name': 'my_fake_view',
#   'num_processed_rows': 0,
#   'num_unprocessed_rows': 8005,
#   'processed_rows_max_datetime': None,
#   'unprocessed_rows_min_datetime': datetime.datetime(2021, 7, 1, 0, 0)}
# Found next batch: date=[2021-07-01T00:00:00], batch_num=[0]
# Found [2500] unprocessed rows in batch [0] for date [2021-07-01T00:00:00]
# Marking batch rows as processed
# Found [0] unprocessed rows in batch [0] for date [2021-07-01T00:00:00]
# Summary for [my_fake_view]:
# { 'ingest_view_name': 'my_fake_view',
#   'num_processed_rows': 2500,
#   'num_unprocessed_rows': 5505,
#   'processed_rows_max_datetime': datetime.datetime(2021, 7, 1, 0, 0),
#   'unprocessed_rows_min_datetime': datetime.datetime(2021, 7, 1, 0, 0)}
# Found next batch: date=[2021-07-01T00:00:00], batch_num=[1]
# Found [2500] unprocessed rows in batch [1] for date [2021-07-01T00:00:00]
# Marking batch rows as processed
# Found [0] unprocessed rows in batch [1] for date [2021-07-01T00:00:00]
# Summary for [my_fake_view]:
# { 'ingest_view_name': 'my_fake_view',
#   'num_processed_rows': 5000,
#   'num_unprocessed_rows': 3005,
#   'processed_rows_max_datetime': datetime.datetime(2021, 7, 1, 0, 0),
#   'unprocessed_rows_min_datetime': datetime.datetime(2021, 7, 1, 0, 0)}
# Found next batch: date=[2021-07-01T00:00:00], batch_num=[2]
# Found [5] unprocessed rows in batch [2] for date [2021-07-01T00:00:00]
# Marking batch rows as processed
# Found [0] unprocessed rows in batch [2] for date [2021-07-01T00:00:00]
# Summary for [my_fake_view]:
# { 'ingest_view_name': 'my_fake_view',
#   'num_processed_rows': 5005,
#   'num_unprocessed_rows': 3000,
#   'processed_rows_max_datetime': datetime.datetime(2021, 7, 1, 0, 0),
#   'unprocessed_rows_min_datetime': datetime.datetime(2021, 7, 21, 0, 0)}
# Found next batch: date=[2021-07-21T00:00:00], batch_num=[0]
# Found [2500] unprocessed rows in batch [0] for date [2021-07-21T00:00:00]
# Marking batch rows as processed
# Found [0] unprocessed rows in batch [0] for date [2021-07-21T00:00:00]
# Summary for [my_fake_view]:
# { 'ingest_view_name': 'my_fake_view',
#   'num_processed_rows': 7505,
#   'num_unprocessed_rows': 500,
#   'processed_rows_max_datetime': datetime.datetime(2021, 7, 21, 0, 0),
#   'unprocessed_rows_min_datetime': datetime.datetime(2021, 7, 21, 0, 0)}
# Found next batch: date=[2021-07-21T00:00:00], batch_num=[1]
# Found [500] unprocessed rows in batch [1] for date [2021-07-21T00:00:00]
# Marking batch rows as processed
# Found [0] unprocessed rows in batch [1] for date [2021-07-21T00:00:00]
# Summary for [my_fake_view]:
# { 'ingest_view_name': 'my_fake_view',
#   'num_processed_rows': 8005,
#   'num_unprocessed_rows': 0,
#   'processed_rows_max_datetime': datetime.datetime(2021, 7, 21, 0, 0),
#   'unprocessed_rows_min_datetime': None}

if __name__ == "__main__":
    import logging

    from recidiviz.big_query.big_query_client import BigQueryClientImpl
    from recidiviz.utils.metadata import local_project_id_override

    logging.getLogger().setLevel(logging.INFO)
    with local_project_id_override("recidiviz-staging"):
        big_query_client_ = BigQueryClientImpl()
        contents_ = InstanceIngestViewContentsImpl(
            big_query_client=big_query_client_,
            region_code="us_pa",
            ingest_instance=DirectIngestInstance.PRIMARY,
            dataset_prefix="ageiduschek",
        )

        ingest_view_name_ = "my_fake_view"

        print(
            f"Summary for [{ingest_view_name_}]: {contents_.get_ingest_view_contents_summary(ingest_view_name_)}"
        )

        # Save a batch of results from 2021-07-01T00:00:00
        contents_.save_query_results(
            ingest_view_name=ingest_view_name_,
            upper_bound_datetime_inclusive=datetime.datetime(
                year=2021, month=7, day=1, hour=0, minute=0, second=0
            ),
            lower_bound_datetime_exclusive=None,
            query_str=(
                "SELECT * "
                "FROM `recidiviz-staging.us_pa_raw_data.dbo_BdActionType` "
                "WHERE update_datetime = '2021-07-01T00:00:00' LIMIT 5005;"
            ),
            order_by_cols_str="ParoleNumber",
        )

        # Save a batch of results from 2021-07-21T00:00:00
        contents_.save_query_results(
            ingest_view_name=ingest_view_name_,
            upper_bound_datetime_inclusive=datetime.datetime(
                year=2021, month=7, day=21, hour=0, minute=0, second=0
            ),
            lower_bound_datetime_exclusive=None,
            query_str=(
                "SELECT * "
                "FROM `recidiviz-staging.us_pa_raw_data.dbo_BdActionType` "
                "WHERE update_datetime = '2021-07-21T00:00:00' LIMIT 3000;"
            ),
            order_by_cols_str="ParoleNumber",
        )

        print(
            f"Summary for [{ingest_view_name_}]:\n"
            f"{contents_.get_ingest_view_contents_summary(ingest_view_name_)}"
        )
        while True:
            all_views_next_batch_info = (
                contents_.get_next_unprocessed_batch_info_by_view()
            )
            if ingest_view_name_ not in all_views_next_batch_info:
                raise ValueError(
                    f"Expected to find [{ingest_view_name_} in "
                    f"all_views_next_batch_info: {all_views_next_batch_info}"
                )
            next_batch_info = all_views_next_batch_info[ingest_view_name_]
            if next_batch_info is None:
                break
            print(
                f"Found next batch: "
                f"date=[{next_batch_info.upper_bound_datetime_inclusive.isoformat()}], "
                f"batch_num=[{next_batch_info.batch_number}]"
            )
            handle = contents_.get_unprocessed_rows_for_batch(
                ingest_view_name=ingest_view_name_,
                upper_bound_datetime_inclusive=next_batch_info.upper_bound_datetime_inclusive,
                batch_number=next_batch_info.batch_number,
            )
            batch_results = list(handle.get_contents_iterator())

            print(
                f"Found [{len(batch_results)}] unprocessed rows in batch "
                f"[{next_batch_info.batch_number}] for date "
                f"[{next_batch_info.upper_bound_datetime_inclusive.isoformat()}]"
            )

            print("Marking batch rows as processed")
            contents_.mark_rows_as_processed(
                ingest_view_name=ingest_view_name_,
                upper_bound_datetime_inclusive=next_batch_info.upper_bound_datetime_inclusive,
                batch_number=next_batch_info.batch_number,
            )

            handle = contents_.get_unprocessed_rows_for_batch(
                ingest_view_name=ingest_view_name_,
                upper_bound_datetime_inclusive=next_batch_info.upper_bound_datetime_inclusive,
                batch_number=next_batch_info.batch_number,
            )
            batch_results = list(handle.get_contents_iterator())

            print(
                f"Found [{len(batch_results)}] unprocessed rows in batch "
                f"[{next_batch_info.batch_number}] for date "
                f"[{next_batch_info.upper_bound_datetime_inclusive.isoformat()}]"
            )
            print(
                f"Summary for [{ingest_view_name_}]:\n"
                f"{contents_.get_ingest_view_contents_summary(ingest_view_name_)}"
            )
