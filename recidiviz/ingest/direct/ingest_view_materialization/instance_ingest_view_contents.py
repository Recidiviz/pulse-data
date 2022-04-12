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

import datetime
import uuid
from typing import Any, Optional

from google.cloud import bigquery

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

_UPPER_BOUND_DATETIME_COL_NAME = "__upper_bound_datetime_inclusive"
_LOWER_BOUND_DATETIME_COL_NAME = "__lower_bound_datetime_exclusive"
_MATERIALIZATION_TIME_COL_NAME = "__materialization_time"
_PROCESSED_TIME_COL_NAME = "__processed_time"
_BATCH_NUMBER_COL_NAME = "__extract_and_merge_batch"

_INGEST_VIEW_RESULTS_BATCH_QUERY_TEMPLATE = f"""SELECT *
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

_EXTRACT_AND_MERGE_DEFAULT_BATCH_SIZE = 2500


def to_string_value_converter(
    field_name: str,  # pylint: disable=unused-argument
    value: Any,
) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (str, bool, int)):
        return str(value)
    if isinstance(value, datetime.datetime):
        return value.isoformat()

    raise ValueError(f"Unexpected value type [{type(value)}]: {value}")


class InstanceIngestViewContents:
    """Provides an interface for I/O to the ingest view results tables for a given
    region's ingest instance.
    """

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
        self.big_query_client = big_query_client
        self.region_code_lower = region_code.lower()
        self.ingest_instance = ingest_instance
        self.dataset_prefix = dataset_prefix
        self._temp_results_dataset = self._build_temp_results_dataset()

    def results_dataset(self) -> str:
        """Returns the dataset of the results tables for this ingest instance."""
        dataset_prefix = f"{self.dataset_prefix}_" if self.dataset_prefix else ""
        return (
            f"{dataset_prefix}{self.region_code_lower}_ingest_view_results_"
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
        """Runs the provided ingest view query and saves the results to the appropriate
        BigQuery table, augmenting with relevant metadata. Will create the ingest view
        results dataset if it does not yet exist.
        """

        # First, load results into a temporary table
        intermediate_table_address = BigQueryAddress(
            dataset_id=self.temp_results_dataset,
            table_id=f"{ingest_view_name}_{str(uuid.uuid4())}",
        )
        self.big_query_client.create_dataset_if_necessary(
            self.big_query_client.dataset_ref_for_id(
                intermediate_table_address.dataset_id
            ),
            default_table_expiration_ms=TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
        )
        query_job = self.big_query_client.insert_into_table_from_query_async(
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

        query_job = self.big_query_client.insert_into_table_from_query_async(
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
        """Using the ingest view results stored at |intermediate_table_address|, creates
        a new, empty table whose schema includes all the columns in
        |intermediate_table_address|, plus metadata columns necessary for managing
        the ingest view results. If the ingest view results table exists already, does
        nothing.
        """
        self.big_query_client.create_dataset_if_necessary(
            self.big_query_client.dataset_ref_for_id(table_address.dataset_id),
            default_table_expiration_ms=(
                TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
                if self.dataset_prefix
                else None
            ),
        )

        if self.big_query_client.table_exists(
            self.big_query_client.dataset_ref_for_id(table_address.dataset_id),
            table_address.table_id,
        ):
            return

        intermediate_table = self.big_query_client.get_table(
            self.big_query_client.dataset_ref_for_id(
                intermediate_table_address.dataset_id
            ),
            intermediate_table_address.table_id,
        )
        final_table_schema = (
            intermediate_table.schema.copy() + self._ADDITIONAL_METADATA_SCHEMA_FIELDS
        )
        self.big_query_client.create_table_with_schema(
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
        """Returns all ingest view result rows in a given back that have not yet been
        processed via the extract and merge step of ingest (i.e. committed to Postgres).
        """
        results_address = self._ingest_view_results_address(ingest_view_name)

        query_job = self.big_query_client.run_query_async(
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

    def mark_rows_as_processed(
        self,
        *,
        ingest_view_name: str,
        upper_bound_datetime_inclusive: datetime.datetime,
        batch_number: int,
    ) -> None:
        """Marks all ingest view results rows in a given batch as processed."""
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
        query_job = self.big_query_client.run_query_async(query_str=query_str)
        query_job.result()


# Run this script if you are making changes to this file. It should produce the
# following output:
# Found [2500] unprocessed rows in batch [0]
# Marking batch [0] rows as processed
# Found [0] unprocessed rows in batch [0]
# Found [2500] unprocessed rows in batch [1]
# Marking batch [1] rows as processed
# Found [0] unprocessed rows in batch [1]
# Found [5] unprocessed rows in batch [2]
# Marking batch [2] rows as processed
# Found [0] unprocessed rows in batch [2]
if __name__ == "__main__":
    import logging

    from recidiviz.big_query.big_query_client import BigQueryClientImpl
    from recidiviz.utils.metadata import local_project_id_override

    logging.getLogger().setLevel(logging.INFO)
    with local_project_id_override("recidiviz-staging"):
        big_query_client_ = BigQueryClientImpl()
        contents_ = InstanceIngestViewContents(
            big_query_client=big_query_client_,
            region_code="us_pa",
            ingest_instance=DirectIngestInstance.PRIMARY,
            dataset_prefix="ageiduschek",
        )

        ingest_view_name_ = "my_fake_view"
        upper_bound_datetime_inclusive_ = datetime.datetime(
            year=2021, month=4, day=14, hour=12, minute=31, second=0
        )
        contents_.save_query_results(
            ingest_view_name=ingest_view_name_,
            upper_bound_datetime_inclusive=upper_bound_datetime_inclusive_,
            lower_bound_datetime_exclusive=None,
            query_str=(
                "SELECT * "
                "FROM `recidiviz-staging.us_pa_raw_data.dbo_BdActionType` "
                "WHERE update_datetime = '2021-07-01T00:00:00' LIMIT 5005;"
            ),
            order_by_cols_str="ParoleNumber",
        )

        for batch_number_ in range(3):
            handle = contents_.get_unprocessed_rows_for_batch(
                ingest_view_name=ingest_view_name_,
                upper_bound_datetime_inclusive=upper_bound_datetime_inclusive_,
                batch_number=batch_number_,
            )
            batch_results = list(handle.get_contents_iterator())

            print(
                f"Found [{len(batch_results)}] unprocessed rows in batch [{batch_number_}]"
            )

            print(f"Marking batch [{batch_number_}] rows as processed")
            contents_.mark_rows_as_processed(
                ingest_view_name=ingest_view_name_,
                upper_bound_datetime_inclusive=upper_bound_datetime_inclusive_,
                batch_number=batch_number_,
            )

            handle = contents_.get_unprocessed_rows_for_batch(
                ingest_view_name=ingest_view_name_,
                upper_bound_datetime_inclusive=upper_bound_datetime_inclusive_,
                batch_number=batch_number_,
            )
            batch_results = list(handle.get_contents_iterator())

            print(
                f"Found [{len(batch_results)}] unprocessed rows in batch [{batch_number_}]"
            )
