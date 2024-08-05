# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Utilities for flash database checklist """
from typing import Iterator

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    raw_data_pruning_new_raw_data_dataset,
    raw_data_pruning_raw_data_diff_results_dataset,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


def copy_raw_data_between_instances(
    state_code: StateCode,
    ingest_instance_source: DirectIngestInstance,
    ingest_instance_destination: DirectIngestInstance,
    big_query_client: BigQueryClient,
) -> None:
    """Copy raw data to a BQ dataset that is the opposite ingest instance with no expiration date, but does not delete
    the source dataset"""
    source_raw_dataset_id = raw_tables_dataset_for_region(
        state_code=state_code, instance=ingest_instance_source
    )

    destination_raw_dataset_id = raw_tables_dataset_for_region(
        state_code=state_code, instance=ingest_instance_destination
    )

    # Create raw data dataset, if necessary
    big_query_client.create_dataset_if_necessary(dataset_id=destination_raw_dataset_id)

    # Copy raw data over to destination
    big_query_client.copy_dataset_tables(
        source_dataset_id=source_raw_dataset_id,
        destination_dataset_id=destination_raw_dataset_id,
        overwrite_destination_tables=True,
    )


def delete_contents_of_raw_data_tables(
    state_code: StateCode,
    ingest_instance: DirectIngestInstance,
    big_query_client: BigQueryClient,
) -> None:
    """Deletes the contents of each table within the raw data dataset (but does not delete the tables themselves)."""
    raw_dataset_id = raw_tables_dataset_for_region(
        state_code=state_code, instance=ingest_instance
    )
    list_of_tables: Iterator[
        bigquery.table.TableListItem
    ] = big_query_client.list_tables(dataset_id=raw_dataset_id)

    query_jobs = []
    for table in list_of_tables:
        table_address = BigQueryAddress(
            dataset_id=table.dataset_id, table_id=table.table_id
        )
        query_job = big_query_client.delete_from_table_async(table_address)
        query_jobs.append(query_job)
    big_query_client.wait_for_big_query_jobs(query_jobs)


def delete_tables_in_pruning_datasets(
    state_code: StateCode,
    ingest_instance: DirectIngestInstance,
    big_query_client: BigQueryClient,
) -> None:
    """Deletes the tables within the datasets used for raw data pruning."""
    new_raw_data_dataset_id = raw_data_pruning_new_raw_data_dataset(
        state_code=state_code, instance=ingest_instance
    )

    diff_results_dataset_id = raw_data_pruning_raw_data_diff_results_dataset(
        state_code=state_code, instance=ingest_instance
    )

    for dataset in [new_raw_data_dataset_id, diff_results_dataset_id]:
        list_of_tables: Iterator[
            bigquery.table.TableListItem
        ] = big_query_client.list_tables(dataset_id=dataset)

        for table in list_of_tables:
            table_address = BigQueryAddress(
                dataset_id=table.dataset_id, table_id=table.table_id
            )
            big_query_client.delete_table(address=table_address, not_found_ok=True)


def copy_raw_data_to_backup(
    state_code: StateCode,
    ingest_instance: DirectIngestInstance,
    big_query_client: BigQueryClient,
) -> None:
    """Copies raw data for a single ingest instance to a backup BQ dataset"""
    source_raw_dataset_id = raw_tables_dataset_for_region(
        state_code=state_code, instance=ingest_instance
    )

    big_query_client.backup_dataset_tables_if_dataset_exists(
        dataset_id=source_raw_dataset_id
    )
