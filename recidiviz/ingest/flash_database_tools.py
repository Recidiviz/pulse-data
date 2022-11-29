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


from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.view_update_manager import (
    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.ingest_view_materialization.instance_ingest_view_contents import (
    InstanceIngestViewContentsImpl,
)
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


def move_ingest_view_results_to_backup(
    state_code: StateCode,
    ingest_instance: DirectIngestInstance,
    big_query_client: BigQueryClient,
) -> None:
    """Copies ingest view data for a single ingest instance to a backup BQ dataset, deletes source dataset"""

    ingest_view_contents = InstanceIngestViewContentsImpl(
        big_query_client=big_query_client,
        region_code=state_code.value,
        ingest_instance=ingest_instance,
        dataset_prefix=None,
    )
    source_dataset_id = ingest_view_contents.results_dataset()
    backup_dataset_id = big_query_client.add_timestamp_suffix_to_dataset_id(
        dataset_id=source_dataset_id
    )

    backup_dataset_ref = big_query_client.dataset_ref_for_id(
        dataset_id=backup_dataset_id
    )
    big_query_client.create_dataset_if_necessary(
        dataset_ref=backup_dataset_ref,
        default_table_expiration_ms=(
            TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS * 30
        ),  # 30 days
    )

    big_query_client.copy_dataset_tables(
        source_dataset_id=source_dataset_id, destination_dataset_id=backup_dataset_id
    )

    big_query_client.delete_dataset(
        dataset_ref=big_query_client.dataset_ref_for_id(dataset_id=source_dataset_id),
        delete_contents=True,
    )


def move_ingest_view_results_between_instances(
    state_code: StateCode,
    ingest_instance_source: DirectIngestInstance,
    ingest_instance_destination: DirectIngestInstance,
    big_query_client: BigQueryClient,
) -> None:
    """Move ingest view data for a single ingest instance to a BQ dataset that is the opposite ingest instance
    with no expiration date, deletes source dataset"""

    source_ingest_view_contents = InstanceIngestViewContentsImpl(
        big_query_client=big_query_client,
        region_code=state_code.value,
        ingest_instance=ingest_instance_source,
        dataset_prefix=None,
    )
    source_dataset_id = source_ingest_view_contents.results_dataset()

    destination_ingest_view_contents = InstanceIngestViewContentsImpl(
        big_query_client=big_query_client,
        region_code=state_code.value,
        ingest_instance=ingest_instance_destination,
        dataset_prefix=None,
    )
    destination_dataset_id = destination_ingest_view_contents.results_dataset()

    destination_dataset_ref = big_query_client.dataset_ref_for_id(
        dataset_id=destination_dataset_id
    )
    big_query_client.create_dataset_if_necessary(
        dataset_ref=destination_dataset_ref,
    )

    big_query_client.copy_dataset_tables(
        source_dataset_id=source_dataset_id,
        destination_dataset_id=destination_dataset_id,
        overwrite_destination_tables=False,
    )

    big_query_client.delete_dataset(
        dataset_ref=big_query_client.dataset_ref_for_id(dataset_id=source_dataset_id),
        delete_contents=True,
    )


def move_raw_data_between_instances(
    state_code: StateCode,
    ingest_instance_source: DirectIngestInstance,
    ingest_instance_destination: DirectIngestInstance,
    big_query_client: BigQueryClient,
) -> None:
    """Move raw data to a BQ dataset that is the opposite ingest instance with no expiration date, deletes source
    dataset"""
    source_raw_dataset_id = raw_tables_dataset_for_region(
        state_code=state_code, instance=ingest_instance_source
    )

    destination_raw_dataset_id = raw_tables_dataset_for_region(
        state_code=state_code, instance=ingest_instance_destination
    )

    # Create raw data dataset, if necessary
    big_query_client.create_dataset_if_necessary(
        dataset_ref=big_query_client.dataset_ref_for_id(
            dataset_id=destination_raw_dataset_id
        ),
    )

    # Copy raw data over to destination
    big_query_client.copy_dataset_tables(
        source_dataset_id=source_raw_dataset_id,
        destination_dataset_id=destination_raw_dataset_id,
        overwrite_destination_tables=True,
    )

    # Delete original datasets
    big_query_client.delete_dataset(
        dataset_ref=big_query_client.dataset_ref_for_id(
            dataset_id=source_raw_dataset_id
        ),
        delete_contents=True,
    )


def delete_contents_in_raw_data_dataset(
    state_code: StateCode,
    ingest_instance: DirectIngestInstance,
    big_query_client: BigQueryClient,
) -> None:
    """Clears the raw data dataset."""
    raw_dataset_id = raw_tables_dataset_for_region(
        state_code=state_code, instance=ingest_instance
    )
    big_query_client.delete_dataset(
        dataset_ref=big_query_client.dataset_ref_for_id(dataset_id=raw_dataset_id),
        delete_contents=True,
    )


def delete_contents_in_latest_view_dataset(
    state_code: StateCode,
    ingest_instance: DirectIngestInstance,
    big_query_client: BigQueryClient,
) -> None:
    """Clears the raw data latest view dataset."""
    latest_view_dataset_id = raw_latest_views_dataset_for_region(
        state_code=state_code, instance=ingest_instance
    )
    big_query_client.delete_dataset(
        dataset_ref=big_query_client.dataset_ref_for_id(
            dataset_id=latest_view_dataset_id
        ),
        delete_contents=True,
    )


def move_raw_data_to_backup(
    state_code: StateCode,
    ingest_instance: DirectIngestInstance,
    big_query_client: BigQueryClient,
) -> None:
    """Copies raw data for a single ingest instance to a backup BQ dataset, deletes source dataset"""
    source_raw_dataset_id = raw_tables_dataset_for_region(
        state_code=state_code, instance=ingest_instance
    )

    big_query_client.backup_dataset_tables_if_dataset_exists(
        dataset_id=source_raw_dataset_id
    )

    big_query_client.delete_dataset(
        dataset_ref=big_query_client.dataset_ref_for_id(
            dataset_id=source_raw_dataset_id
        ),
        delete_contents=True,
    )
