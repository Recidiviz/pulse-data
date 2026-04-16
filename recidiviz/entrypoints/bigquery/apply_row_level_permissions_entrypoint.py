# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Entrypoint for applying row level permissions to all tables in our deployed datasets."""
import argparse
import logging
from concurrent import futures

from google.cloud import bigquery, exceptions

from recidiviz.big_query.big_query_address import (
    BigQueryAddress,
    ProjectSpecificBigQueryAddress,
)
from recidiviz.big_query.big_query_client import (
    BQ_CLIENT_MAX_POOL_SIZE,
    BigQueryClientImpl,
)
from recidiviz.big_query.row_access_policy_query_builder import (
    RESTRICTED_ACCESS_STATE_CODE_TO_ACCESS_GROUP,
)
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_source_table_datasets,
)
from recidiviz.utils import metadata
from recidiviz.view_registry.deployed_views import (
    DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED,
)


class ApplyRowLevelPermissionsEntrypoint(EntrypointInterface):
    """Entrypoint for applying row level permissions to all tables in our deployed datasets."""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()

        return parser

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        _apply_row_level_permissions_to_all_tables()


def _table_may_need_row_access_policies(
    table_item: bigquery.table.TableListItem,
) -> bool:
    """Returns True if the table might need row access policies based on
    metadata available from TableListItem (without fetching full schema).

    Tables that return False are guaranteed to not need policies.
    Tables that return True may or may not need policies (schema inspection
    via get_table() is still required for the final determination).
    """
    # Row-level policies can only be applied to regular tables
    if table_item.table_type != "TABLE":
        return False

    address = BigQueryAddress(
        dataset_id=table_item.dataset_id, table_id=table_item.table_id
    )
    state_code = address.state_code_for_address()

    # Tables in non-restricted state-specific datasets never need policies
    if (
        state_code is not None
        and state_code not in RESTRICTED_ACCESS_STATE_CODE_TO_ACCESS_GROUP
    ):
        return False

    return True


def _apply_permissions_for_table(
    client: BigQueryClientImpl, address: ProjectSpecificBigQueryAddress
) -> None:
    table = client.get_table(address=address.to_project_agnostic_address())
    client.apply_row_level_permissions(table)


def _apply_row_level_permissions_to_all_tables() -> None:
    """Applies row level permissions to all tables in our deployed datasets."""
    client = BigQueryClientImpl()

    managed_source_table_datasets = get_source_table_datasets(metadata.project_id())
    managed_view_datasets = DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED
    managed_datasets = managed_source_table_datasets.union(managed_view_datasets)

    with futures.ThreadPoolExecutor(
        # Conservatively allow only half as many workers as allowed connections.
        # Lower this number if we see "urllib3.connectionpool:Connection pool is
        # full, discarding connection" errors.
        max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
    ) as executor:
        dataset_futures = []
        dataset_ids = [
            dataset_item.dataset_id
            for dataset_item in client.list_datasets()
            if dataset_item.dataset_id in managed_datasets
        ]
        for dataset_id in dataset_ids:
            dataset_futures.append(
                executor.submit(client.list_tables, dataset_id=dataset_id)
            )

        table_futures: dict[futures.Future, ProjectSpecificBigQueryAddress] = {}
        for future in futures.as_completed(dataset_futures):
            try:
                table_items = list(future.result())
            except exceptions.NotFound as e:
                # Sometimes a dataset is deleted while we are running this script.
                logging.info("Error getting tables: %s", e)
                continue

            for table_item in table_items:
                if not _table_may_need_row_access_policies(table_item):
                    continue
                table_address = ProjectSpecificBigQueryAddress.from_list_item(
                    table_item
                )
                table_futures[
                    executor.submit(
                        _apply_permissions_for_table,
                        client,
                        table_address,
                    )
                ] = table_address

        failed_tables: list[str] = []
        for f in futures.as_completed(table_futures):
            table_address = table_futures[f]
            try:
                f.result()
            except Exception as e:
                logging.error(
                    "Error applying permissions for %s: %s", table_address.to_str(), e
                )
                failed_tables.append(table_address.to_str())

        if failed_tables:
            raise RuntimeError(
                f"Errors encountered while applying row level permissions for: {failed_tables}"
            )
