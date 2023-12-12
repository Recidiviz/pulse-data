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
"""Helper to run an operation against tables in many datasets."""
import re
import threading
from concurrent import futures
from typing import Callable, Dict, List, Optional

from tqdm import tqdm

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.big_query.big_query_client import BQ_CLIENT_MAX_POOL_SIZE, BigQueryClient
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.types import T


def dataset_prefix_to_filter_regex(dataset_prefix: str) -> str:
    """For the given dataset_prefix, returns a regex that would match any dataset_id
    that starts with that prefix.
    """
    return f"^{dataset_prefix}_.*"


def dataset_id_to_filter_regex(dataset_id: str) -> str:
    """For the given dataset_id, returns a regex that would match only that exact
    dataset_id.
    """
    return f"^{dataset_id}$"


def run_operation_for_tables_in_datasets(
    client: BigQueryClient,
    operation: Callable[[BigQueryClient, ProjectSpecificBigQueryAddress], T],
    prompt: Optional[str],
    dataset_filter: Optional[str] = None,
) -> Dict[ProjectSpecificBigQueryAddress, T]:
    """Helper to run the provided function against all tables.

    If `dataset_filter` is provided, will only include tables that are in datasets
    matched by the filter regex.

    If `prompt` is provided, will prompt for confirmation once all of the datasets are
    identified, appending "for N datasets" to the end of the prompt where N is the
    number of affected datasets.
    """
    print("Fetching all datasets...")

    dataset_filter_pattern = re.compile(dataset_filter) if dataset_filter else None
    datasets = [
        dataset_item
        for dataset_item in client.list_datasets()
        if (
            dataset_filter_pattern is None
            or re.match(dataset_filter_pattern, dataset_item.dataset_id)
        )
    ]

    if prompt:
        prompt_for_confirmation(f"{prompt} for {len(datasets)} datasets")

    results = {}
    with futures.ThreadPoolExecutor(
        # Conservatively allow only half as many workers as allowed connections.
        # Lower this number if we see "urllib3.connectionpool:Connection pool is
        # full, discarding connection" errors.
        max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
    ) as executor:
        overall_progress = tqdm(desc="Updated datasets", total=len(datasets))
        dataset_tables_futures = {
            executor.submit(
                client.list_tables, dataset_id=dataset_item.dataset_id
            ): dataset_item
            for dataset_item in datasets
        }

        def _run_operation(
            table_address: ProjectSpecificBigQueryAddress,
            dataset_progress: tqdm,
            dataset_progress_lock: threading.Lock,
        ) -> T:
            result = operation(client, table_address)

            # Prevent two threads from checking `n` when it has the same value.
            with dataset_progress_lock:
                dataset_progress.update()
                dataset_completed = dataset_progress.n == dataset_progress.total

            if dataset_completed:
                dataset_progress.close()
                overall_progress.update()
            return result

        operation_futures = {}
        for tables_future in futures.as_completed(dataset_tables_futures):
            table_items = list(tables_future.result())
            dataset_item = dataset_tables_futures[tables_future]

            if not table_items:
                overall_progress.update()
                continue

            dataset_progress = tqdm(
                desc=dataset_item.dataset_id,
                total=len(table_items),
                leave=False,
            )
            dataset_progress_lock = threading.Lock()

            for table_item in table_items:
                table_address = ProjectSpecificBigQueryAddress.from_list_item(
                    table_item
                )
                operation_futures[
                    executor.submit(
                        _run_operation,
                        table_address=table_address,
                        dataset_progress=dataset_progress,
                        dataset_progress_lock=dataset_progress_lock,
                    )
                ] = table_address

        for f in futures.as_completed(operation_futures):
            table_address = operation_futures[f]
            results[table_address] = f.result()

        overall_progress.close()

    return results


def run_operation_for_given_tables(
    client: BigQueryClient,
    operation: Callable[[BigQueryClient, ProjectSpecificBigQueryAddress], T],
    tables: List[ProjectSpecificBigQueryAddress],
) -> Dict[ProjectSpecificBigQueryAddress, T]:
    """
    Helper to run the provided function against given tables.
    """
    print(f"Updating {len(tables)} tables...")
    results = {}
    with futures.ThreadPoolExecutor(
        # Conservatively allow only half as many workers as allowed connections.
        # Lower this number if we see "urllib3.connectionpool:Connection pool is
        # full, discarding connection" errors.
        max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
    ) as executor:
        overall_progress = tqdm(desc="Updated tables", total=len(tables))

        def _run_operation(
            specific_table_address: ProjectSpecificBigQueryAddress,
        ) -> T:
            result = operation(client, specific_table_address)
            overall_progress.update()
            return result

        operation_futures = {}

        for table_address in tables:
            operation_futures[
                executor.submit(
                    _run_operation,
                    specific_table_address=table_address,
                )
            ] = table_address

        for f in futures.as_completed(operation_futures):
            table_address = operation_futures[f]
            results[table_address] = f.result()

        overall_progress.close()

    return results
