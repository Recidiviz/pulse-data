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
import threading
from concurrent import futures
from typing import Callable, Optional

from tqdm import tqdm

from recidiviz.big_query.big_query_client import BQ_CLIENT_MAX_POOL_SIZE, BigQueryClient
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation


def run_operation_for_tables(
    client: BigQueryClient,
    prompt: str,
    operation: Callable[[BigQueryClient, str, str], None],
    dataset_prefix: Optional[str] = None,
) -> None:
    """Helper to run the provided function against all tables.

    If `dataset_prefix` is provided, will only include tables that are in datasets
    matched by the prefix.

    Will prompt for confirmation once all of the datasets are identified, appending
    "for N datasets" to the end of the prompt where N is the number of affected datasets.
    """
    print("Fetching all datasets...")
    datasets = [
        dataset_item
        for dataset_item in client.list_datasets()
        if dataset_prefix is None
        or dataset_item.dataset_id.startswith(f"{dataset_prefix}_")
    ]

    prompt_for_confirmation(f"{prompt} for {len(datasets)} datasets")

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
            dataset_id: str,
            table_id: str,
            dataset_progress: tqdm,
            dataset_progress_lock: threading.Lock,
        ) -> None:
            operation(client, dataset_id, table_id)

            # Prevent two threads from checking `n` when it has the same value.
            with dataset_progress_lock:
                dataset_progress.update()
                dataset_completed = dataset_progress.n == dataset_progress.total

            if dataset_completed:
                dataset_progress.close()
                overall_progress.update()

        operation_futures = []
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

            operation_futures.extend(
                [
                    executor.submit(
                        _run_operation,
                        dataset_id=dataset_item.dataset_id,
                        table_id=table_item.table_id,
                        dataset_progress=dataset_progress,
                        dataset_progress_lock=dataset_progress_lock,
                    )
                    for table_item in table_items
                ]
            )

        futures.wait(operation_futures)
        overall_progress.close()
