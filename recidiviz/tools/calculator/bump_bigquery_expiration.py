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
"""Tool to extend the expiration of tables in a sandbox to some number of days from now.

Note: This does not change the default expiration for the dataset, so any newly created
tables or views will still get an expiration based on the dataset default (e.g. 1 day).

Example:

python -m recidiviz.tools.calculator.bump_bigquery_expiration \
    --project-id recidiviz-staging \
    --dataset-prefix zbrenda \
    --days 3

"""
import argparse
import datetime
import logging
import threading
from concurrent import futures

from tqdm import tqdm

from recidiviz.big_query.big_query_client import (
    BQ_CLIENT_MAX_POOL_SIZE,
    BigQueryClientImpl,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def bump_expiration_for_dataset_prefix(
    client: BigQueryClientImpl, dataset_prefix: str, expiration: datetime.datetime
) -> None:
    """Bump expirations on all tables and views in matching datasets."""
    print("Fetching all datasets...")
    datasets = [
        dataset_item
        for dataset_item in client.list_datasets()
        if dataset_item.dataset_id.startswith(f"{dataset_prefix}_")
    ]

    prompt_for_confirmation(
        f"Extend the expiration for {len(datasets)} datasets to {expiration.isoformat()}?"
    )

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

        def set_expiration(
            dataset_id: str,
            table_id: str,
            dataset_progress: tqdm,
            dataset_progress_lock: threading.Lock,
        ) -> None:
            client.set_table_expiration(
                dataset_id=dataset_id,
                table_id=table_id,
                expiration=expiration,
            )

            # Prevent two threads from checking `n` when it has the same value.
            with dataset_progress_lock:
                dataset_progress.update()
                dataset_completed = dataset_progress.n == dataset_progress.total

            if dataset_completed:
                dataset_progress.close()
                overall_progress.update()

        set_expiration_futures = []
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

            set_expiration_futures.extend(
                [
                    executor.submit(
                        set_expiration,
                        dataset_id=dataset_item.dataset_id,
                        table_id=table_item.table_id,
                        dataset_progress=dataset_progress,
                        dataset_progress_lock=dataset_progress_lock,
                    )
                    for table_item in table_items
                ]
            )

        futures.wait(set_expiration_futures)
        overall_progress.close()


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project-id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )
    parser.add_argument(
        "--dataset-prefix",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--days",
        type=int,
        required=True,
        help="Number of days from now that the tables should expire.",
    )

    return parser


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = create_parser().parse_args()

    with local_project_id_override(args.project_id):
        bump_expiration_for_dataset_prefix(
            BigQueryClientImpl(),
            dataset_prefix=args.dataset_prefix,
            expiration=datetime.datetime.now() + datetime.timedelta(days=args.days),
        )
