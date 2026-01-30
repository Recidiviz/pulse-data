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
"""Entrypoint for cleaning up and validating BigQuery datasets"""
import argparse
import datetime
import enum
import logging
from concurrent import futures

from google.api_core.exceptions import NotFound
from google.cloud.bigquery.dataset import DatasetListItem
from tqdm import tqdm

from recidiviz.big_query.big_query_client import (
    BQ_CLIENT_MAX_POOL_SIZE,
    BigQueryClientImpl,
)
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
    get_source_table_datasets,
)
from recidiviz.source_tables.source_table_cleanup_validation import (
    validate_clean_source_table_datasets,
)
from recidiviz.utils import metadata

# Empty datasets must be at least 2 hours old to be deleted
EMPTY_DATASET_DELETION_MIN_SECONDS = 2 * 60 * 60

# Datasets that contain temporary contents we want to clean up must be at least 24 hours old to be deleted
NON_EMPTY_TEMP_DATASET_DELETION_MIN_SECONDS = 24 * 60 * 60

DATASET_MANAGED_BY_TERRAFORM_KEY = "managed_by_terraform"

TEMP_DATASET_PREFIXES_TO_CLEAN_UP = [
    "beam_temp_dataset_",
    "temp_dataset_",
    "bq_read_all_",
]


class DatasetCleanupAndValidationEntrypoint(EntrypointInterface):
    """Entrypoint for cleaning up unused datasets and validating source table datasets"""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        """Parses arguments for the dataset cleanup and validation process."""
        parser = argparse.ArgumentParser()
        parser.add_argument("--dry-run", action="store_true")

        return parser

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        # First, validate source table datasets contain expected tables
        bq_client = BigQueryClientImpl()
        project_id = metadata.project_id()
        source_table_repository = build_source_table_repository_for_collected_schemata(
            project_id=project_id
        )
        validate_clean_source_table_datasets(
            bq_client=bq_client,
            source_table_repository=source_table_repository,
        )

        # Then, clean up unused datasets
        _delete_empty_or_temp_datasets(dry_run=args.dry_run)


class DatasetClassification(enum.Enum):
    DELETION_ELIGIBLE_EMPTY_DATASET = 0
    DELETION_ELIGIBLE_TEMP_DATASET = 1
    TERRAFORM_DATASET = 2
    DELETED_DATASET = 3
    IN_USE_DATASET = 4

    def log_message(self, dry_run: bool, dataset_id: str) -> None:
        message = None
        match self:
            case DatasetClassification.TERRAFORM_DATASET:
                message = "%sSkipping empty dataset that is in Terraform state [%s]"
            case DatasetClassification.DELETION_ELIGIBLE_EMPTY_DATASET:
                message = "%sDataset %s is empty and was not created very recently. Deleting..."
            case DatasetClassification.DELETION_ELIGIBLE_TEMP_DATASET:
                message = "%sDataset %s is a dataset created by Beam and not updated in a while. Deleting..."
            case DatasetClassification.DELETED_DATASET:
                message = "%sSkipping Dataset as it has already been deleted by another process [%s]"
            case DatasetClassification.IN_USE_DATASET:
                message = "%sSkipping Dataset as it is in use [%s]"

        if message:
            logging.info(message, "[DRY RUN]" if dry_run else "", dataset_id)


def _delete_empty_or_temp_datasets(*, dry_run: bool = False) -> None:
    """Deletes all empty datasets in BigQuery."""
    bq_client = BigQueryClientImpl()
    deletion_candidate_datasets = [
        dataset_resource
        for dataset_resource in list(bq_client.list_datasets())
        if dataset_resource.dataset_id
        not in get_source_table_datasets(project_id=metadata.project_id())
    ]
    datasets_to_delete = []

    dataset_classifications: list[tuple[str, DatasetClassification]] = []
    with tqdm(
        desc="Classifying datasets", total=len(deletion_candidate_datasets)
    ) as overall_progress:
        with futures.ThreadPoolExecutor(
            # Conservatively allow only half as many workers as allowed connections.
            # Lower this number if we see "urllib3.connectionpool:Connection pool is
            # full, discarding connection" errors.
            max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
        ) as executor:
            dataset_future_to_dataset_id = {
                executor.submit(
                    _classify_unknown_dataset,
                    dataset_resource=dataset_resource,
                ): dataset_resource.dataset_id
                for dataset_resource in deletion_candidate_datasets
            }

            for f in futures.as_completed(dataset_future_to_dataset_id):
                overall_progress.update()

                dataset_id = dataset_future_to_dataset_id[f]
                dataset_classification = f.result()
                dataset_classifications.append((dataset_id, dataset_classification))

                if dataset_classification in [
                    DatasetClassification.DELETION_ELIGIBLE_EMPTY_DATASET,
                    DatasetClassification.DELETION_ELIGIBLE_TEMP_DATASET,
                ]:
                    datasets_to_delete.append(dataset_future_to_dataset_id[f])

    for dataset_id, dataset_classification in dataset_classifications:
        if dataset_classification != DatasetClassification.IN_USE_DATASET:
            dataset_classification.log_message(dry_run=dry_run, dataset_id=dataset_id)

    if dry_run:
        raise RuntimeError("Dry run, not proceeding with dataset deletion")

    with tqdm(
        desc="Deleting datasets", total=len(datasets_to_delete)
    ) as overall_progress:
        with futures.ThreadPoolExecutor(
            # Conservatively allow only half as many workers as allowed connections.
            # Lower this number if we see "urllib3.connectionpool:Connection pool is
            # full, discarding connection" errors.
            max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
        ) as executor:
            delete_futures = [
                executor.submit(
                    bq_client.delete_dataset,
                    dataset_id=empty_or_temp_dataset,
                    delete_contents=True,
                )
                for empty_or_temp_dataset in datasets_to_delete
            ]
            for delete_future in futures.as_completed(delete_futures):
                overall_progress.update()
                delete_future.result()


def _classify_unknown_dataset(
    dataset_resource: DatasetListItem,
) -> DatasetClassification:
    "Classifies a dataset and whether it is in use"
    bq_client = BigQueryClientImpl()

    try:
        dataset = bq_client.get_dataset(dataset_resource.dataset_id)
    except NotFound:
        return DatasetClassification.DELETED_DATASET

    dataset_labels = dataset.labels
    # Skip datasets that are managed by terraform
    managed_by_terraform = False
    for label, value in dataset_labels.items():
        if label == DATASET_MANAGED_BY_TERRAFORM_KEY and value == "true":
            managed_by_terraform = True

    if managed_by_terraform:
        return DatasetClassification.TERRAFORM_DATASET

    created_time = dataset.created
    dataset_age_seconds = (
        datetime.datetime.now(datetime.timezone.utc) - created_time
    ).total_seconds()

    if (
        any(
            dataset.dataset_id.startswith(prefix)
            for prefix in TEMP_DATASET_PREFIXES_TO_CLEAN_UP
        )
        and dataset_age_seconds > NON_EMPTY_TEMP_DATASET_DELETION_MIN_SECONDS
    ):
        return DatasetClassification.DELETION_ELIGIBLE_TEMP_DATASET

    if (
        bq_client.dataset_is_empty(dataset_resource.dataset_id)
        and dataset_age_seconds > EMPTY_DATASET_DELETION_MIN_SECONDS
    ):

        return DatasetClassification.DELETION_ELIGIBLE_EMPTY_DATASET

    return DatasetClassification.IN_USE_DATASET
