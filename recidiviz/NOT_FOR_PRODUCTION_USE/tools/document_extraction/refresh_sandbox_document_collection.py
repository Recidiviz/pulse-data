# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tool to upload a sample of documents from a document collection to a sandbox.

This script identifies documents that need to be uploaded for a given collection,
uploads them to a sandbox GCS bucket, and records metadata to sandbox BQ datasets.

Usage:
    python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.refresh_sandbox_document_collection \
        --project_id [PROJECT_ID] \
        --collection_name [COLLECTION_NAME] \
        --sandbox_dataset_prefix [PREFIX] \
        --sandbox_bucket [BUCKET] \
        --sample_size [SAMPLE_SIZE]

Example:
    python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.refresh_sandbox_document_collection \
        --project_id recidiviz-staging \
        --collection_name US_IX_CASE_NOTES \
        --sandbox_dataset_prefix anna \
        --sandbox_bucket recidiviz-staging-anna-scratch \
        --sample_size 100
"""
import argparse
import logging
import sys
from typing import List, Tuple

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.store.document_collection_config import (
    DOCUMENT_ID_COLUMN_NAME,
    DOCUMENT_STORE_METADATA_DATASET_ID,
    collect_document_collection_configs,
    get_document_collection_config,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.store.document_store_updater import (
    DocumentStoreUpdater,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.store.new_document_identifier import (
    DEFAULT_TEMP_DATASET_ID,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override, project_id

# 72-hour expiration for sandbox datasets (in milliseconds)
SANDBOX_DATASET_EXPIRATION_MS = 72 * 60 * 60 * 1000


def main(
    collection_name: str,
    sandbox_dataset_prefix: str,
    sandbox_bucket: str,
    sample_size: int | None,
    sample_entity_count: int | None,
    active_in_compartment: str | None,
    lookback_days: int | None = None,
) -> None:
    """Uploads documents from a collection to a sandbox."""
    current_project_id = project_id()

    # Get the collection config
    logging.info("Loading collection config for %s", collection_name)
    collection_config = get_document_collection_config(collection_name)
    logging.info(
        "Collection: %s (state: %s)",
        collection_config.collection_name,
        collection_config.state_code.value if collection_config.state_code else "N/A",
    )
    logging.info("Description: %s", collection_config.collection_description)

    # Create sandbox dataset names
    temp_dataset_id = f"{sandbox_dataset_prefix}_{DEFAULT_TEMP_DATASET_ID}"
    metadata_dataset_id = (
        f"{sandbox_dataset_prefix}_{DOCUMENT_STORE_METADATA_DATASET_ID}"
    )

    logging.info("Using sandbox datasets:")
    logging.info("  Temp dataset: %s", temp_dataset_id)
    logging.info("  Metadata dataset: %s", metadata_dataset_id)
    logging.info("  GCS bucket: %s", sandbox_bucket)

    # Initialize clients
    bq_client = BigQueryClientImpl()
    gcs_fs = GcsfsFactory.build()

    # Create sandbox datasets if they don't exist (with expiration for sandbox)
    bq_client.create_dataset_if_necessary(
        temp_dataset_id,
        default_table_expiration_ms=SANDBOX_DATASET_EXPIRATION_MS,
    )
    bq_client.create_dataset_if_necessary(
        metadata_dataset_id,
        default_table_expiration_ms=SANDBOX_DATASET_EXPIRATION_MS,
    )

    # Create metadata table if it doesn't exist
    metadata_address = collection_config.metadata_table_address(
        dataset_override=metadata_dataset_id
    )
    if not bq_client.table_exists(metadata_address):
        logging.info("Creating metadata table: %s", metadata_address)
        bq_client.create_table_with_schema(
            address=metadata_address,
            schema_fields=collection_config.metadata_table_schema(),
            clustering_fields=[DOCUMENT_ID_COLUMN_NAME],
        )

    # Use DocumentStoreUpdater to orchestrate the process
    updater = DocumentStoreUpdater(
        big_query_client=bq_client,
        gcs_fs=gcs_fs,
        project_id=current_project_id,
        temp_dataset_id=temp_dataset_id,
        metadata_dataset_override=metadata_dataset_id,
        sandbox_bucket=sandbox_bucket,
    )

    result = updater.update_document_collection(
        collection_config=collection_config,
        sample_size=sample_size,
        sample_entity_count=sample_entity_count,
        active_in_compartment=active_in_compartment,
        batch_size=50,
        lookback_days=lookback_days,
    )

    # Clean up the temp dataset
    logging.info("Cleaning up temp dataset: %s", temp_dataset_id)
    bq_client.delete_dataset(
        dataset_id=temp_dataset_id,
        delete_contents=True,
    )

    # Report results
    logging.info("=" * 60)
    logging.info("Upload complete!")
    logging.info("  Successful uploads: %d", len(result.successes))
    logging.info("  Failed uploads: %d", len(result.failures))
    logging.info(
        "  Metadata table: %s.%s",
        metadata_dataset_id,
        collection_config.metadata_table_address().table_id,
    )

    if result.failures:
        logging.warning("Failed document IDs:")
        for doc_id, error in result.failures.items():
            logging.warning("  %s: %s", doc_id, str(error))

    if result.successes:
        logging.info("Sample uploaded files:")
        for i, (doc_id, path) in enumerate(result.successes.items()):
            if i >= 5:
                logging.info("  ... and %d more", len(result.successes) - 5)
                break
            logging.info("  %s -> %s", doc_id[:16] + "...", path.uri())


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser(
        description="Upload documents from a collection to a sandbox."
    )

    parser.add_argument(
        "--list_collections",
        action="store_true",
        help="List all available document collections and exit.",
    )
    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=False,
        help="The GCP project ID to use.",
    )
    parser.add_argument(
        "--collection_name",
        dest="collection_name",
        type=str,
        required=False,
        help="The name of the document collection to upload (e.g., US_IX_CASE_NOTES).",
    )
    parser.add_argument(
        "--sandbox_dataset_prefix",
        dest="sandbox_dataset_prefix",
        type=str,
        required=False,
        help="Prefix for sandbox BQ datasets (e.g., 'anna' creates anna_temp_document_collection).",
    )
    parser.add_argument(
        "--sandbox_bucket",
        dest="sandbox_bucket",
        type=str,
        required=False,
        help="The GCS bucket to upload documents to (e.g., recidiviz-staging-anna-scratch).",
    )
    parser.add_argument(
        "--sample_size",
        dest="sample_size",
        type=int,
        default=None,
        help="Maximum number of documents to upload. If not specified, uploads all new documents.",
    )
    parser.add_argument(
        "--sample_entity_count",
        dest="sample_entity_count",
        type=int,
        default=None,
        help=(
            "Number of distinct entities (e.g., people) to sample. "
            "Returns ALL documents for the sampled entities. "
            "Cannot be used together with --sample_size."
        ),
    )
    parser.add_argument(
        "--active_in_compartment",
        dest="active_in_compartment",
        type=str,
        default=None,
        help=(
            "If provided, restricts entity sampling to people with an active "
            "session in the given compartment (e.g., SUPERVISION, INCARCERATION)."
        ),
    )
    parser.add_argument(
        "--lookback_days",
        dest="lookback_days",
        type=int,
        default=None,
        help=(
            "If provided, only includes documents whose document_update_datetime "
            "is within the last N days."
        ),
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv[1:])

    if known_args.list_collections:
        configs = collect_document_collection_configs()
        print("Available document collections:")
        for name, config in sorted(configs.items()):
            state = config.state_code.value if config.state_code else "N/A"
            print(f"  {name} (state: {state})")
        sys.exit(0)

    # Validate required arguments for upload
    if not known_args.project_id:
        print("Error: --project_id is required for upload")
        sys.exit(1)
    if not known_args.collection_name:
        print("Error: --collection_name is required for upload")
        sys.exit(1)
    if not known_args.sandbox_dataset_prefix:
        print("Error: --sandbox_dataset_prefix is required for upload")
        sys.exit(1)
    if not known_args.sandbox_bucket:
        print("Error: --sandbox_bucket is required for upload")
        sys.exit(1)

    with local_project_id_override(known_args.project_id):
        main(
            collection_name=known_args.collection_name,
            sandbox_dataset_prefix=known_args.sandbox_dataset_prefix,
            sandbox_bucket=known_args.sandbox_bucket,
            sample_size=known_args.sample_size,
            sample_entity_count=known_args.sample_entity_count,
            active_in_compartment=known_args.active_in_compartment,
            lookback_days=known_args.lookback_days,
        )
