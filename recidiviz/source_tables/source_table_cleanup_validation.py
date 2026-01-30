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
"""Validation logic for source table datasets in BigQuery."""
import logging
from collections import defaultdict

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.source_tables.source_table_repository import SourceTableRepository
from recidiviz.source_tables.untracked_source_table_exemptions import (
    ALLOWED_TABLES_IN_SOURCE_TABLE_DATASETS_WITH_NO_CONFIG,
)


def validate_clean_source_table_datasets(
    bq_client: BigQueryClient,
    source_table_repository: SourceTableRepository,
) -> None:
    """Validates that source table datasets contain exactly the tables we expect
    based on source table configs.

    Raises:
        ValueError: If any source table dataset contains unexpected tables or
            is missing expected tables.
    """
    validation_errors = []

    expected_source_tables_by_dataset: dict[str, set[BigQueryAddress]] = defaultdict(
        set
    )
    for collection in source_table_repository.source_table_collections:
        # Skip collections with table expiration set (e.g., temp_load datasets)
        # since tables in these datasets are expected to be transient
        if collection.default_table_expiration_ms is not None:
            continue
        dataset_id = collection.dataset_id
        expected_source_tables_by_dataset[dataset_id].update(
            collection.source_tables_by_address
        )

    actual_datasets = {item.dataset_id for item in bq_client.list_datasets()}

    for dataset_id in sorted(expected_source_tables_by_dataset):
        logging.info("Validating source tables in dataset [%s]", dataset_id)

        # Get expected tables from source table configs (already filtered by
        # deployed_projects for the current project)
        expected_tables = {
            address.table_id
            for address in expected_source_tables_by_dataset[dataset_id]
        }

        # Skip datasets where there are no expected tables in this project and the
        # dataset does not exist (e.g., billing data tables that only exist in prod)
        if not expected_tables and dataset_id not in actual_datasets:
            continue

        # Get actual tables in BigQuery
        actual_tables = {table.table_id for table in bq_client.list_tables(dataset_id)}

        # Get legacy source tables for this dataset (if any)
        legacy_tables = ALLOWED_TABLES_IN_SOURCE_TABLE_DATASETS_WITH_NO_CONFIG.get(
            dataset_id, set()
        )

        # Find unexpected tables (in BQ but not in configs, excluding legacy)
        unexpected_tables = actual_tables - expected_tables - legacy_tables
        if unexpected_tables:
            validation_errors.append(
                f"Dataset {dataset_id} contains unexpected tables: {sorted(unexpected_tables)}"
            )

        # Find missing tables (in configs but not in BQ)
        missing_tables = expected_tables - actual_tables
        if missing_tables:
            validation_errors.append(
                f"Dataset {dataset_id} is missing expected tables: {sorted(missing_tables)}"
            )

    if validation_errors:
        error_message = (
            "Source table dataset validation failed:\n"
            + "\n".join(f"  - {error}" for error in validation_errors)
            + "\n\nTo fix unexpected tables:"
            + "\n  1. If these are new tables that should be tracked, add YAML configs using:"
            + "\n     python -m recidiviz.tools.update_source_table_yaml --dataset-id <dataset_id> --table-ids <table_id> --project-id <project_id>"
            + "\n  2. If these are legacy tables that existed before source table management, add them to"
            + "\n     ALLOWED_TABLES_IN_SOURCE_TABLE_DATASETS_WITH_NO_CONFIG in untracked_source_table_exemptions.py"
            + "\n  3. If these are truly unwanted tables, delete them from BigQuery. If you think an automatic"
            + "\n     process should have deleted this table, consult in #platform-team."
            + "\n\nTo verify your fix locally, run:"
            + "\n  python -m recidiviz.tools.validate_source_table_datasets --project-id <project_id>"
        )
        raise ValueError(error_message)

    logging.info(
        "Successfully validated %d source table datasets",
        len(expected_source_tables_by_dataset),
    )
