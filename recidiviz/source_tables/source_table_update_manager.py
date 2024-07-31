# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Utilities for updating source table schema"""
import enum
import logging
from collections import defaultdict
from typing import Any, Iterable

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import (
    BQ_CLIENT_MAX_POOL_SIZE,
    BigQueryClient,
    BigQueryClientImpl,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableConfig,
)
from recidiviz.tools.deploy.logging import redirect_logging_to_file
from recidiviz.utils.future_executor import map_fn_with_progress_bar_results


class SourceTableFailedToUpdateError(ValueError):
    pass


class SourceTableDryRunResult(enum.StrEnum):
    CREATE_TABLE = "create_table"
    MISMATCH_CLUSTERING_FIELDS = "mismatch_clustering_fields"
    NO_CHANGES = "no_changes"
    UPDATE_SCHEMA_WITH_CHANGES = "update_schema_with_changes"
    UPDATE_SCHEMA_MODE_CHANGES = "update_schema_mode_changes"
    UPDATE_SCHEMA_TYPE_CHANGES = "update_schema_field_type_changes"
    UPDATE_SCHEMA_WITH_ADDITIONS = "update_schema_with_additions"
    UPDATE_SCHEMA_WITH_DELETIONS = "update_schema_with_deletions"


def _validate_clustering_fields_match(
    current_table: bigquery.Table, source_table_config: SourceTableConfig
) -> bool:
    if (
        not current_table.clustering_fields
        and not source_table_config.clustering_fields
    ):
        return True

    return current_table.clustering_fields == source_table_config.clustering_fields


def validate_table_schema_fields(
    table_schema_fields: dict[str, bigquery.SchemaField],
    desired_schema_fields: dict[str, bigquery.SchemaField],
    field_names: Iterable[str],
) -> SourceTableDryRunResult | None:
    for name in field_names:
        old_schema_field = table_schema_fields[name]
        new_schema_field = desired_schema_fields[name]

        if old_schema_field.field_type != new_schema_field.field_type:
            return SourceTableDryRunResult.UPDATE_SCHEMA_TYPE_CHANGES

        if old_schema_field.mode != new_schema_field.mode:
            return SourceTableDryRunResult.UPDATE_SCHEMA_MODE_CHANGES

    return None


class SourceTableUpdateManager:
    """Class for managing source table updates"""

    def __init__(self, client: BigQueryClient | None = None) -> None:
        self.client = client or BigQueryClientImpl()

    def _dry_run_table(
        self,
        source_table_collection: SourceTableCollection,
        source_table_address: BigQueryAddress,
    ) -> tuple[BigQueryAddress, SourceTableDryRunResult]:
        """Performs a dry run of a table update for a given config"""
        source_table_config = source_table_collection.source_tables_by_address[
            source_table_address
        ]
        result = SourceTableDryRunResult.CREATE_TABLE
        if self.client.table_exists(
            source_table_config.address.dataset_id, source_table_config.address.table_id
        ):
            # Compare schema derived from metric class to existing dataflow views and
            # update if necessary.
            current_table = self.client.get_table(
                source_table_config.address.dataset_id,
                source_table_config.address.table_id,
            )

            if not _validate_clustering_fields_match(
                current_table, source_table_config
            ):
                return (
                    source_table_config.address,
                    SourceTableDryRunResult.MISMATCH_CLUSTERING_FIELDS,
                )

            table_schema_fields = {field.name: field for field in current_table.schema}
            table_schema_field_names = set(table_schema_fields.keys())
            desired_schema_fields = {
                field.name: field for field in source_table_config.schema_fields
            }
            desired_schema_field_names = set(desired_schema_fields.keys())
            to_add = desired_schema_field_names - table_schema_field_names
            to_remove = table_schema_field_names - desired_schema_field_names

            if (
                source_table_collection.validation_config
                and source_table_collection.validation_config.only_check_required_columns
            ):
                if desired_schema_field_names.issubset(table_schema_field_names):
                    result = SourceTableDryRunResult.NO_CHANGES

                    if field_changes := validate_table_schema_fields(
                        table_schema_fields,
                        desired_schema_fields,
                        desired_schema_field_names,
                    ):
                        result = field_changes
                else:
                    result = SourceTableDryRunResult.UPDATE_SCHEMA_WITH_CHANGES

                return source_table_config.address, result

            if table_schema_field_names == desired_schema_field_names:
                result = SourceTableDryRunResult.NO_CHANGES
                if field_changes := validate_table_schema_fields(
                    table_schema_fields, desired_schema_fields, table_schema_field_names
                ):
                    result = field_changes
            elif to_add and to_remove:
                result = SourceTableDryRunResult.UPDATE_SCHEMA_WITH_CHANGES
            elif to_add:
                result = SourceTableDryRunResult.UPDATE_SCHEMA_WITH_ADDITIONS
            else:
                result = SourceTableDryRunResult.UPDATE_SCHEMA_WITH_DELETIONS

        return source_table_config.address, result

    def dry_run(
        self, source_table_collections: list[SourceTableCollection], log_file: str
    ) -> dict[SourceTableDryRunResult, list[BigQueryAddress]]:
        """Performs a dry run update of the schemas of all tables specified by the provided list of collections,
        printing a progress bar as tables complete"""

        result_by_address: dict[BigQueryAddress, SourceTableDryRunResult] = {}

        with redirect_logging_to_file(log_file):
            successes, exceptions = map_fn_with_progress_bar_results(
                fn=self._dry_run_table,
                kwargs_list=[
                    {
                        "source_table_collection": source_table_collection,
                        "source_table_address": source_table_config.address,
                    }
                    for source_table_collection in source_table_collections
                    for source_table_config in source_table_collection.source_tables
                ],
                max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2),
                timeout=60 * 10,
                progress_bar_message="Performing source table dry run...",
            )

            if exceptions:
                for exception in exceptions:
                    logging.exception(exception)
                raise ValueError(
                    "Found exceptions doing a source table update dry run - check logs!"
                )

            for success_result, _ in successes:
                address, result = success_result

                result_by_address[address] = result

        changes: dict[SourceTableDryRunResult, list[BigQueryAddress]] = defaultdict(
            list
        )
        for address, result in result_by_address.items():
            if result != SourceTableDryRunResult.NO_CHANGES:
                changes[result].append(address)

        return changes

    def _update_table(
        self,
        source_table_collection: SourceTableCollection,
        source_table_address: BigQueryAddress,
    ) -> None:
        """Updates a single source table's schema"""
        source_table_config = source_table_collection.source_tables_by_address[
            source_table_address
        ]
        update_config = source_table_collection.update_config

        if not update_config.attempt_to_manage:
            raise ValueError(
                f"Attempted to update unmanaged table {source_table_address.to_str()}"
            )

        try:
            if self.client.table_exists(
                source_table_config.address.dataset_id,
                source_table_config.address.table_id,
            ):
                # Compare schema derived from metric class to existing dataflow views and
                # update if necessary.
                current_table = self.client.get_table(
                    source_table_config.address.dataset_id,
                    source_table_config.address.table_id,
                )
                try:
                    if not _validate_clustering_fields_match(
                        current_table, source_table_config
                    ):
                        raise ValueError(
                            f"Existing table: {source_table_config.address} "
                            f"has clustering fields {current_table.clustering_fields} that do "
                            f"not match {source_table_config.clustering_fields}"
                        )

                    self.client.update_schema(
                        source_table_config.address.dataset_id,
                        source_table_config.address.table_id,
                        source_table_config.schema_fields,
                        allow_field_deletions=update_config.allow_field_deletions,
                    )
                except ValueError as e:
                    if not update_config.recreate_on_update_error:
                        raise e

                    logging.warning(
                        "Failed to update schema for %s due to %s, will try to delete and create table.",
                        source_table_address.to_str(),
                        e,
                    )

                    # We are okay deleting and recreating the table as its contents are deleted / recreated
                    self.client.delete_table(
                        dataset_id=source_table_config.address.dataset_id,
                        table_id=source_table_config.address.table_id,
                    )
                    self.client.create_table_with_schema(
                        dataset_id=source_table_config.address.dataset_id,
                        table_id=source_table_config.address.table_id,
                        schema_fields=source_table_config.schema_fields,
                    )
            else:
                self.client.create_table_with_schema(
                    source_table_config.address.dataset_id,
                    source_table_config.address.table_id,
                    source_table_config.schema_fields,
                    clustering_fields=source_table_config.clustering_fields,
                )
        except Exception as e:
            logging.exception(
                "Failed to update schema for `%s`",
                source_table_config.address.to_str(),
            )
            #  pylint: disable=raise-missing-from
            raise SourceTableFailedToUpdateError(
                f"Failed to update schema for `{source_table_config.address.to_str()}`: {e}"
            )

    def _create_dataset_if_necessary(
        self,
        source_table_collection: SourceTableCollection,
    ) -> None:
        self.client.create_dataset_if_necessary(
            dataset_id=source_table_collection.dataset_id,
            default_table_expiration_ms=source_table_collection.table_expiration_ms,
        )

    def update(self, source_table_collection: SourceTableCollection) -> None:
        self._create_dataset_if_necessary(
            source_table_collection=source_table_collection
        )

        for source_table_config in source_table_collection.source_tables:
            self._update_table(source_table_collection, source_table_config.address)

    def update_async(
        self,
        source_table_collections: list[SourceTableCollection],
        log_file: str,
        log_output: bool = False,
    ) -> tuple[
        list[tuple[Any, dict[str, Any]]], list[tuple[Exception, dict[str, Any]]]
    ]:
        """Updates the schemas of all tables specified by the provided list of collections, printing a progress bar
        as tables complete"""
        logging.info("Logs can be found at %s", log_file)
        with redirect_logging_to_file(log_file):
            datasets_to_create = {
                source_table_collection.dataset_id: source_table_collection
                for source_table_collection in source_table_collections
            }

            _, exceptions = map_fn_with_progress_bar_results(
                fn=self._create_dataset_if_necessary,
                kwargs_list=[
                    {"source_table_collection": source_table_collection}
                    for source_table_collection in datasets_to_create.values()
                ],
                max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2),
                timeout=60 * 10,  # 3 minutes
                progress_bar_message="Creating datasets if necessary...",
            )
            if exceptions:
                raise ValueError(
                    f"Failed to create datasets, encountered the following exceptions: {exceptions}"
                )

            successes, exceptions = map_fn_with_progress_bar_results(
                fn=self._update_table,
                kwargs_list=[
                    {
                        "source_table_collection": source_table_collection,
                        "source_table_address": source_table_config.address,
                    }
                    for source_table_collection in source_table_collections
                    for source_table_config in source_table_collection.source_tables
                ],
                max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2),
                timeout=60 * 10,  # 10 minutes
                progress_bar_message="Updating table schemas...",
            )

            if exceptions:
                raise ValueError(
                    f"Failed to update table schemas, encountered the following exceptions: {exceptions}"
                )

        if log_output:
            with open(log_file, "r", encoding="utf-8") as file:
                for line in file.readlines():
                    logging.info(line)

        return successes, exceptions
