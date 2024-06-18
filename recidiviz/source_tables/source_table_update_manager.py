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
from typing import Any

import attr
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


@attr.s(auto_attribs=True)
class SourceTableCollectionUpdateConfig:
    source_table_collection: SourceTableCollection
    allow_field_deletions: bool
    # For tables that are ephemeral, or whose contents are fully repopulated each time they're changed,
    # it may be beneficial to recreate the table in the case of an update error (e.g. incompatible schema type changes,
    # clustering field mismatch)
    recreate_on_update_error: bool = attr.ib(default=False)


class SourceTableDryRunResult(enum.StrEnum):
    CREATE_TABLE = "create_table"
    MISMATCH_CLUSTERING_FIELDS = "mismatch_clustering_fields"
    UPDATE_SCHEMA_NO_CHANGES = "update_schema_no_changes"
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


class SourceTableUpdateManager:
    """Class for managing source table updates"""

    def __init__(self, client: BigQueryClient | None = None) -> None:
        self.client = client or BigQueryClientImpl()

    def _dry_run_table(
        self,
        update_config: SourceTableCollectionUpdateConfig,
        source_table_address: BigQueryAddress,
    ) -> tuple[BigQueryAddress, SourceTableDryRunResult]:
        """Performs a dry run of a table update for a given config"""
        source_table_config = (
            update_config.source_table_collection.source_tables_by_address[
                source_table_address
            ]
        )
        dataset_ref = self.client.dataset_ref_for_id(
            source_table_config.address.dataset_id
        )
        result = SourceTableDryRunResult.CREATE_TABLE
        if self.client.table_exists(dataset_ref, source_table_config.address.table_id):
            # Compare schema derived from metric class to existing dataflow views and
            # update if necessary.
            current_table = self.client.get_table(
                dataset_ref, source_table_config.address.table_id
            )

            if _validate_clustering_fields_match(current_table, source_table_config):
                return (
                    source_table_config.address,
                    SourceTableDryRunResult.MISMATCH_CLUSTERING_FIELDS,
                )

            table_schema_fields = {
                field.name: field for field in source_table_config.schema_fields
            }
            table_schema_field_names = set(table_schema_fields.keys())
            desired_schema_fields = {
                field.name: field for field in current_table.schema
            }
            desired_schema_field_names = set(desired_schema_fields.keys())
            to_add = desired_schema_field_names - table_schema_field_names
            to_remove = table_schema_field_names - desired_schema_field_names

            if table_schema_field_names == desired_schema_field_names:
                result = SourceTableDryRunResult.UPDATE_SCHEMA_NO_CHANGES
                for name in table_schema_field_names:
                    old_schema_field = table_schema_fields[name]
                    new_schema_field = desired_schema_fields[name]

                    if old_schema_field.field_type != new_schema_field.field_type:
                        result = SourceTableDryRunResult.UPDATE_SCHEMA_TYPE_CHANGES

                    if old_schema_field.mode != new_schema_field.mode:
                        result = SourceTableDryRunResult.UPDATE_SCHEMA_MODE_CHANGES
            elif to_add and to_remove:
                result = SourceTableDryRunResult.UPDATE_SCHEMA_WITH_CHANGES
            elif to_add:
                result = SourceTableDryRunResult.UPDATE_SCHEMA_WITH_ADDITIONS
            else:
                result = SourceTableDryRunResult.UPDATE_SCHEMA_WITH_DELETIONS

        return source_table_config.address, result

    def dry_run(
        self, update_configs: list[SourceTableCollectionUpdateConfig], log_file: str
    ) -> dict[BigQueryAddress, SourceTableDryRunResult]:
        """Performs a dry run update of the schemas of all tables specified by the provided list of collections,
        printing a progress bar as tables complete"""

        results: dict[BigQueryAddress, SourceTableDryRunResult] = {}

        with redirect_logging_to_file(log_file):
            successes, exceptions = map_fn_with_progress_bar_results(
                fn=self._dry_run_table,
                kwargs_list=[
                    {
                        "update_config": update_config,
                        "source_table_address": source_table_config.address,
                    }
                    for update_config in update_configs
                    for source_table_config in update_config.source_table_collection.source_tables
                ],
                max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2),
                timeout=60 * 10,
                progress_bar_message="Performing source table dry run...",
            )

            if exceptions:
                raise ValueError(
                    "Found exceptions doing a source table update dry run - check logs!"
                )

            for success_result, _ in successes:
                address, result = success_result

                results[address] = result

            return results

    def _update_table(
        self,
        update_config: SourceTableCollectionUpdateConfig,
        source_table_address: BigQueryAddress,
    ) -> None:
        """Updates a single source table's schema'"""
        source_table_config = (
            update_config.source_table_collection.source_tables_by_address[
                source_table_address
            ]
        )
        dataset_ref = self.client.dataset_ref_for_id(
            source_table_config.address.dataset_id
        )
        try:
            if self.client.table_exists(
                dataset_ref, source_table_config.address.table_id
            ):
                # Compare schema derived from metric class to existing dataflow views and
                # update if necessary.
                current_table = self.client.get_table(
                    dataset_ref, source_table_config.address.table_id
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
        self, update_config: SourceTableCollectionUpdateConfig
    ) -> None:
        dataset_ref = self.client.dataset_ref_for_id(
            update_config.source_table_collection.dataset_id
        )
        self.client.create_dataset_if_necessary(
            dataset_ref=dataset_ref,
            default_table_expiration_ms=update_config.source_table_collection.table_expiration_ms,
        )

    def update(self, update_config: SourceTableCollectionUpdateConfig) -> None:
        self._create_dataset_if_necessary(update_config=update_config)

        for source_table_config in update_config.source_table_collection.source_tables:
            self._update_table(update_config, source_table_config.address)

    def update_async(
        self, update_configs: list[SourceTableCollectionUpdateConfig], log_file: str
    ) -> tuple[
        list[tuple[Any, dict[str, Any]]], list[tuple[Exception, dict[str, Any]]]
    ]:
        """Updates the schemas of all tables specified by the provided list of collections, printing a progress bar as tables complete"""
        logging.info("Logs can be found at %s", log_file)
        with redirect_logging_to_file(log_file):
            datasets_to_create: dict[str, SourceTableCollectionUpdateConfig] = {
                update_config.source_table_collection.dataset_id: update_config
                for update_config in update_configs
            }

            map_fn_with_progress_bar_results(
                fn=self._create_dataset_if_necessary,
                kwargs_list=[
                    {"update_config": update_config}
                    for update_config in datasets_to_create.values()
                ],
                max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2),
                timeout=60 * 10,  # 3 minutes
                progress_bar_message="Creating datasets if necessary...",
            )

            return map_fn_with_progress_bar_results(
                fn=self._update_table,
                kwargs_list=[
                    {
                        "update_config": update_config,
                        "source_table_address": source_table_config.address,
                    }
                    for update_config in update_configs
                    for source_table_config in update_config.source_table_collection.source_tables
                ],
                max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2),
                timeout=60 * 10,  # 10 minutes
                progress_bar_message="Updating table schemas...",
            )
