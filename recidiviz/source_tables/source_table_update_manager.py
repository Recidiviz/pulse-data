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

import attr
import google
from google.cloud import bigquery
from google.cloud.bigquery import ExternalConfig
from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import (
    BQ_CLIENT_MAX_POOL_SIZE,
    BigQueryClient,
    BigQueryClientImpl,
)
from recidiviz.big_query.big_query_external_table_utils import (
    external_config_has_non_schema_updates,
    get_external_config_non_schema_updates,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    SourceTableConfig,
)
from recidiviz.tools.deploy.logging import redirect_logging_to_file
from recidiviz.utils.future_executor import map_fn_with_progress_bar_results
from recidiviz.utils.types import assert_type


class SourceTableFailedToUpdateError(ValueError):
    pass


class SourceTableUpdateType(enum.StrEnum):
    """Describes the type of change a source table update job would need to make to a
    given source table in order to make the schema in BQ match the schema defined in
    code.
    """

    CREATE_TABLE = "create_table"

    # Existing table table-level changes
    MISMATCH_CLUSTERING_FIELDS = "mismatch_clustering_fields"
    MISMATCH_PARTITIONING_FIELDS = "mismatch_partitioning_fields"
    UPDATE_SCHEMA_WITH_ADDITIONS = "update_schema_with_additions"
    UPDATE_SCHEMA_WITH_DELETIONS = "update_schema_with_deletions"
    ADD_EXTERNAL_DATA_CONFIGURATION = "add_external_data_configuration"
    REMOVE_EXTERNAL_DATA_CONFIGURATION = "remove_external_data_configuration"

    # Describes any update to a table's external data configuration outside of a schema
    # change. Schema changes will be captured by the field-level update types below.
    UPDATE_EXTERNAL_DATA_CONFIGURATION = "update_external_data_configuration"

    # Existing table field-level changes
    DOCUMENTATION_CHANGE = "documentation_change"
    UPDATE_SCHEMA_MODE_CHANGES = "update_schema_mode_changes"
    UPDATE_SCHEMA_TYPE_CHANGES = "update_schema_field_type_changes"

    @property
    def can_be_applied_to_existing_tables(self) -> bool:
        """If True, updates of this type CAN be applied to an existing table (i.e. will
        BQ allow us to make this change), regardless of whether the updates. If False,
        this table must be deleted and re-generated to apply this update.
        """

        if self is SourceTableUpdateType.CREATE_TABLE:
            raise ValueError(f"Unexpected change type for existing table: {self}")

        if self in (
            # Partitions and clustering fields are immutable after table creation.
            SourceTableUpdateType.MISMATCH_PARTITIONING_FIELDS,
            SourceTableUpdateType.MISMATCH_CLUSTERING_FIELDS,
            # A normal table cannot become an external table after creation or vice
            # versa.
            SourceTableUpdateType.ADD_EXTERNAL_DATA_CONFIGURATION,
            SourceTableUpdateType.REMOVE_EXTERNAL_DATA_CONFIGURATION,
        ):
            return False

        if self in (
            SourceTableUpdateType.UPDATE_SCHEMA_WITH_ADDITIONS,
            SourceTableUpdateType.UPDATE_SCHEMA_WITH_DELETIONS,
            SourceTableUpdateType.DOCUMENTATION_CHANGE,
            # TODO(#39040): Differentiate between NULLABLE -> REQUIRED (may crash on
            #  non-empty tables) and REQUIRED -> NULLABLE (always allowed).
            SourceTableUpdateType.UPDATE_SCHEMA_MODE_CHANGES,
            # Some type changes cannot be applied (i.e. if all values can't be cast to
            # INT64), but it's possible that the change will be applied successfully,
            # so we allow it by default.
            SourceTableUpdateType.UPDATE_SCHEMA_TYPE_CHANGES,
            # Most external data config changes are ok to apply and we enforce that
            # external data tables are regenerable, so we will just delete and recreate
            # the table if we can't apply changes.
            SourceTableUpdateType.UPDATE_EXTERNAL_DATA_CONFIGURATION,
        ):
            return True

        raise ValueError(f"Unhandled type [{self}]")

    def is_allowed_update_for_config(
        self, update_config: SourceTableCollectionUpdateConfig, table_exists: bool
    ) -> bool:
        """Returns True if this update type is allowed for a source table with the given
        |update_config|, False otherwise.
        """
        if update_config == SourceTableCollectionUpdateConfig.externally_managed():
            # We don't make any updates for ANY externally-managed tables.
            return False

        if self is SourceTableUpdateType.DOCUMENTATION_CHANGE:
            return True

        if update_config == SourceTableCollectionUpdateConfig.regenerable():
            # Any change type is allowed - if BQ yells / crashes when applying this
            # change (e.g. changing mode from NULLABLE -> REQUIRED), we'll just delete
            # the table and recreate it.
            return True

        if not table_exists and self is SourceTableUpdateType.CREATE_TABLE:
            return True

        if update_config != SourceTableCollectionUpdateConfig.protected():
            raise ValueError(f"Unexpected update_config: {update_config}")

        if not table_exists:
            raise ValueError("Expected table_exists=True at this point.")

        if not self.can_be_applied_to_existing_tables:
            return False

        if self in {
            SourceTableUpdateType.UPDATE_SCHEMA_WITH_ADDITIONS,
        }:
            return True

        # These are all potentially possible to apply but could crash / result in
        # data loss, so we disallow these.
        if self in {
            SourceTableUpdateType.UPDATE_SCHEMA_WITH_DELETIONS,
            SourceTableUpdateType.UPDATE_SCHEMA_TYPE_CHANGES,
            SourceTableUpdateType.UPDATE_SCHEMA_MODE_CHANGES,
            SourceTableUpdateType.UPDATE_EXTERNAL_DATA_CONFIGURATION,
        }:
            return False

        raise ValueError(f"Unexpected SourceTableUpdateType: {self}")

    @property
    def is_single_existing_field_update_type(self) -> bool:
        """Returns True if this is an update type that pertains to modifying a single,
        existing field without deleting it.
        """
        match self:
            case (
                self.UPDATE_SCHEMA_MODE_CHANGES
                | self.UPDATE_SCHEMA_TYPE_CHANGES
                | self.DOCUMENTATION_CHANGE
            ):
                return True

            case (
                self.CREATE_TABLE
                | self.MISMATCH_CLUSTERING_FIELDS
                | self.MISMATCH_PARTITIONING_FIELDS
                | self.UPDATE_SCHEMA_WITH_ADDITIONS
                | self.UPDATE_SCHEMA_WITH_DELETIONS
                | self.ADD_EXTERNAL_DATA_CONFIGURATION
                | self.REMOVE_EXTERNAL_DATA_CONFIGURATION
                | self.UPDATE_EXTERNAL_DATA_CONFIGURATION
            ):
                return False

        raise ValueError(f"Unhandled type [{self}]")


def _validate_clustering_fields_match(
    current_table: bigquery.Table, source_table_config: SourceTableConfig
) -> bool:
    if (
        not current_table.clustering_fields
        and not source_table_config.clustering_fields
    ):
        return True

    return current_table.clustering_fields == source_table_config.clustering_fields


def _get_external_data_configuration_non_schema_update_type(
    current_table: bigquery.Table, source_table_config: SourceTableConfig
) -> SourceTableUpdateType | None:
    if (
        current_table.external_data_configuration is None
        and source_table_config.external_data_configuration is not None
    ):
        return SourceTableUpdateType.ADD_EXTERNAL_DATA_CONFIGURATION

    if (
        current_table.external_data_configuration is not None
        and source_table_config.external_data_configuration is None
    ):
        return SourceTableUpdateType.REMOVE_EXTERNAL_DATA_CONFIGURATION

    if (
        current_table.external_data_configuration is not None
        and source_table_config.external_data_configuration is not None
    ):
        if external_config_has_non_schema_updates(
            current_table_external_config=current_table.external_data_configuration,
            desired_external_config=source_table_config.external_data_configuration,
        ):

            return SourceTableUpdateType.UPDATE_EXTERNAL_DATA_CONFIGURATION
    return None


def _validate_partitioning_fields_match(
    current_table: bigquery.Table, source_table_config: SourceTableConfig
) -> bool:
    if (
        not current_table.time_partitioning
        and not source_table_config.time_partitioning
        and current_table.require_partition_filter is not None
        and source_table_config.require_partition_filter is not None
    ):
        return True

    # Allows the implicit None == False since they have the same behavior
    same_require_partition_filter = bool(
        current_table.require_partition_filter
    ) == bool(source_table_config.require_partition_filter)

    return (
        same_require_partition_filter
        and current_table.time_partitioning == source_table_config.time_partitioning
    )


def get_table_field_level_required_updates(
    *,
    existing_table_schema_fields: dict[str, bigquery.SchemaField],
    desired_table_schema_fields: dict[str, bigquery.SchemaField],
    field_names_to_compare: set[str],
) -> dict[str, set[SourceTableUpdateType]]:
    """Given an existing table schema and desired table schema, returns a map of field
    name to a set of all the updates we would need to make to the table schema match the
    desired schema. The |field_names_to_compare| must be a subset of both provided
    schemas.
    """

    if missing_in_existing := field_names_to_compare - set(
        existing_table_schema_fields
    ):
        raise ValueError(
            f"The field_names_to_compare must be a subset of "
            f"existing_table_schema_fields. Missing fields: {missing_in_existing}"
        )
    if missing_in_desired := field_names_to_compare - set(desired_table_schema_fields):
        raise ValueError(
            f"The field_names_to_compare must be a subset of "
            f"desired_table_schema_fields. Missing fields: {missing_in_desired}"
        )

    changes = {}
    for field_name in field_names_to_compare:
        old_schema_field = existing_table_schema_fields[field_name]
        new_schema_field = desired_table_schema_fields[field_name]

        field_changes = set()

        if old_schema_field.field_type != new_schema_field.field_type:
            field_changes.add(SourceTableUpdateType.UPDATE_SCHEMA_TYPE_CHANGES)

        if old_schema_field.mode != new_schema_field.mode:
            field_changes.add(SourceTableUpdateType.UPDATE_SCHEMA_MODE_CHANGES)

        if old_schema_field.description != new_schema_field.description:
            field_changes.add(SourceTableUpdateType.DOCUMENTATION_CHANGE)

        if not field_changes:
            if old_schema_field != new_schema_field:
                raise ValueError(
                    f"Field [{field_name}] has changes of an unknown type. "
                    f"Old field: {old_schema_field}. Desired field: {new_schema_field}"
                )
        changes[field_name] = field_changes
    return changes


@attr.define(kw_only=True)
class SourceTableWithRequiredUpdateTypes:
    """Represents the changes we need to apply to an existing table (or table that
    should exist) in order for it to match the provided |source_table_config|.
    """

    deployed_table: bigquery.Table | None
    source_table_config: SourceTableConfig

    table_level_update_types: set[SourceTableUpdateType]
    existing_field_update_types: dict[str, set[SourceTableUpdateType]]

    def __attrs_post_init__(self) -> None:
        for update_type in self.table_level_update_types:
            if update_type.is_single_existing_field_update_type:
                raise ValueError(
                    f"Found single field update type [{update_type}] in "
                    f"table_level_update_types. Updates of this type should be added "
                    f"to existing_field_update_types."
                )

        for update_types in self.existing_field_update_types.values():
            for update_type in update_types:
                if not update_type.is_single_existing_field_update_type:
                    raise ValueError(
                        f"Found [{update_type}] in existing_field_update_types that is "
                        f"not a single field update type. Updates of this type should "
                        f"be added to table_level_update_types."
                    )

    @property
    def has_updates_to_make(self) -> bool:
        return bool(self.table_level_update_types) or bool(
            self.existing_field_update_types
        )

    @property
    def all_update_types(self) -> set[SourceTableUpdateType]:
        return self.table_level_update_types | {
            t for types in self.existing_field_update_types.values() for t in types
        }

    @property
    def address(self) -> BigQueryAddress:
        return self.source_table_config.address

    def are_changes_safe_to_apply_to_collection(
        self, collection_update_config: SourceTableCollectionUpdateConfig
    ) -> bool:
        """Returns True if the required changes for this table can be applied safely
        given the update config for this table's SourceTableCollection.
        """
        table_exists = self.deployed_table is not None
        return all(
            update_type.is_allowed_update_for_config(
                collection_update_config, table_exists=table_exists
            )
            for update_type in self.all_update_types
        )

    def build_updates_message(self) -> str:
        """Builds a string that tells us all the updates that should / will be applied
        to a table as part of a source table update.
        """
        if not self.all_update_types:
            raise ValueError(
                f"Did not expect to build results without any update_types. Found no "
                f"update types for {self.address.to_str()}"
            )

        sorted_update_types = sorted(self.all_update_types, key=lambda t: t.name)
        update_types_str = ", ".join(t.name for t in sorted_update_types)
        table_str = f"* {self.address.to_str()} ({update_types_str})"
        if not self.deployed_table:
            return table_str

        deployed_schema = self.deployed_table.schema
        deployed_schema_fields = {f.name for f in deployed_schema}
        new_schema = self.source_table_config.schema_fields
        new_schema_schema_fields = {f.name for f in new_schema}

        for update_type in sorted(self.table_level_update_types, key=lambda t: t.name):
            if update_type is SourceTableUpdateType.UPDATE_SCHEMA_WITH_DELETIONS:
                if not (
                    deleted_fields := deployed_schema_fields - new_schema_schema_fields
                ):
                    raise ValueError(
                        f"Expected to find fields to delete, but all existing fields "
                        f"({sorted(deployed_schema_fields)}) are present in the new "
                        f"schema."
                    )
                table_str += "\n  Deleted fields:"
                for f in sorted(deleted_fields):
                    table_str += f"\n    - {f}"
            elif update_type is SourceTableUpdateType.UPDATE_SCHEMA_WITH_ADDITIONS:
                if not (
                    added_fields := new_schema_schema_fields - deployed_schema_fields
                ):
                    raise ValueError(
                        f"Expected to find fields to add, but all fields in the new "
                        f"schema ({sorted(new_schema_schema_fields)}) are present in "
                        f"the deployed table."
                    )

                table_str += "\n  Added fields:"
                for f in sorted(added_fields):
                    table_str += f"\n    - {f}"

            elif (
                update_type is SourceTableUpdateType.UPDATE_EXTERNAL_DATA_CONFIGURATION
            ):
                # Schema changes will be captured in the "Changed fields" section, so
                # we strip the schemas out of the configs here (the BQ API doesn't
                # return the schema on the external config for the deployed table).
                updates = get_external_config_non_schema_updates(
                    current_table_external_config=self.deployed_table.external_data_configuration,
                    desired_external_config=assert_type(
                        self.source_table_config.external_data_configuration,
                        ExternalConfig,
                    ),
                )
                if not updates:
                    raise ValueError(
                        f"Found no external config updates for {self.address.to_str()} "
                        f"even though we registered a {update_type.name} update type."
                    )
                update_lines = ["\n  Changed external_data_configuration fields:"]
                for field, (old_val, new_val) in updates.items():
                    update_lines.append(f"    {field}:")
                    update_lines.append(f"      - old: {old_val}")
                    update_lines.append(f"      - new: {new_val}")
                table_str += "\n".join(update_lines)
            elif update_type in (
                SourceTableUpdateType.MISMATCH_PARTITIONING_FIELDS,
                SourceTableUpdateType.MISMATCH_CLUSTERING_FIELDS,
                SourceTableUpdateType.ADD_EXTERNAL_DATA_CONFIGURATION,
                SourceTableUpdateType.REMOVE_EXTERNAL_DATA_CONFIGURATION,
            ):
                # We don't print any extra info when partitioning / clustering fields
                # or external data configs change.
                pass

            else:
                raise ValueError(f"Unexpected source table update type [{update_type}]")

        changes = []
        for field_name in sorted(self.existing_field_update_types):
            update_types = self.existing_field_update_types[field_name]
            deployed_field = one(f for f in deployed_schema if f.name == field_name)
            new_field = one(f for f in new_schema if f.name == field_name)

            for update_type in sorted(update_types, key=lambda t: t.name):
                if update_type is SourceTableUpdateType.UPDATE_SCHEMA_TYPE_CHANGES:
                    if deployed_field.field_type == new_field.field_type:
                        raise ValueError(
                            f"Expected field_type change for field [{field_name}] in "
                            f"table [{self.address.to_str()}] but found both with "
                            f"field_type [{deployed_field.field_type}]."
                        )
                    changes.append(
                        f"\n    - '{deployed_field.name}' changed TYPE from "
                        f"{deployed_field.field_type} --> {new_field.field_type}"
                    )

                elif update_type is SourceTableUpdateType.UPDATE_SCHEMA_MODE_CHANGES:
                    if deployed_field.mode == new_field.mode:
                        raise ValueError(
                            f"Expected mode change for field [{field_name}] in "
                            f"table [{self.address.to_str()}] but found both with "
                            f"mode [{deployed_field.mode}]."
                        )
                    changes.append(
                        f"\n    - '{deployed_field.name}' changed MODE from "
                        f"{deployed_field.mode} --> {new_field.mode}"
                    )

                elif update_type is SourceTableUpdateType.DOCUMENTATION_CHANGE:
                    if deployed_field.description == new_field.description:
                        raise ValueError(
                            f"Expected description change for field [{field_name}] in "
                            f"table [{self.address.to_str()}] but found both with "
                            f"description [{deployed_field.description}]."
                        )
                    changes.append(
                        f"\n    - '{deployed_field.name}' updated its DESCRIPTION to {new_field.description}"
                    )
                else:
                    raise ValueError(f"Unexpected update_type [{update_type}]")

        if changes:
            table_str += "\n  Changed fields:"
            table_str += "".join(changes)

        return table_str


class SourceTableUpdateManager:
    """Class for managing source table updates"""

    def __init__(self, client: BigQueryClient | None = None) -> None:
        self.client = client or BigQueryClientImpl()

    def _get_required_update_types_for_table_work_fn(
        self,
        source_table_collection_and_address: tuple[
            SourceTableCollection, BigQueryAddress
        ],
    ) -> tuple[BigQueryAddress, SourceTableWithRequiredUpdateTypes]:
        (
            source_table_collection,
            source_table_address,
        ) = source_table_collection_and_address
        return source_table_address, self._get_required_update_types_for_table(
            source_table_collection, source_table_address
        )

    def _get_required_update_types_for_table(
        self,
        source_table_collection: SourceTableCollection,
        source_table_address: BigQueryAddress,
    ) -> SourceTableWithRequiredUpdateTypes:
        """Gets the updates we want to apply to the source table with the given config."""
        source_table_config = source_table_collection.source_tables_by_address[
            source_table_address
        ]

        try:
            current_table = self.client.get_table(source_table_config.address)
        except google.cloud.exceptions.NotFound:
            return SourceTableWithRequiredUpdateTypes(
                source_table_config=source_table_config,
                table_level_update_types={SourceTableUpdateType.CREATE_TABLE},
                existing_field_update_types={},
                deployed_table=None,
            )

        # If the table exists, collect all the ways the table needs to be updated
        table_level_update_types = set()
        if not _validate_partitioning_fields_match(current_table, source_table_config):
            table_level_update_types.add(
                SourceTableUpdateType.MISMATCH_PARTITIONING_FIELDS
            )

        if not _validate_clustering_fields_match(current_table, source_table_config):
            table_level_update_types.add(
                SourceTableUpdateType.MISMATCH_CLUSTERING_FIELDS
            )

        if external_config_update_type := _get_external_data_configuration_non_schema_update_type(
            current_table, source_table_config
        ):
            table_level_update_types.add(external_config_update_type)

        only_check_required_columns = (
            source_table_collection.validation_config
            and source_table_collection.validation_config.only_check_required_columns
        )

        existing_table_schema_fields = {
            field.name: field for field in current_table.schema
        }
        existing_table_schema_field_names = set(existing_table_schema_fields.keys())
        desired_table_schema_fields = {
            field.name: field for field in source_table_config.schema_fields
        }
        desired_table_schema_field_names = set(desired_table_schema_fields.keys())

        to_add = desired_table_schema_field_names - existing_table_schema_field_names
        to_remove = existing_table_schema_field_names - desired_table_schema_field_names

        if to_add:
            table_level_update_types.add(
                SourceTableUpdateType.UPDATE_SCHEMA_WITH_ADDITIONS
            )

        if to_remove and not only_check_required_columns:
            table_level_update_types.add(
                SourceTableUpdateType.UPDATE_SCHEMA_WITH_DELETIONS
            )

        field_names_to_compare = desired_table_schema_field_names.intersection(
            existing_table_schema_field_names
        )
        field_update_types = get_table_field_level_required_updates(
            existing_table_schema_fields=existing_table_schema_fields,
            desired_table_schema_fields=desired_table_schema_fields,
            field_names_to_compare=field_names_to_compare,
        )

        return SourceTableWithRequiredUpdateTypes(
            source_table_config=source_table_config,
            table_level_update_types=table_level_update_types,
            existing_field_update_types={
                field_name: update_types
                for field_name, update_types in field_update_types.items()
                if update_types
            },
            deployed_table=current_table,
        )

    # TODO(#33293): Delete tables in `regenerable()` collection datasets that do not exist in code anymore. We
    #  validate all source table datasets in a separate `dataset_cleanup_and_validation` process.
    def get_changes_to_apply_to_source_tables(
        self, source_table_collections: list[SourceTableCollection], log_file: str
    ) -> dict[BigQueryAddress, SourceTableWithRequiredUpdateTypes]:
        """Checks for changes that should be applied to the source tables specified by
        the provided list of collections, printing a progress bar as tables complete.
        """

        result_by_address: dict[
            BigQueryAddress, SourceTableWithRequiredUpdateTypes
        ] = {}

        with redirect_logging_to_file(log_file):
            get_changes_result = map_fn_with_progress_bar_results(
                work_fn=self._get_required_update_types_for_table_work_fn,
                work_items=[
                    (source_table_collection, source_table_config.address)
                    for source_table_collection in source_table_collections
                    for source_table_config in source_table_collection.source_tables
                ],
                max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2),
                overall_timeout_sec=60 * 10,
                single_work_item_timeout_sec=60 * 10,
                progress_bar_message="Fetching source table changes...",
            )

            if get_changes_result.exceptions:
                for exception in get_changes_result.exceptions:
                    logging.exception(exception)
                raise ValueError(
                    f"Found exceptions while fetching source table changes - check "
                    f"logs: {log_file}"
                )

            for _, result in get_changes_result.successes:
                address, source_table_required_updates = result
                result_by_address[address] = source_table_required_updates

        return {
            address: source_table_required_updates
            for address, source_table_required_updates in result_by_address.items()
            if source_table_required_updates.has_updates_to_make
        }

    def _update_table_work_fn(
        self,
        source_table_collection_and_required_updates: tuple[
            SourceTableCollection, SourceTableWithRequiredUpdateTypes
        ],
    ) -> None:
        (
            source_table_collection,
            source_table_required_updates,
        ) = source_table_collection_and_required_updates
        self._update_table(source_table_collection, source_table_required_updates)

    def _create_table_with_config(self, source_table_config: SourceTableConfig) -> None:
        if source_table_config.external_data_configuration is None:
            self.client.create_table_with_schema(
                address=source_table_config.address,
                schema_fields=source_table_config.schema_fields,
                clustering_fields=source_table_config.clustering_fields,
                time_partitioning=source_table_config.time_partitioning,
                require_partition_filter=source_table_config.require_partition_filter,
            )
        else:
            self.client.create_external_table(
                address=source_table_config.address,
                external_data_config=source_table_config.external_data_configuration,
                allow_auto_detect_schema=False,
            )

    def _update_existing_table_with_config(
        self,
        source_table_config: SourceTableConfig,
        update_config: SourceTableCollectionUpdateConfig,
    ) -> None:
        if source_table_config.external_data_configuration is None:
            self.client.update_schema(
                address=source_table_config.address,
                desired_schema_fields=source_table_config.schema_fields,
                allow_field_deletions=update_config.allow_field_deletions,
            )
        else:
            self.client.update_external_table(
                address=source_table_config.address,
                external_data_config=source_table_config.external_data_configuration,
                allow_auto_detect_schema=False,
            )

    def _update_table(
        self,
        source_table_collection: SourceTableCollection,
        source_table_required_updates: SourceTableWithRequiredUpdateTypes,
    ) -> None:
        """Updates a single source table's schema"""
        source_table_address = source_table_required_updates.address
        source_table_config = source_table_required_updates.source_table_config
        update_config = source_table_collection.update_config

        if not source_table_required_updates.are_changes_safe_to_apply_to_collection(
            update_config
        ):
            update_type_names = sorted(
                t.name for t in source_table_required_updates.all_update_types
            )
            unmanaged_str = (
                "EXTERNALLY MANAGED " if not update_config.attempt_to_manage else ""
            )
            raise SourceTableFailedToUpdateError(
                f"Cannot apply changes of type(s) {update_type_names} to "
                f"{unmanaged_str}table [{source_table_address.to_str()}]."
            )

        try:
            current_table = source_table_required_updates.deployed_table
            if current_table:
                try:
                    self._update_existing_table_with_config(
                        source_table_config, update_config
                    )
                except Exception as e:
                    if not update_config.recreate_on_update_error:
                        raise e

                    logging.warning(
                        "Failed to update schema for %s due to %s, will try to delete and create table.",
                        source_table_address.to_str(),
                        e,
                    )

                    # We are okay deleting and recreating the table as its contents are deleted / recreated
                    self.client.delete_table(address=source_table_config.address)
                    self._create_table_with_config(source_table_config)
            else:
                self._create_table_with_config(source_table_config)
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
            updates = self._get_required_update_types_for_table(
                source_table_collection, source_table_config.address
            )
            self._update_table(source_table_collection, updates)

    # TODO(#33293): Delete tables in `regenerable()` collection datasets that do not exist in code anymore. We
    #  validate all source table datasets in a separate `dataset_cleanup_and_validation` process.
    def update_async(
        self,
        source_table_collections: list[SourceTableCollection],
        log_file: str,
        log_output: bool,
    ) -> None:
        """Updates the schemas of all tables specified by the provided list of collections, printing a progress bar
        as tables complete"""
        logging.info("Logs can be found at %s", log_file)
        with redirect_logging_to_file(log_file):
            updates_to_make = self.get_changes_to_apply_to_source_tables(
                source_table_collections, log_file
            )

            datasets_to_create = {
                source_table_collection.dataset_id: source_table_collection
                for source_table_collection in source_table_collections
            }

            create_datasets_result = map_fn_with_progress_bar_results(
                work_fn=self._create_dataset_if_necessary,
                work_items=list(datasets_to_create.values()),
                max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2),
                overall_timeout_sec=60 * 10,  # 3 minutes
                single_work_item_timeout_sec=60 * 10,  # 3 minutes
                progress_bar_message="Creating datasets if necessary...",
            )
            if create_datasets_result.exceptions:
                raise ExceptionGroup(
                    "Failed to create datasets, encountered the following exception(s)",
                    [
                        exception
                        for (
                            _task_kwargs,
                            exception,
                        ) in create_datasets_result.exceptions
                    ],
                )

            if updates_to_make:
                logging.info(
                    "Found [%s] tables with updates to apply", len(updates_to_make)
                )
                update_tables_result = map_fn_with_progress_bar_results(
                    work_fn=self._update_table_work_fn,
                    work_items=[
                        (
                            source_table_collection,
                            updates_to_make[source_table_config.address],
                        )
                        for source_table_collection in source_table_collections
                        for source_table_config in source_table_collection.source_tables
                        if source_table_config.address in updates_to_make
                    ],
                    max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2),
                    overall_timeout_sec=60 * 10,  # 10 minutes
                    single_work_item_timeout_sec=60 * 10,  # 10 minutes
                    progress_bar_message="Updating table schemas...",
                )

                if update_tables_result.exceptions:
                    raise ExceptionGroup(
                        "Failed to update table schemas, encountered the following exceptions",
                        [
                            exception
                            for (
                                _task_kwargs,
                                exception,
                            ) in update_tables_result.exceptions
                        ],
                    )
            else:
                logging.info("Found no table updates to apply")

        if log_output:
            with open(log_file, "r", encoding="utf-8") as file:
                for line in file.readlines():
                    logging.info(line)
