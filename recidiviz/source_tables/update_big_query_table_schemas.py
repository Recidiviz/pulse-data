# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""
Module responsible for updating BigQuery source table schemata.
This is primarily called via the `update_big_query_table_schemata` Airflow calculation
DAG task, but the dry run functionality may be called via the
recidiviz.tools.deploy.source_tables.check_source_table_schemas script.
"""
import logging
import os
import textwrap
from enum import Enum

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.source_tables.source_table_config import (
    SourceTableCollectionUpdateConfig,
)
from recidiviz.source_tables.source_table_repository import SourceTableRepository
from recidiviz.source_tables.source_table_update_manager import (
    SourceTableUpdateManager,
    SourceTableUpdateType,
    SourceTableWithRequiredUpdateTypes,
)
from recidiviz.tools.deploy.logging import get_deploy_logs_dir
from recidiviz.utils.string_formatting import fix_indent


class SourceTableCheckType(Enum):
    """Describes which collection of source tables to run a schema check against."""

    # Check for changes only in externally managed source tables
    EXTERNALLY_MANAGED = "externally_managed"
    # Check for changes only in managed, protected source tables
    PROTECTED = "protected"
    # Check for changes only in managed, regenerable source tables
    REGENERABLE = "regenerable"
    # Check for changes in all known source tables
    ALL = "all"

    def matches_update_config(
        self, update_config: SourceTableCollectionUpdateConfig
    ) -> bool:
        """Returns true if a source table with the given config should be checked when
        running a check with this type.
        """
        if self is SourceTableCheckType.ALL:
            return True
        if self is SourceTableCheckType.PROTECTED:
            return update_config == SourceTableCollectionUpdateConfig.protected()
        if self is SourceTableCheckType.REGENERABLE:
            return update_config == SourceTableCollectionUpdateConfig.regenerable()
        if self is SourceTableCheckType.EXTERNALLY_MANAGED:
            return (
                update_config == SourceTableCollectionUpdateConfig.externally_managed()
            )

        raise ValueError(
            f"Unexpected source table check type: [{SourceTableCheckType}]"
        )


def _build_table_update_results_str(
    results: dict[BigQueryAddress, SourceTableWithRequiredUpdateTypes],
) -> str:
    """Builds a helpful printout enumerating the differences found in this set of dry
    run results.
    """
    table_strs = []
    for address in sorted(results.keys(), key=lambda a: a.to_str()):
        result = results[address]
        if not result.has_updates_to_make:
            continue

        table_strs.append(fix_indent(result.build_updates_message(), indent_level=2))
    return "\n" + "\n".join(table_strs) + "\n"


def check_source_table_schemas(
    source_table_repository: SourceTableRepository,
    source_table_check_type: SourceTableCheckType,
) -> None:
    """Checks for table updates that would be necessary to make the schemas of tables
    deployed to BQ match the schemas of tables defined in the |source_table_repository|.
    Only checks tables that match the provided |source_table_check_type|.

    Raise a ValueError if any change is found that can't be applied safely by our
    standard source table update process.
    """

    logging.info(
        "Checking [%s] source tables for differences between what is deployed and ",
        source_table_check_type.name,
    )

    source_table_collections = [
        c
        for c in source_table_repository.source_table_collections
        if source_table_check_type.matches_update_config(c.update_config)
    ]

    address_to_update_config = {
        st.address: c.update_config
        for c in source_table_collections
        for st in c.source_tables
    }

    update_manager = SourceTableUpdateManager(client=BigQueryClientImpl())
    log_file = os.path.join(
        get_deploy_logs_dir(),
        f"check_source_table_schemas_{source_table_check_type.value}.log",
    )
    changes = update_manager.get_changes_to_apply_to_source_tables(
        source_table_collections=source_table_collections, log_file=log_file
    )
    if not changes:
        logging.info("✅ Dry run found no changes to be made.")
        return

    unsafe_managed_table_changes: dict[
        BigQueryAddress, SourceTableWithRequiredUpdateTypes
    ] = {}
    unsafe_unmanaged_table_changes: dict[
        BigQueryAddress, SourceTableWithRequiredUpdateTypes
    ] = {}
    safe_changes: dict[BigQueryAddress, SourceTableWithRequiredUpdateTypes] = {}

    for address, dry_run_result in changes.items():
        update_config = address_to_update_config[address]

        if dry_run_result.are_changes_safe_to_apply_to_collection(update_config):
            safe_changes[address] = dry_run_result
        elif update_config.attempt_to_manage:
            unsafe_managed_table_changes[address] = dry_run_result
        else:
            unsafe_unmanaged_table_changes[address] = dry_run_result

    # TODO(#41360): Remove this check once all external table have been migrated to
    #  yaml_managed. For now, we allow disagreement between deployed external data
    #  config and the config in YAML.
    unsafe_unmanaged_table_changes = {
        address: result
        for address, result in unsafe_unmanaged_table_changes.items()
        if result.all_update_types
        != {SourceTableUpdateType.UPDATE_EXTERNAL_DATA_CONFIGURATION}
    }

    messages = []
    if unsafe_unmanaged_table_changes:
        messages.append(
            textwrap.fill(
                "\n‼️Found the following externally managed tables with schemas "
                "defined in its YAML config in "
                "recidiviz/source_tables/externally_managed that do not match the "
                "schema of the tables defined in BigQuery. These tables are "
                "not managed by our standard source table update process. If the YAML "
                "schema is incorrect, you will need to update it. If the YAML schema "
                "is correct, you will need to update the process that generates this "
                "table to agree (or manually update the table if there is no automatic "
                "process that generates it). Tables with changes:",
                width=88,
            )
        )
        messages.append(_build_table_update_results_str(unsafe_unmanaged_table_changes))

    if unsafe_managed_table_changes:
        messages.append(
            textwrap.fill(
                "‼️Found the following managed tables with schemas derived from "
                "current code which have changes that are NOT SAFE to apply "
                "automatically. If the new schema is correct, please manually update "
                "the table to drop/rename the appropriate columns via DML statements "
                "in the BigQuery UI (if you're unsure how to do this, ask in "
                "#platform-team). In order to avoid breaking our currently deployed "
                "DAG, any manual changes should be applied right before the deploy "
                "containing the relevant code changes that impacted the schema. If you "
                "run any manual commands before the staging deploy, do not forget to "
                "add the analogous commands to the next staging AND prod Pre-Deploy "
                "Tasks checklists at https://go/platform-deploy-log/. Tables with "
                "unsafe changes:",
                width=88,
            )
        )
        messages.append(_build_table_update_results_str(unsafe_managed_table_changes))

    if safe_changes:
        messages.append(
            textwrap.fill(
                "✅ Found the following managed source table changes which have schema "
                "changes that can be safely applied automatically via our "
                "update_big_query_table_schemata step. Tables with changes:",
                width=88,
            )
        )
        messages.append(_build_table_update_results_str(safe_changes))

    msg = "\n".join(messages)
    if unsafe_unmanaged_table_changes or unsafe_managed_table_changes:
        raise ValueError(
            f"Found source table schema changes that are not safe to apply "
            f"automatically. All changes found:\n\n{msg}"
        )
    print(msg)


def update_all_managed_source_table_schemas(
    source_table_repository: SourceTableRepository,
    update_manager: SourceTableUpdateManager | None = None,
) -> None:
    """Updates the schemas of all managed source tables in the given
    |source_table_repository| to match what is defined in their respective configs.

    Throws if any update cannot be applied safely without risking data loss.
    """
    if not update_manager:
        update_manager = SourceTableUpdateManager(client=BigQueryClientImpl())

    source_table_collections = [
        source_table_collection
        for source_table_collection in source_table_repository.source_table_collections
        if source_table_collection.update_config.attempt_to_manage
    ]

    update_manager.update_async(
        source_table_collections=source_table_collections,
        log_file=os.path.join(
            get_deploy_logs_dir(), "update_all_source_table_schemas.log"
        ),
        log_output=True,
    )
