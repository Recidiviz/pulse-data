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
"""Provides utilities for updating views within a live BigQuery instance."""

import logging
from typing import Dict, List, Sequence, Set

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)
from recidiviz.big_query.big_query_view_update_sandbox_context import (
    BigQueryViewUpdateSandboxContext,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_source_table_datasets,
)
from recidiviz.utils import metadata


def get_managed_view_and_materialized_table_addresses_by_dataset(
    managed_views_dag_walker: BigQueryViewDagWalker,
) -> Dict[str, Set[BigQueryAddress]]:
    """Creates a dictionary mapping every managed dataset in BigQuery to the set
    of managed views in the dataset. Returned dictionary's key is a dataset_id and
    each key's value is a set of all the BigQueryAddress's that are from the referenced dataset.
    """
    managed_views_for_dataset_map: Dict[str, Set[BigQueryAddress]] = {}
    for view in managed_views_dag_walker.views:
        managed_views_for_dataset_map.setdefault(view.address.dataset_id, set()).add(
            view.address
        )
        if view.materialized_address:
            managed_views_for_dataset_map.setdefault(
                view.materialized_address.dataset_id, set()
            ).add(view.materialized_address)
    return managed_views_for_dataset_map


def delete_unmanaged_views_and_tables_from_dataset(
    bq_client: BigQueryClient,
    dataset_id: str,
    managed_tables: Set[BigQueryAddress],
    dry_run: bool,
) -> Set[BigQueryAddress]:
    """This function takes in a set of managed views/tables and compares it to the list of
    tables BigQuery has. The function then deletes any views/tables that are in BigQuery but not
    in the set of managed views/tables. It then returns a set of the BigQueryAddress's
    from these unmanaged views/tables that are to be deleted."""
    unmanaged_views_and_tables: Set[BigQueryAddress] = set()
    if not bq_client.dataset_exists(dataset_id):
        raise ValueError(f"Dataset {dataset_id} does not exist in BigQuery")
    for table in list(bq_client.list_tables(dataset_id)):
        table_bq_address = BigQueryAddress.from_table(table)
        if table_bq_address not in managed_tables:
            unmanaged_views_and_tables.add(table_bq_address)
    for view_address in unmanaged_views_and_tables:
        if dry_run:
            logging.info(
                "[DRY RUN] Regular run would delete unmanaged table/view %s.",
                view_address.to_str(),
            )

        else:
            logging.info(
                "Deleting unmanaged table/view %s.",
                view_address.to_str(),
            )

            bq_client.delete_table(view_address)
    return unmanaged_views_and_tables


def cleanup_datasets_and_delete_unmanaged_views(
    bq_client: BigQueryClient,
    managed_views_map: Dict[str, Set[BigQueryAddress]],
    datasets_that_have_ever_been_managed: Set[str],
    dry_run: bool = True,
) -> None:
    """This function filters through a list of managed dataset ids and a map of managed
    views to their corresponding datasets and checks that the dataset is in the provided
    primary list |datasets_that_have_ever_been_managed|. It then cleans up the
    datasets by deleting unmanaged datasets and deleting any unmanaged views within
    managed datasets."""
    managed_dataset_ids: List[str] = list(managed_views_map.keys())

    for dataset_id in managed_dataset_ids:
        if dataset_id not in datasets_that_have_ever_been_managed:
            raise ValueError(
                "Managed dataset %s not found in the provided "
                "|datasets_that_have_ever_been_managed|: "
                f"[{datasets_that_have_ever_been_managed}]." % dataset_id,
            )

    for dataset_id in datasets_that_have_ever_been_managed:
        if dataset_id not in managed_views_map:
            if bq_client.dataset_exists(dataset_id):
                if dry_run:
                    logging.info(
                        "[DRY RUN] Regular run would delete unmanaged dataset %s.",
                        dataset_id,
                    )
                else:
                    logging.info(
                        "Deleting dataset %s, which is no longer managed.",
                        dataset_id,
                    )
                    bq_client.delete_dataset(dataset_id, delete_contents=True)
            else:
                logging.info(
                    "Dataset %s isn't being managed and no longer exists in BigQuery. "
                    "It can be safely removed from the list: [%s].",
                    dataset_id,
                    datasets_that_have_ever_been_managed,
                )

        else:
            delete_unmanaged_views_and_tables_from_dataset(
                bq_client, dataset_id, managed_views_map[dataset_id], dry_run
            )


def validate_builders_not_in_current_source_datasets(
    view_builders: Sequence[BigQueryViewBuilder],
    sandbox_context: BigQueryViewSandboxContext
    | BigQueryViewUpdateSandboxContext
    | None,
) -> None:
    """Validates that no |view_builders| have an overlapping dataset name with the
    defined source table repository for the current project.
    """
    source_table_datasets = get_source_table_datasets(metadata.project_id())
    validate_builders_not_in_source_datasets(
        source_table_datasets, view_builders, sandbox_context=sandbox_context
    )


def validate_builders_not_in_source_datasets(
    source_table_datasets: set[str],
    view_builders: Sequence[BigQueryViewBuilder],
    sandbox_context: BigQueryViewSandboxContext
    | BigQueryViewUpdateSandboxContext
    | None,
) -> None:
    """Validates that |view_builders| have no overlapping dataset names with
    |source_table_datasets|, throwing if any do.
    """
    overlapping_errors: list = []
    for view_builder in view_builders:
        dataset_id = (
            view_builder.dataset_id
            if not sandbox_context
            else BigQueryAddressOverrides.format_sandbox_dataset(
                sandbox_context.output_sandbox_dataset_prefix, view_builder.dataset_id
            )
        )
        if dataset_id in source_table_datasets:
            overlapping_errors.append(
                ValueError(
                    f"Found view [{view_builder.view_id}] in source-table-only dataset "
                    f"[{dataset_id}]"
                )
            )
            # raise at most one error per view
            continue

        if view_builder.materialized_address:
            materialized_dataset_id = (
                view_builder.materialized_address.dataset_id
                if not sandbox_context
                else BigQueryAddressOverrides.format_sandbox_dataset(
                    sandbox_context.output_sandbox_dataset_prefix,
                    view_builder.materialized_address.dataset_id,
                )
            )
            if materialized_dataset_id in source_table_datasets:
                overlapping_errors.append(
                    ValueError(
                        f"Found view with materialization [{view_builder.materialized_address.table_id}] in source-table-only dataset "
                        f"[{materialized_dataset_id}]"
                    )
                )

    if overlapping_errors:
        raise ExceptionGroup(
            "Found the following views in source table-only datasets:",
            overlapping_errors,
        )
