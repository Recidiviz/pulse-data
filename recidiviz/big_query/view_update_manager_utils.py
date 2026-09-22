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

import datetime
import logging
from typing import Dict, List, Sequence, Set

from google.cloud import bigquery

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
from recidiviz.utils import metadata
from recidiviz.view_registry.deployed_source_table_repository import (
    get_externally_hydrated_source_table_datasets,
)

MIN_AGE_TO_DELETE_UNMANAGED_RESOURCES = datetime.timedelta(hours=12)
"""Tables and datasets created more recently than this are never deleted by cleanup.
Cleanup runs with the managed map of the code version it was started with, so a DAG
on newer code may materialize views into a managed dataset that cleanup does not
yet know about. This must exceed the longest DAG run.
"""


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


def _is_too_new_to_delete(
    *, created: datetime.datetime | None, resource_description: str
) -> bool:
    """Returns True if the resource was created within
    MIN_AGE_TO_DELETE_UNMANAGED_RESOURCES, raising if its creation time is unknown.
    """
    if created is None:
        raise ValueError(
            f"Cannot determine the creation time of {resource_description}, so "
            f"cannot safely decide whether to delete it."
        )
    age = datetime.datetime.now(tz=datetime.timezone.utc) - created
    return age < MIN_AGE_TO_DELETE_UNMANAGED_RESOURCES


def delete_unmanaged_views_and_tables_from_dataset(
    bq_client: BigQueryClient,
    dataset_id: str,
    managed_tables: Set[BigQueryAddress],
    dry_run: bool,
) -> Set[BigQueryAddress]:
    """Deletes every view or table in |dataset_id| that is not in |managed_tables|,
    skipping any created within MIN_AGE_TO_DELETE_UNMANAGED_RESOURCES. Returns the
    addresses that were deleted (or would be deleted, in a dry run). Logs and returns
    an empty set if the dataset does not exist.
    """
    if not bq_client.dataset_exists(dataset_id):
        logging.info(
            "Managed dataset [%s] does not exist in BigQuery yet. Skipping cleanup.",
            dataset_id,
        )
        return set()

    unmanaged_views_and_tables: Set[BigQueryAddress] = set()
    for table in list(bq_client.list_tables(dataset_id)):
        table_bq_address = BigQueryAddress.from_table(table)
        if table_bq_address in managed_tables:
            continue
        if _is_too_new_to_delete(
            created=table.created,
            resource_description=f"table [{table_bq_address.to_str()}]",
        ):
            logging.info(
                "Skipping unmanaged table/view %s created within the last %s.",
                table_bq_address.to_str(),
                MIN_AGE_TO_DELETE_UNMANAGED_RESOURCES,
            )
            continue
        unmanaged_views_and_tables.add(table_bq_address)

    for view_address in unmanaged_views_and_tables:
        if dry_run:
            logging.info(
                "[DRY RUN] Regular run would delete unmanaged table/view %s.",
                view_address.to_str(),
            )
            continue
        logging.info("Deleting unmanaged table/view %s.", view_address.to_str())
        bq_client.delete_table(view_address)
    return unmanaged_views_and_tables


def _delete_unmanaged_dataset(
    bq_client: BigQueryClient, dataset_id: str, dry_run: bool
) -> None:
    """Deletes |dataset_id| and its contents unless it was created within
    MIN_AGE_TO_DELETE_UNMANAGED_RESOURCES.
    """
    dataset: bigquery.Dataset = bq_client.get_dataset(dataset_id)
    if _is_too_new_to_delete(
        created=dataset.created, resource_description=f"dataset [{dataset_id}]"
    ):
        logging.info(
            "Skipping unmanaged dataset %s created within the last %s.",
            dataset_id,
            MIN_AGE_TO_DELETE_UNMANAGED_RESOURCES,
        )
        return
    if dry_run:
        logging.info(
            "[DRY RUN] Regular run would delete unmanaged dataset %s.", dataset_id
        )
        return
    logging.info("Deleting dataset %s, which is no longer managed.", dataset_id)
    bq_client.delete_dataset(dataset_id, delete_contents=True)


def cleanup_datasets_and_delete_unmanaged_views(
    bq_client: BigQueryClient,
    managed_views_map: Dict[str, Set[BigQueryAddress]],
    datasets_that_have_ever_been_managed: Set[str],
    dry_run: bool = True,
) -> None:
    """Deletes every dataset in |datasets_that_have_ever_been_managed| that is no
    longer in |managed_views_map|, and every unmanaged view or table within the
    datasets that are. Raises if a managed dataset is missing from
    |datasets_that_have_ever_been_managed|. Tables and datasets created within
    MIN_AGE_TO_DELETE_UNMANAGED_RESOURCES are never deleted.
    """
    managed_dataset_ids: List[str] = list(managed_views_map.keys())

    for dataset_id in managed_dataset_ids:
        if dataset_id not in datasets_that_have_ever_been_managed:
            raise ValueError(
                f"Managed dataset [{dataset_id}] not found in the provided "
                "|datasets_that_have_ever_been_managed|: "
                f"[{datasets_that_have_ever_been_managed}]."
            )

    for dataset_id in datasets_that_have_ever_been_managed:
        if dataset_id in managed_views_map:
            delete_unmanaged_views_and_tables_from_dataset(
                bq_client, dataset_id, managed_views_map[dataset_id], dry_run
            )
            continue
        if not bq_client.dataset_exists(dataset_id):
            logging.info(
                "Dataset %s isn't being managed and no longer exists in BigQuery. "
                "It can be safely removed from the list: [%s].",
                dataset_id,
                datasets_that_have_ever_been_managed,
            )
            continue
        _delete_unmanaged_dataset(bq_client, dataset_id, dry_run)


def validate_builders_not_in_current_source_datasets(
    view_builders: Sequence[BigQueryViewBuilder],
    sandbox_context: (
        BigQueryViewSandboxContext | BigQueryViewUpdateSandboxContext | None
    ),
) -> None:
    """Validates that no |view_builders| have an overlapping dataset name with the
    defined source table repository for the current project.
    """
    source_table_datasets = get_externally_hydrated_source_table_datasets(
        metadata.project_id()
    )
    validate_builders_not_in_source_datasets(
        source_table_datasets, view_builders, sandbox_context=sandbox_context
    )


def validate_builders_not_in_source_datasets(
    source_table_datasets: set[str],
    view_builders: Sequence[BigQueryViewBuilder],
    sandbox_context: (
        BigQueryViewSandboxContext | BigQueryViewUpdateSandboxContext | None
    ),
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
