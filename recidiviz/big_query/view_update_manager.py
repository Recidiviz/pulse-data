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
from concurrent import futures
from enum import Enum
from typing import Dict, Iterable, List, Optional, Sequence, Set

import attr
from google.cloud import exceptions

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address_formatter import (
    BigQueryAddressFormatterProvider,
)
from recidiviz.big_query.big_query_client import (
    BQ_CLIENT_MAX_POOL_SIZE,
    BigQueryClient,
    BigQueryClientImpl,
    BigQueryViewMaterializationResult,
)
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalker,
    ProcessDagResult,
)
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)
from recidiviz.big_query.build_views_to_update import build_views_to_update
from recidiviz.big_query.constants import TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
from recidiviz.big_query.view_update_config import (
    get_deployed_view_dag_update_perf_config,
)
from recidiviz.big_query.view_update_manager_utils import (
    cleanup_datasets_and_delete_unmanaged_views,
    get_managed_view_and_materialized_table_addresses_by_dataset,
)
from recidiviz.common import attr_validators
from recidiviz.monitoring.instruments import get_monitoring_instrument
from recidiviz.monitoring.keys import CounterInstrumentKey
from recidiviz.utils import structured_logging
from recidiviz.view_registry.address_overrides_factory import (
    address_overrides_for_view_builders,
)

# We set this to 10 because urllib3 (used by the Google BigQuery client) has a default limit of 10 connections and
# we were seeing "urllib3.connectionpool:Connection pool is full, discarding connection" errors when this number
# increased.
# In the future, we could increase the worker number by playing around with increasing the pool size per this post:
# https://github.com/googleapis/python-storage/issues/253
MAX_WORKERS = 10

# The number of slowest-to-process views to print at the end of processing the full DAG.
NUM_SLOW_VIEWS_TO_LOG = 25


@attr.define(kw_only=True)
class BigQueryViewUpdateSandboxContext:
    """Object that provides a set of address overrides for a *collection of views* that
    will be loaded into a sandbox.
    """

    # Address overrides for any parent source tables views in this view update may
    # query. May be empty (BigQueryAddressOverrides.empty()) if the update should read
    # from only deployed source tables.
    input_source_table_overrides: BigQueryAddressOverrides = attr.ib(
        validator=attr.validators.instance_of(BigQueryAddressOverrides)
    )

    # The prefix to append to the output dataset for any views loaded by this view
    # update.
    output_sandbox_dataset_prefix: str = attr.ib(validator=attr_validators.is_str)

    # If given, this will give us a formatter that can be used to apply additional
    #  formatting on each parent address of views loaded by this view update.
    parent_address_formatter_provider: BigQueryAddressFormatterProvider | None = (
        attr.ib(validator=attr_validators.is_opt(BigQueryAddressFormatterProvider))
    )


class CreateOrUpdateViewStatus(Enum):
    # Returned for a given view if this view was not deployed (e.g. because it cannot
    # be deployed to this project or because a parent couldn't be deployed)
    SKIPPED = "SKIPPED"
    # Returned if neither this view or any of the views in its parent chain were updated
    SUCCESS_WITHOUT_CHANGES = "SUCCESS_WITHOUT_CHANGES"
    # Returned if this view or any views in its parent chain have been updated from the
    # version that was saved in BigQuery before this update.
    SUCCESS_WITH_CHANGES = "SUCCESS_WITH_CHANGES"


@attr.define(kw_only=True)
class CreateOrUpdateViewResult:
    """Object with info about a single view that was created or updated via
    create_managed_dataset_and_deploy_views_for_view_builders().
    """

    view: BigQueryView = attr.ib(validator=attr.validators.instance_of(BigQueryView))
    status: CreateOrUpdateViewStatus = attr.ib(
        validator=attr.validators.instance_of(CreateOrUpdateViewStatus)
    )
    materialization_result: BigQueryViewMaterializationResult | None = attr.ib(
        validator=attr_validators.is_opt(BigQueryViewMaterializationResult)
    )


def create_managed_dataset_and_deploy_views_for_view_builders(
    *,
    view_source_table_datasets: Set[str],
    view_builders_to_update: Sequence[BigQueryViewBuilder],
    historically_managed_datasets_to_clean: Optional[Set[str]],
    materialize_changed_views_only: bool,
    view_update_sandbox_context: BigQueryViewUpdateSandboxContext | None = None,
    bq_region_override: Optional[str] = None,
    default_table_expiration_for_new_datasets: Optional[int] = None,
    views_might_exist: bool = True,
    allow_slow_views: bool = False,
) -> tuple[ProcessDagResult[CreateOrUpdateViewResult], BigQueryViewDagWalker]:
    """Creates or updates all the views in the provided list with the view query in the
    provided view builder list. The view will be re-materialized if it is configured
    with a materialized address unless |materialize_changed_views_only| is True and the
    view and all of its ancestor views have not been updated.

    If a `historically_managed_datasets_to_clean` set is provided,
    then cleans up unmanaged views and datasets by deleting them from BigQuery.

    If `views_might_exist` is set then we will optimistically try to update
    them, and fallback to creating the views if they do not exist.

    Should only be called if we expect the views to have changed (either the view query
    or schema from querying underlying tables), e.g. at deploy time.
    """
    if (
        default_table_expiration_for_new_datasets is None
        and view_update_sandbox_context
    ):
        default_table_expiration_for_new_datasets = (
            TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
        )
        logging.info(
            "Setting default table expiration for sandbox view load. New datasets "
            "created in this process will have a default table expiration of 24 hours."
        )

    sandbox_context = None
    if view_update_sandbox_context:
        view_address_overrides = address_overrides_for_view_builders(
            view_dataset_override_prefix=view_update_sandbox_context.output_sandbox_dataset_prefix,
            view_builders=view_builders_to_update,
        )

        merged_overrides = BigQueryAddressOverrides.merge(
            view_update_sandbox_context.input_source_table_overrides,
            view_address_overrides,
        )

        sandbox_context = BigQueryViewSandboxContext(
            output_sandbox_dataset_prefix=view_update_sandbox_context.output_sandbox_dataset_prefix,
            parent_address_formatter_provider=view_update_sandbox_context.parent_address_formatter_provider,
            parent_address_overrides=merged_overrides,
        )

    try:
        views_to_update = build_views_to_update(
            view_source_table_datasets=view_source_table_datasets,
            candidate_view_builders=view_builders_to_update,
            sandbox_context=sandbox_context,
        )

        return _create_managed_dataset_and_deploy_views(
            views_to_update=views_to_update,
            bq_region_override=bq_region_override,
            materialize_changed_views_only=materialize_changed_views_only,
            historically_managed_datasets_to_clean=historically_managed_datasets_to_clean,
            default_table_expiration_for_new_datasets=default_table_expiration_for_new_datasets,
            views_might_exist=views_might_exist,
            allow_slow_views=allow_slow_views,
        )
    except Exception as e:
        get_monitoring_instrument(CounterInstrumentKey.VIEW_UPDATE_FAILURE).add(
            amount=1
        )

        raise e


def _create_all_datasets_if_necessary(
    bq_client: BigQueryClient,
    dataset_ids: List[str],
    dataset_table_expiration: Optional[int],
) -> None:
    """Creates all required datasets for the list of dataset ids,
    with a table timeout if necessary. Done up front to avoid conflicts during a run of the DagWalker.
    """

    def create_dataset(dataset_id: str) -> None:
        bq_client.create_dataset_if_necessary(
            dataset_id, default_table_expiration_ms=dataset_table_expiration
        )

    with futures.ThreadPoolExecutor(
        # Conservatively allow only half as many workers as allowed connections.
        # Lower this number if we see "urllib3.connectionpool:Connection pool is
        # full, discarding connection" errors.
        max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
    ) as executor:
        create_dataset_futures = {
            executor.submit(
                structured_logging.with_context(create_dataset),
                dataset_id,
            )
            for dataset_id in dataset_ids
        }
        for future in futures.as_completed(create_dataset_futures):
            future.result()


def _create_managed_dataset_and_deploy_views(
    *,
    views_to_update: Iterable[BigQueryView],
    bq_region_override: Optional[str],
    materialize_changed_views_only: bool,
    historically_managed_datasets_to_clean: Optional[Set[str]] = None,
    default_table_expiration_for_new_datasets: Optional[int] = None,
    views_might_exist: bool = True,
    allow_slow_views: bool = False,
) -> tuple[ProcessDagResult[CreateOrUpdateViewResult], BigQueryViewDagWalker]:
    """Create and update the given views and their parent datasets. Cleans up unmanaged views and datasets

    For each dataset key in the given dictionary, creates  the dataset if it does not
    exist, and creates or updates the underlying views mapped to that dataset.

    If a view has a set materialized_address field, materializes the view into a
    table.

    Then, cleans up BigQuery by deleting unmanaged datasets and unmanaged views within managed datasets. This is not
    performed if a temporary dataset table expiration is already set.

    Args:
        views_to_update: A list of view objects to be created or updated.
        default_table_expiration_for_new_datasets: If not None, new datasets will
            be created with this default expiration length (in milliseconds).
        historically_managed_datasets_to_clean: Set of datasets that have
            ever been managed if we should clean up unmanaged views in this deploy
            process. If null, does not perform the cleanup step. If provided,
            will error if any dataset required for the |views_to_update| is not
            included in this set.
        views_might_exist: If set then we will optimistically try to update
            them, and fallback to creating the views if they do not exist.
        allow_slow_views: If set then we will not fail view update if a view
            takes longer to update than is typically allowed.
    """
    bq_client = BigQueryClientImpl(region_override=bq_region_override)
    dag_walker = BigQueryViewDagWalker(views_to_update)

    managed_views_map = get_managed_view_and_materialized_table_addresses_by_dataset(
        dag_walker
    )
    managed_dataset_ids = list(managed_views_map.keys())
    _create_all_datasets_if_necessary(
        bq_client, managed_dataset_ids, default_table_expiration_for_new_datasets
    )

    if (
        historically_managed_datasets_to_clean
        # We don't want to delete unmanaged views/tables if we're creating sandbox datasets
        and default_table_expiration_for_new_datasets is None
    ):
        cleanup_datasets_and_delete_unmanaged_views(
            bq_client,
            managed_views_map,
            datasets_that_have_ever_been_managed=historically_managed_datasets_to_clean,
            dry_run=False,
        )

    def process_fn(
        v: BigQueryView, parent_results: Dict[BigQueryView, CreateOrUpdateViewResult]
    ) -> CreateOrUpdateViewResult:
        """Returns True if this view or any of its parents were updated."""
        try:
            return _create_or_update_view_and_materialize_if_necessary(
                bq_client=bq_client,
                view=v,
                parent_results=parent_results,
                materialize_changed_views_only=materialize_changed_views_only,
                might_exist=views_might_exist,
            )
        except Exception as e:
            raise ValueError(f"Error creating or updating view [{v.address}]") from e

    perf_config = (
        None if allow_slow_views else get_deployed_view_dag_update_perf_config()
    )
    results = dag_walker.process_dag(
        process_fn,
        synchronous=False,
        perf_config=perf_config,
    )
    results.log_processing_stats(n_slowest=NUM_SLOW_VIEWS_TO_LOG)

    return results, dag_walker


def _create_or_update_view_and_materialize_if_necessary(
    *,
    bq_client: BigQueryClient,
    view: BigQueryView,
    parent_results: Dict[BigQueryView, CreateOrUpdateViewResult],
    materialize_changed_views_only: bool,
    might_exist: bool,
) -> CreateOrUpdateViewResult:
    """Creates or updates the provided view in BigQuery and materializes that view into
    a table when appropriate. Returns a CreateOrUpdateViewResult object containing
    metadata about the update.
    """
    parent_statuses = {
        parent_result.status for parent_result in parent_results.values()
    }
    parent_changed = CreateOrUpdateViewStatus.SUCCESS_WITH_CHANGES in parent_statuses

    existing_view = None
    if might_exist:
        try:
            existing_view = bq_client.get_table(view.address)
        except exceptions.NotFound:
            pass

    existing_materialized_table = None
    if might_exist and view.materialized_address:
        try:
            existing_materialized_table = bq_client.get_table(view.materialized_address)
        except exceptions.NotFound:
            pass

    # TODO(https://issuetracker.google.com/issues/180636362): Currently we have to
    # delete and recreate the view for changes from underlying tables to be reflected in
    # its schema.
    # TODO(#30446): Once `parent_changed` reflects changes to the schemas of source
    # tables as well, only delete this `if existing_view is not None and parent_changed`.
    if existing_view is not None:
        # If there is a network blip and this delete retries, we still want it to
        # succeed so as to not halt the view update, so we set `not_found_ok=True`. If
        # for some reason someone else deleted it out from under us, it is also okay to
        # just proceed with the view update.
        bq_client.delete_table(view.address, not_found_ok=True)

    updated_view = bq_client.create_or_update_view(view, might_exist=might_exist)

    view_configuration_changed = (
        existing_view is None
        # Update the view if clustering fields have changed
        or existing_view.clustering_fields != updated_view.clustering_fields
        # Update the view if time partitioning configuration has changed
        or existing_view.time_partitioning != updated_view.time_partitioning
        # We also check for schema changes, just in case a parent view or table has
        # added a column
        or existing_view.schema != updated_view.schema
        or existing_materialized_table is None
        # If the updated view's schema doesn't match the materialized table's data,
        # we're also in a state where schema changes haven't been fully reflected.
        or updated_view.schema != existing_materialized_table.schema
    )

    view_changed = (
        existing_view is None
        # If the view query has changed, the view has changed
        or existing_view.view_query != updated_view.view_query
        or view_configuration_changed
        or parent_changed
    )

    materialization_result = None
    if view.materialized_address:
        if (
            not materialize_changed_views_only
            or view_changed
            or not bq_client.table_exists(view.materialized_address)
        ):
            materialization_result = bq_client.materialize_view_to_table(
                view=view,
                use_query_cache=True,
                view_configuration_changed=view_configuration_changed,
            )
        else:
            logging.info(
                "Skipping materialization of view [%s.%s] which has not changed.",
                view.dataset_id,
                view.view_id,
            )
    # View has changes if the view was updated or newly materialized
    has_changes = view_changed or materialization_result is not None

    update_status = (
        CreateOrUpdateViewStatus.SUCCESS_WITH_CHANGES
        if has_changes
        else CreateOrUpdateViewStatus.SUCCESS_WITHOUT_CHANGES
    )

    return CreateOrUpdateViewResult(
        view=view, status=update_status, materialization_result=materialization_result
    )
