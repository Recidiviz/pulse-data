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
from typing import Dict, Iterable, List, Optional, Sequence

import attr
from google.cloud import bigquery, exceptions

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_client import (
    BQ_CLIENT_MAX_POOL_SIZE,
    BigQueryClient,
    BigQueryClientImpl,
    BigQueryViewMaterializationResult,
)
from recidiviz.big_query.big_query_schema_utils import diff_declared_schema_to_bq_schema
from recidiviz.big_query.big_query_utils import are_bq_schemas_same
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalker,
    BigQueryViewDagWalkerProcessingFailureMode,
    ProcessDagResult,
)
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)
from recidiviz.big_query.big_query_view_update_sandbox_context import (
    BigQueryViewUpdateSandboxContext,
)
from recidiviz.big_query.big_query_view_utils import build_views_to_update
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


class CreateOrUpdateViewStatus(Enum):
    """Status of an attempt to create or update a single BigQueryView during the
    execution of create_managed_dataset_and_deploy_views_for_view_builders().
    """

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
    updated_view: bigquery.Table | None
    status: CreateOrUpdateViewStatus = attr.ib(
        validator=attr.validators.instance_of(CreateOrUpdateViewStatus)
    )
    materialization_result: BigQueryViewMaterializationResult | None = attr.ib(
        validator=attr_validators.is_opt(BigQueryViewMaterializationResult)
    )


def create_managed_dataset_and_deploy_views_for_view_builders(
    *,
    view_builders_to_update: Sequence[BigQueryViewBuilder],
    historically_managed_datasets_to_clean: set[str] | None,
    rematerialize_changed_views_only: bool,
    failure_mode: BigQueryViewDagWalkerProcessingFailureMode,
    view_update_sandbox_context: BigQueryViewUpdateSandboxContext | None = None,
    bq_region_override: str | None = None,
    default_table_expiration_for_new_datasets: int | None = None,
    views_might_exist: bool = True,
    allow_slow_views: bool = False,
) -> tuple[ProcessDagResult[CreateOrUpdateViewResult], BigQueryViewDagWalker]:
    """Creates or updates all the views in the provided list with the view query in the
    provided view builder list.

    Args:
        views_to_update (sequence[BigQueryViewBuilder]): A list of view builders to be
            created or updated.
        historically_managed_datasets_to_clean(set[str] | None): Set of datasets that have
            ever been managed if we should clean up unmanaged views in this deploy
            process. If null, does not perform the cleanup step. If provided,
            will error if any dataset required for the |views_to_update| is not
            included in this set.
        rematerialize_changed_views_only (bool): If true, only re-materialize views whose
            view and all of its ancestors views have not be updated; otherwise, always
            re-materialize all views.
        failure_mode (BigQueryViewDagWalkerProcessingFailureMode): If set to FAIL_FAST,
            will hard fail at the first node processing failure. If set to FAIL_EXHAUSTIVELY,
            will catch processing failures and proceed processing all non-descendants of
            the errant nodes, re-raising all caught errors after all possible nodes have
            been processed.
        view_update_sandbox_context (BigQueryViewUpdateSandboxContext | None): Sandbox
            context that provides a set of address overrides for a collection of views.
        bq_region_override (str | None): The bq region to be passed to the BigQueryClient.
        default_table_expiration_for_new_datasets (int | None): If not None, new datasets
            will be created with this default expiration length (in milliseconds).
        views_might_exist (bool): If set then we will optimistically try to update
            them, and fallback to creating the views if they do not exist.
        allow_slow_views (bool): If set then we will not fail view update if a view
            takes longer to update than is typically allowed.
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
            state_code_filter=view_update_sandbox_context.state_code_filter,
        )

    try:
        views_to_update = build_views_to_update(
            candidate_view_builders=view_builders_to_update,
            sandbox_context=sandbox_context,
        )

        return _create_managed_dataset_and_deploy_views(
            views_to_update=views_to_update,
            historically_managed_datasets_to_clean=historically_managed_datasets_to_clean,
            rematerialize_changed_views_only=rematerialize_changed_views_only,
            default_table_expiration_for_new_datasets=default_table_expiration_for_new_datasets,
            bq_region_override=bq_region_override,
            views_might_exist=views_might_exist,
            allow_slow_views=allow_slow_views,
            failure_mode=failure_mode,
            sandbox_context=sandbox_context,
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
    historically_managed_datasets_to_clean: set[str] | None,
    rematerialize_changed_views_only: bool,
    failure_mode: BigQueryViewDagWalkerProcessingFailureMode,
    bq_region_override: str | None,
    default_table_expiration_for_new_datasets: int | None,
    views_might_exist: bool,
    allow_slow_views: bool,
    sandbox_context: BigQueryViewSandboxContext | None,
) -> tuple[ProcessDagResult[CreateOrUpdateViewResult], BigQueryViewDagWalker]:
    """Creates or updates all the views in the provided list with the view query in the
    provided view list.

    Args:
        views_to_update (iterable[BigQueryView]): A list of view objects to be created
            or updated.
        historically_managed_datasets_to_clean(set[str] | None): Set of datasets that have
            ever been managed if we should clean up unmanaged views in this deploy
            process. If null, does not perform the cleanup step. If provided,
            will error if any dataset required for the |views_to_update| is not
            included in this set.
        rematerialize_changed_views_only (bool): If true, only re-materialize views whose
            view and all of its ancestors views have not be updated; otherwise, always
            re-materialize all views.
        failure_mode (BigQueryViewDagWalkerProcessingFailureMode): If set to FAIL_FAST,
            will hard fail at the first node processing failure. If set to FAIL_EXHAUSTIVELY,
            will catch processing failures and proceed processing all non-descendants of
            the errant nodes, re-raising all caught errors after all possible nodes have
            been processed.
        bq_region_override (str | None): The bq region to be passed to the BigQueryClient.
        default_table_expiration_for_new_datasets (int | None): If not None, new datasets
            will be created with this default expiration length (in milliseconds).
        views_might_exist (bool): If set then we will optimistically try to update
            them, and fallback to creating the views if they do not exist.
        allow_slow_views (bool): If set then we will not fail view update if a view
            takes longer to update than is typically allowed.
        sandbox_context (BigQueryViewSandboxContext | None): If provided,
            configures sandbox-specific behavior such as dataset prefix
            overrides and relaxed schema validation during materialization.
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
        """Attempts to create or update |v|, returning a CreateOrUpdateViewResult object
        that details the status of the attempt, as well as metadata about the view's
        materialization, if it was materialized.
        """
        return _create_or_update_view_and_materialize_if_necessary(
            bq_client=bq_client,
            view=v,
            parent_results=parent_results,
            rematerialize_changed_views_only=rematerialize_changed_views_only,
            might_exist=views_might_exist,
            sandbox_context=sandbox_context,
        )

    perf_config = (
        None if allow_slow_views else get_deployed_view_dag_update_perf_config()
    )
    results = dag_walker.process_dag(
        process_fn,
        synchronous=False,
        perf_config=perf_config,
        failure_mode=failure_mode,
    )
    results.log_processing_stats(n_slowest=NUM_SLOW_VIEWS_TO_LOG)

    return results, dag_walker


def _materialize_view_if_necessary(
    *,
    bq_client: BigQueryClient,
    view: BigQueryView,
    view_changed: bool,
    view_configuration_changed: bool,
    rematerialize_changed_views_only: bool,
    use_declared_schema: bool,
) -> BigQueryViewMaterializationResult | None:
    """Materializes the view if necessary. Raises a clean ValueError if
    BigQuery rejects the materialization due to a declared schema mismatch."""
    if not view.materialized_address:
        return None

    if (
        rematerialize_changed_views_only
        and not view_changed
        and bq_client.table_exists(view.materialized_address)
    ):
        logging.info(
            "Skipping materialization of view [%s.%s] which has not changed.",
            view.dataset_id,
            view.view_id,
        )
        return None

    return bq_client.materialize_view_to_table(
        view=view,
        use_query_cache=True,
        view_configuration_changed=view_configuration_changed,
        use_declared_schema=use_declared_schema,
    )


def _create_or_update_view_and_materialize_if_necessary(
    *,
    bq_client: BigQueryClient,
    view: BigQueryView,
    parent_results: Dict[BigQueryView, CreateOrUpdateViewResult],
    rematerialize_changed_views_only: bool,
    might_exist: bool,
    sandbox_context: BigQueryViewSandboxContext | None,
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
        # We also check for schema changes, just in case a parent view or table has
        # added a column
        or existing_view.schema != updated_view.schema
        or existing_materialized_table is None
        # Update the view if clustering fields have changed (clustering info stored on
        # the materialized table)
        or existing_materialized_table.clustering_fields != view.clustering_fields
        # Update the view if time partitioning configuration has changed (partitioning
        # info stored on the materialized table)
        or existing_materialized_table.time_partitioning != view.time_partitioning
        # If the view declares a schema, the materialized table schema may differ from
        # the deployed view schema because of mode metadata, but the table schema should
        # match the declared view schema exactly.
        # TODO(#54941): adjust these two clauses when all views have declared schemas
        or (
            view.bq_schema is not None
            and not are_bq_schemas_same(
                view.bq_schema, existing_materialized_table.schema
            )
        )
        # If the view has no declared schema, we expect that the table should be
        # materialized with the same schema as the view.
        or (
            view.bq_schema is None
            and not are_bq_schemas_same(
                updated_view.schema, existing_materialized_table.schema
            )
        )
    )

    view_changed = (
        existing_view is None
        # If the view query has changed, the view has changed
        or existing_view.view_query != updated_view.view_query
        or view_configuration_changed
        or parent_changed
    )

    # In a sandbox, avoid erroring out if declared schema doesn't match deployed schemas.
    materialize_with_declared_schema = view.schema is not None
    if (
        view.schema
        and sandbox_context
        and diff_declared_schema_to_bq_schema(
            view.schema,
            updated_view.schema,
        )
    ):

        logging.warning(
            "Not using declared schema to materialize [%s] because declared schema "
            "does not match view schema.",
            view.address,
        )
        materialize_with_declared_schema = False

    materialization_result = _materialize_view_if_necessary(
        bq_client=bq_client,
        view=view,
        view_changed=view_changed,
        view_configuration_changed=view_configuration_changed,
        rematerialize_changed_views_only=rematerialize_changed_views_only,
        use_declared_schema=materialize_with_declared_schema,
    )
    # View has changes if the view was updated or newly materialized
    has_changes = view_changed or materialization_result is not None

    update_status = (
        CreateOrUpdateViewStatus.SUCCESS_WITH_CHANGES
        if has_changes
        else CreateOrUpdateViewStatus.SUCCESS_WITHOUT_CHANGES
    )

    return CreateOrUpdateViewResult(
        view=view,
        updated_view=updated_view,
        status=update_status,
        materialization_result=materialization_result,
    )
