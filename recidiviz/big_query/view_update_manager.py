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
from http import HTTPStatus
from typing import Dict, List, Optional, Sequence, Set, Tuple

from flask import Blueprint
from google.cloud import exceptions
from opencensus.stats import aggregation, measure
from opencensus.stats import view as opencensus_view

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.big_query.big_query_view import (
    BigQueryView,
    BigQueryViewBuilder,
    BigQueryViewBuilderShouldNotBuildError,
)
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.view_update_manager_utils import (
    cleanup_datasets_and_delete_unmanaged_views,
    get_managed_view_and_materialized_table_addresses_by_dataset,
)
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.utils import metadata, monitoring, structured_logging
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.view_registry.datasets import VIEW_SOURCE_TABLE_DATASETS
from recidiviz.view_registry.deployed_views import (
    DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED,
    deployed_view_builders,
)

m_failed_view_update = measure.MeasureInt(
    "bigquery/view_update_manager/view_update_all_failure",
    "Counted every time updating all views fails",
    "1",
)

failed_view_updates_view = opencensus_view.View(
    "bigquery/view_update_manager/num_view_update_failure",
    "The sum of times all views fail to update",
    [monitoring.TagKey.CREATE_UPDATE_VIEWS_NAMESPACE],
    m_failed_view_update,
    aggregation.SumAggregation(),
)

monitoring.register_views([failed_view_updates_view])

# When creating temporary datasets with prefixed names, set the default table expiration to 24 hours
TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS = 24 * 60 * 60 * 1000

# We set this to 10 because urllib3 (used by the Google BigQuery client) has a default limit of 10 connections and
# we were seeing "urllib3.connectionpool:Connection pool is full, discarding connection" errors when this number
# increased.
# In the future, we could increase the worker number by playing around with increasing the pool size per this post:
# https://github.com/googleapis/python-storage/issues/253
MAX_WORKERS = 10

view_update_manager_blueprint = Blueprint("view_update", __name__)


# TODO(#11437): We are aware that this endpoint will regularly time out, since updating
#  all views takes a long time. This endpoint is a **temporary** solution,
#  and will be deleted once we put the BigQuery view update into the DAG.
@view_update_manager_blueprint.route("/update_all_managed_views")
@requires_gae_auth
def update_all_managed_views() -> Tuple[str, HTTPStatus]:
    """API endpoint to update all managed views."""
    create_managed_dataset_and_deploy_views_for_view_builders(
        view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
        view_builders_to_update=deployed_view_builders(metadata.project_id()),
        historically_managed_datasets_to_clean=DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED,
    )

    logging.info("All managed views successfully updated and materialized.")

    return "", HTTPStatus.OK


def rematerialize_views_for_view_builders(
    views_to_update_builders: Sequence[BigQueryViewBuilder],
    all_view_builders: Sequence[BigQueryViewBuilder],
    view_source_table_datasets: Set[str],
    address_overrides: Optional[BigQueryAddressOverrides] = None,
    skip_missing_views: bool = False,
) -> None:
    """For all views corresponding to the builders in |views_to_update_builders| list,
    re-materializes any materialized views. This should be called only when we want to
    refresh the data in the materialized view(s), not when we want to update the
    underlying query of the view(s).

    Args:
        views_to_update_builders: List of views to re-materialize
        all_view_builders: Superset of the views_to_update that contains all views that
            either depend on or are dependents of the list of input views.
        view_source_table_datasets: Set of datasets containing tables that can be
            treated as root nodes in the view dependency graph.
        address_overrides: A class that provides a mapping of table/view addresses
            and the address they should be replaced with for the given list of
            views_to_update.
        skip_missing_views: If True, ignores any input views that do not exist. If
            False, crashes if tries to materialize a view that does not exist.
    """
    views_to_update = build_views_to_update(
        view_source_table_datasets=view_source_table_datasets,
        candidate_view_builders=views_to_update_builders,
        address_overrides=address_overrides,
    )
    rematerialize_views(
        views_to_update=views_to_update,
        all_view_builders=all_view_builders,
        view_source_table_datasets=view_source_table_datasets,
        address_overrides=address_overrides,
        skip_missing_views=skip_missing_views,
    )


def rematerialize_views(
    views_to_update: List[BigQueryView],
    all_view_builders: Sequence[BigQueryViewBuilder],
    view_source_table_datasets: Set[str],
    address_overrides: Optional[BigQueryAddressOverrides] = None,
    skip_missing_views: bool = False,
    bq_region_override: Optional[str] = None,
) -> None:
    """For all views in the provided |views_to_update| list, re-materializes any
    materialized views. This should be called only when we want to refresh the data in
    the materialized view(s), not when we want to update the underlying query of the
    view(s).

    Args:
        views_to_update: List of views to re-materialize
        all_view_builders: Superset of the views_to_update that contains all views that
            either depend on or are dependents of the list of input views.
        view_source_table_datasets: Set of datasets containing tables that can be
            treated as root nodes in the view dependency graph.
        address_overrides: A class that provides a mapping of table/view addresses
            and the address they should be replaced with for the given list of
            views_to_update.
        skip_missing_views: If True, ignores any input views that do not exist. If
            False, crashes if tries to materialize a view that does not exist.
        bq_region_override: If set, overrides the region (e.g. us-east1) associated with
            all BigQuery operations.
    """
    default_table_expiration_for_new_datasets = None
    if address_overrides:
        default_table_expiration_for_new_datasets = (
            TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
        )
        logging.info(
            "Found non-empty dataset overrides. New datasets created in this process will have a "
            "default table expiration of 24 hours."
        )

    try:
        bq_client = BigQueryClientImpl(region_override=bq_region_override)

        all_views_dag_walker = BigQueryViewDagWalker(
            build_views_to_update(
                view_source_table_datasets=view_source_table_datasets,
                candidate_view_builders=all_view_builders,
                address_overrides=address_overrides,
            )
        )
        dataset_map = get_managed_view_and_materialized_table_addresses_by_dataset(
            all_views_dag_walker
        )
        _create_all_datasets_if_necessary(
            bq_client,
            list(dataset_map.keys()),
            default_table_expiration_for_new_datasets,
        )

        # Limit DAG to only ancestor views and the set of views to update
        ancestors_dag_walker = all_views_dag_walker.get_ancestors_sub_dag(
            views_to_update
        )

        def _materialize_view(
            v: BigQueryView, _parent_results: Dict[BigQueryView, None]
        ) -> None:
            if not v.materialized_address:
                logging.info(
                    "Skipping non-materialized view [%s.%s].", v.dataset_id, v.view_id
                )
                return

            if skip_missing_views and not bq_client.table_exists(
                bq_client.dataset_ref_for_id(dataset_id=v.dataset_id), v.view_id
            ):
                logging.info(
                    "Skipping materialization of view [%s.%s] which does not exist",
                    v.dataset_id,
                    v.view_id,
                )
                return

            bq_client.materialize_view_to_table(v)

        ancestors_dag_walker.process_dag(_materialize_view)
    except Exception as e:
        with monitoring.measurements() as measurements:
            measurements.measure_int_put(m_failed_view_update, 1)
        raise e from e


def create_managed_dataset_and_deploy_views_for_view_builders(
    view_source_table_datasets: Set[str],
    view_builders_to_update: Sequence[BigQueryViewBuilder],
    historically_managed_datasets_to_clean: Optional[Set[str]],
    address_overrides: Optional[BigQueryAddressOverrides] = None,
    bq_region_override: Optional[str] = None,
    force_materialize: bool = False,
    default_table_expiration_for_new_datasets: Optional[int] = None,
) -> None:
    """Creates or updates all the views in the provided list with the view query in the
    provided view builder list. If any materialized view has been updated (or if an
    ancestor view has been updated) or the force_materialize flag is set, the view
    will be re-materialized to ensure the schemas remain consistent.

    If a |historically_managed_datasets_to_clean| set is provided,
    then cleans up unmanaged views and datasets by deleting them from BigQuery.

    Should only be called if we expect the views to have changed (either the view query
    or schema from querying underlying tables), e.g. at deploy time.
    """
    if default_table_expiration_for_new_datasets is None and address_overrides:
        default_table_expiration_for_new_datasets = (
            TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
        )
        logging.info(
            "Found non-empty dataset overrides. New datasets created in this process will have a "
            "default table expiration of 24 hours."
        )
    try:
        views_to_update = build_views_to_update(
            view_source_table_datasets=view_source_table_datasets,
            candidate_view_builders=view_builders_to_update,
            address_overrides=address_overrides,
        )

        _create_managed_dataset_and_deploy_views(
            views_to_update,
            bq_region_override,
            force_materialize,
            historically_managed_datasets_to_clean=historically_managed_datasets_to_clean,
            default_table_expiration_for_new_datasets=default_table_expiration_for_new_datasets,
        )
    except Exception as e:
        with monitoring.measurements() as measurements:
            measurements.measure_int_put(m_failed_view_update, 1)
        raise e


def copy_dataset_schemas_to_sandbox(
    datasets: Set[str], sandbox_prefix: str, default_table_expiration: int
) -> None:
    """Copies the schemas of all tables in `datasets` to sandbox datasets prefixed
    with `sandbox_prefix`. This does not copy any of the contents of the tables, only
    the schemas.
    """

    def copy_dataset_schema(dataset: str) -> Optional[str]:
        test_dataset = BigQueryAddressOverrides.format_sandbox_dataset(
            sandbox_prefix, dataset
        )
        if not bq_client.dataset_exists(
            dataset_ref=bq_client.dataset_ref_for_id(dataset)
        ):
            return None
        bq_client.create_dataset_if_necessary(
            dataset_ref=bq_client.dataset_ref_for_id(test_dataset),
            default_table_expiration_ms=default_table_expiration,
        )
        bq_client.copy_dataset_tables(dataset, test_dataset, schema_only=True)
        return test_dataset

    bq_client = BigQueryClientImpl()
    with futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        copy_futures = {
            executor.submit(
                structured_logging.with_context(copy_dataset_schema), dataset
            )
            for dataset in datasets
        }
        for future in futures.as_completed(copy_futures):
            if output_dataset := future.result():
                logging.info("Completed copy of schemas to '%s'", output_dataset)


def build_views_to_update(
    view_source_table_datasets: Set[str],
    candidate_view_builders: Sequence[BigQueryViewBuilder],
    address_overrides: Optional[BigQueryAddressOverrides],
    override_should_build_predicate: bool = False,
) -> List[BigQueryView]:
    """Returns the list of views that should be updated, built from builders in the |candidate_view_builders| list."""

    views_to_update = []
    for view_builder in candidate_view_builders:
        if view_builder.dataset_id in view_source_table_datasets:
            raise ValueError(
                f"Found view [{view_builder.view_id}] in source-table-only dataset [{view_builder.dataset_id}]"
            )

        try:
            view = view_builder.build(
                address_overrides=address_overrides,
                override_should_build_predicate=override_should_build_predicate,
            )
        except BigQueryViewBuilderShouldNotBuildError:
            logging.warning(
                "Condition failed for view builder %s in dataset %s. Continuing without it.",
                view_builder.view_id,
                view_builder.dataset_id,
            )
            continue
        views_to_update.append(view)
    return views_to_update


def _create_all_datasets_if_necessary(
    bq_client: BigQueryClient,
    dataset_ids: List[str],
    dataset_table_expiration: Optional[int],
) -> None:
    """Creates all required datasets for the list of dataset ids,
    with a table timeout if necessary. Done up front to avoid conflicts during a run of the DagWalker.
    """
    for dataset_id in dataset_ids:
        dataset_ref = bq_client.dataset_ref_for_id(dataset_id)
        bq_client.create_dataset_if_necessary(dataset_ref, dataset_table_expiration)


def _create_managed_dataset_and_deploy_views(
    views_to_update: List[BigQueryView],
    bq_region_override: Optional[str],
    force_materialize: bool,
    historically_managed_datasets_to_clean: Optional[Set[str]] = None,
    default_table_expiration_for_new_datasets: Optional[int] = None,
) -> None:
    """Create and update the given views and their parent datasets. Cleans up unmanaged views and datasets

    For each dataset key in the given dictionary, creates  the dataset if it does not
    exist, and creates or updates the underlying views mapped to that dataset.

    If a view has a set materialized_address field, materializes the view into a
    table.

    Then, cleans up BigQuery by deleting unmanaged datasets and unmanaged views within managed datasets. This is not
    performed if a temporary dataset table expiration is already set.

    Args:
        views_to_update: A list of view objects to be created or updated.
        set_temp_dataset_table_expiration: If True, new datasets will be created with an expiration
            of TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS.
        historically_managed_datasets_to_clean: Set of datasets that have
            ever been managed if we should cleanup unmanaged views in this deploy
            process. If null, does not perform the cleanup step. If provided,
            will error if any dataset required for the |views_to_update| is not
            included in this set.
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

    def process_fn(v: BigQueryView, parent_results: Dict[BigQueryView, bool]) -> bool:
        """Returns True if this view or any of its parents were updated."""
        return _create_or_update_view_and_materialize_if_necessary(
            bq_client, v, parent_results, force_materialize
        )

    dag_walker.process_dag(process_fn)


def _create_or_update_view_and_materialize_if_necessary(
    bq_client: BigQueryClient,
    view: BigQueryView,
    parent_results: Dict[BigQueryView, bool],
    force_materialize: bool,
) -> bool:
    """Creates or updates the provided view in BigQuery and materializes that view into a table when appropriate.
    Returns True if this view or any views in its parent chain have been updated from the version that was saved in
    BigQuery before this update.
    """
    parent_changed = any(parent_results.values())
    view_changed = False
    dataset_ref = bq_client.dataset_ref_for_id(view.dataset_id)

    try:
        existing_view = bq_client.get_table(dataset_ref, view.view_id)
        if (
            existing_view.view_query != view.view_query
            or existing_view.clustering_fields != view.clustering_fields
        ):
            # If the view query has changed, the view has changed
            # Also update the view if clustering fields have changed
            view_changed = True
        old_schema = existing_view.schema
    except exceptions.NotFound:
        view_changed = True
        old_schema = None

    # TODO(https://issuetracker.google.com/issues/180636362): Currently we have to delete and recreate the view for
    # changes from underlying tables to be reflected in its schema.
    if old_schema is not None:
        bq_client.delete_table(dataset_ref.dataset_id, view.view_id)
    updated_view = bq_client.create_or_update_view(view)

    if updated_view.schema != old_schema:
        # We also check for schema changes, just in case a parent view or table has added a column
        view_changed = True

    if view.materialized_address:
        materialized_view_dataset_ref = bq_client.dataset_ref_for_id(
            view.materialized_address.dataset_id
        )
        if (
            view_changed
            or parent_changed
            or not bq_client.table_exists(
                materialized_view_dataset_ref, view.materialized_address.table_id
            )
            or force_materialize
        ):
            bq_client.materialize_view_to_table(view)
        else:
            logging.info(
                "Skipping materialization of view [%s.%s] which has not changed.",
                view.dataset_id,
                view.view_id,
            )
    return view_changed or parent_changed or force_materialize


def view_builder_sub_graph_for_view_builders_to_load(
    view_builders_to_load: Sequence[BigQueryViewBuilder],
    all_view_builders_in_dag: Sequence[BigQueryViewBuilder],
    get_ancestors: bool,
    get_descendants: bool,
    include_dataflow_views: Optional[bool] = True,
) -> Sequence[BigQueryViewBuilder]:
    """Returns the view builders that are in the sub-graph of the provided
    |view_builders_to_load|. Used when we are loading just a subset of views,
    and we need to determine the ancestors and/or descendants of the views we're
    loading.

    If |get_ancestors| is True, includes all ancestor views of |view_builders_to_load|.
    If |get_descendants| is True, includes all views that are descendant from the
    |view_builders_to_load|.
    """
    # Get views from view_builders_to_load
    views_to_load = build_views_to_update(
        view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
        candidate_view_builders=view_builders_to_load,
        address_overrides=None,
    )

    # Get dag walker for *all* views
    all_views_dag_walker = BigQueryViewDagWalker(
        build_views_to_update(
            view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
            candidate_view_builders=all_view_builders_in_dag,
            address_overrides=None,
        )
    )

    # If necessary, get descendants of views_to_load
    if get_descendants:
        descendants_dag_walker = all_views_dag_walker.get_descendants_sub_dag(
            views_to_load
        )
        views_to_load.extend(descendants_dag_walker.views)

    # If necessary, get ancestor views of views_to_load
    if get_ancestors:
        ancestors_dag_walker = all_views_dag_walker.get_ancestors_sub_dag(views_to_load)
        views_to_load.extend(ancestors_dag_walker.views)

    # Get set of view addresses that are in the view sub-graph
    distinct_view_addresses_to_update = {view.address for view in set(views_to_load)}

    # Return the view builders in the sub-graph
    return [
        builder
        for builder in all_view_builders_in_dag
        if BigQueryAddress(dataset_id=builder.dataset_id, table_id=builder.view_id)
        in distinct_view_addresses_to_update
        # Only update views in the DATAFLOW_METRICS_MATERIALIZED_DATASET if the
        # dataflow_dataset_override is set
        if include_dataflow_views
        or builder.dataset_id != DATAFLOW_METRICS_MATERIALIZED_DATASET
    ]
