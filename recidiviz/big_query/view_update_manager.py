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
from typing import Dict, List, Optional, Sequence, Set

from google.cloud import exceptions
from opencensus.stats import aggregation, measure
from opencensus.stats import view as opencensus_view

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
from recidiviz.utils import monitoring

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


def rematerialize_views_for_view_builders(
    views_to_update_builders: Sequence[BigQueryViewBuilder],
    all_view_builders: Sequence[BigQueryViewBuilder],
    view_source_table_datasets: Set[str],
    dataset_overrides: Optional[Dict[str, str]] = None,
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
        dataset_overrides: A dictionary mapping dataset_ids to the dataset name they
            should be replaced with for the given list of views_to_update.
        skip_missing_views: If True, ignores any input views that do not exist. If
            False, crashes if tries to materialize a view that does not exist.
    """
    views_to_update = build_views_to_update(
        view_source_table_datasets=view_source_table_datasets,
        candidate_view_builders=views_to_update_builders,
        dataset_overrides=dataset_overrides,
    )
    rematerialize_views(
        views_to_update=views_to_update,
        all_view_builders=all_view_builders,
        view_source_table_datasets=view_source_table_datasets,
        dataset_overrides=dataset_overrides,
        skip_missing_views=skip_missing_views,
    )


def rematerialize_views(
    views_to_update: List[BigQueryView],
    all_view_builders: Sequence[BigQueryViewBuilder],
    view_source_table_datasets: Set[str],
    dataset_overrides: Optional[Dict[str, str]] = None,
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
        dataset_overrides: A dictionary mapping dataset_ids to the dataset name they
            should be replaced with for the given list of views_to_update.
        skip_missing_views: If True, ignores any input views that do not exist. If
            False, crashes if tries to materialize a view that does not exist.
        bq_region_override: If set, overrides the region (e.g. us-east1) associated with
            all BigQuery operations.
    """
    set_default_table_expiration_for_new_datasets = bool(dataset_overrides)
    if set_default_table_expiration_for_new_datasets:
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
                dataset_overrides=dataset_overrides,
            )
        )
        dataset_map = get_managed_view_and_materialized_table_addresses_by_dataset(
            all_views_dag_walker
        )
        _create_all_datasets_if_necessary(
            bq_client,
            list(dataset_map.keys()),
            set_default_table_expiration_for_new_datasets,
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
    dataset_overrides: Optional[Dict[str, str]] = None,
    bq_region_override: Optional[str] = None,
    force_materialize: bool = False,
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
    set_default_table_expiration_for_new_datasets = bool(dataset_overrides)
    if set_default_table_expiration_for_new_datasets:
        logging.info(
            "Found non-empty dataset overrides. New datasets created in this process will have a "
            "default table expiration of 24 hours."
        )
    try:
        views_to_update = build_views_to_update(
            view_source_table_datasets=view_source_table_datasets,
            candidate_view_builders=view_builders_to_update,
            dataset_overrides=dataset_overrides,
        )

        _create_managed_dataset_and_deploy_views(
            views_to_update,
            bq_region_override,
            force_materialize,
            historically_managed_datasets_to_clean=historically_managed_datasets_to_clean,
            set_temp_dataset_table_expiration=set_default_table_expiration_for_new_datasets,
        )
    except Exception as e:
        with monitoring.measurements() as measurements:
            measurements.measure_int_put(m_failed_view_update, 1)
        raise e


def build_views_to_update(
    view_source_table_datasets: Set[str],
    candidate_view_builders: Sequence[BigQueryViewBuilder],
    dataset_overrides: Optional[Dict[str, str]],
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
                dataset_overrides=dataset_overrides,
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
    set_temp_dataset_table_expiration: bool,
) -> None:
    """Creates all required datasets for the list of dataset ids,
    with a table timeout if necessary. Done up front to avoid conflicts during a run of the DagWalker.
    """
    new_dataset_table_expiration_ms = (
        TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
        if set_temp_dataset_table_expiration
        else None
    )

    for dataset_id in dataset_ids:
        dataset_ref = bq_client.dataset_ref_for_id(dataset_id)
        bq_client.create_dataset_if_necessary(
            dataset_ref, new_dataset_table_expiration_ms
        )


def _create_managed_dataset_and_deploy_views(
    views_to_update: List[BigQueryView],
    bq_region_override: Optional[str],
    force_materialize: bool,
    historically_managed_datasets_to_clean: Optional[Set[str]] = None,
    set_temp_dataset_table_expiration: bool = False,
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
        bq_client, managed_dataset_ids, set_temp_dataset_table_expiration
    )

    if (
        historically_managed_datasets_to_clean
        # We don't want to delete unmanaged views/tables if we're creating sandbox datasets
        and not set_temp_dataset_table_expiration
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
        if existing_view.view_query != view.view_query:
            # If the view query has changed, the view has changed
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
