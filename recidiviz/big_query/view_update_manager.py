# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
from typing import Dict, List, Sequence, Optional, Set

from google.cloud import exceptions
from opencensus.stats import measure, view as opencensus_view, aggregation

from recidiviz.big_query.big_query_client import BigQueryClientImpl, BigQueryClient
from recidiviz.big_query.big_query_view import (
    BigQueryView,
    BigQueryViewBuilder,
    BigQueryViewBuilderShouldNotBuildError,
)
from recidiviz.view_registry.namespaces import BigQueryViewNamespace
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.ingest.direct.views.normalized_direct_ingest_big_query_view_types import (
    NormalizedDirectIngestPreProcessedIngestViewBuilder,
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


def rematerialize_views(
    view_builders_by_namespace: Dict[
        BigQueryViewNamespace, Sequence[BigQueryViewBuilder]
    ],
    view_source_table_datasets: Set[str],
    dataset_overrides: Optional[Dict[str, str]] = None,
) -> None:
    """For all registered views, re-materializes any materialized views. This should be called only when we want to
    refresh the data in the materialized view, not when we want to update the underlying query of the view.
    """
    for namespace, builders in view_builders_by_namespace.items():
        rematerialize_views_for_namespace(
            bq_view_namespace=namespace,
            view_source_table_datasets=view_source_table_datasets,
            candidate_view_builders=builders,
            dataset_overrides=dataset_overrides,
        )


def rematerialize_views_for_namespace(
    # TODO(#5785): Clarify use case of BigQueryViewNamespace filter (see ticket for more)
    bq_view_namespace: BigQueryViewNamespace,
    view_source_table_datasets: Set[str],
    candidate_view_builders: Sequence[BigQueryViewBuilder],
    dataset_overrides: Optional[Dict[str, str]] = None,
    skip_missing_views: bool = False,
) -> None:
    """For all views in a given namespace, re-materializes any materialized views. This should be called only when we
    want to refresh the data in the materialized view, not when we want to update the underlying query of the view.
    """
    set_default_table_expiration_for_new_datasets = bool(dataset_overrides)
    if set_default_table_expiration_for_new_datasets:
        logging.info(
            "Found non-empty dataset overrides. New datasets created in this process will have a "
            "default table expiration of 24 hours."
        )

    try:
        views_to_update = _build_views_to_update(
            view_source_table_datasets=view_source_table_datasets,
            candidate_view_builders=candidate_view_builders,
            dataset_overrides=dataset_overrides,
        )

        bq_client = BigQueryClientImpl()
        _create_all_datasets_if_necessary(
            bq_client, views_to_update, set_default_table_expiration_for_new_datasets
        )

        dag_walker = BigQueryViewDagWalker(views_to_update)

        def _materialize_view(
            v: BigQueryView, _parent_results: Dict[BigQueryView, None]
        ) -> None:
            if not v.materialized_location:
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

        dag_walker.process_dag(_materialize_view)

    except Exception as e:
        with monitoring.measurements(
            {monitoring.TagKey.CREATE_UPDATE_VIEWS_NAMESPACE: bq_view_namespace.value}
        ) as measurements:
            measurements.measure_int_put(m_failed_view_update, 1)
        raise e from e


def create_dataset_and_deploy_views_for_view_builders(
    # TODO(#5785): Clarify use case of BigQueryViewNamespace filter (see ticket for more)
    bq_view_namespace: BigQueryViewNamespace,
    view_source_table_datasets: Set[str],
    view_builders_to_update: Sequence[BigQueryViewBuilder],
    dataset_overrides: Optional[Dict[str, str]] = None,
) -> None:
    """Creates or updates all the views in the provided list with the view query in the provided view builder list. If
    any materialized view has been updated (or if an ancestor view has been updated), the view will be re-materialized
    to ensure the schemas remain consistent.

    Should only be called if we expect the views to have changed (either the view query or schema from querying
    underlying tables), e.g. at deploy time.
    """
    set_default_table_expiration_for_new_datasets = bool(dataset_overrides)
    if set_default_table_expiration_for_new_datasets:
        logging.info(
            "Found non-empty dataset overrides. New datasets created in this process will have a "
            "default table expiration of 24 hours."
        )
    try:
        views_to_update = _build_views_to_update(
            view_source_table_datasets=view_source_table_datasets,
            candidate_view_builders=view_builders_to_update,
            dataset_overrides=dataset_overrides,
        )

        _create_dataset_and_deploy_views(
            views_to_update, set_default_table_expiration_for_new_datasets
        )
    except Exception as e:
        with monitoring.measurements(
            {monitoring.TagKey.CREATE_UPDATE_VIEWS_NAMESPACE: bq_view_namespace.value}
        ) as measurements:
            measurements.measure_int_put(m_failed_view_update, 1)
        raise e


def _build_views_to_update(
    view_source_table_datasets: Set[str],
    candidate_view_builders: Sequence[BigQueryViewBuilder],
    dataset_overrides: Optional[Dict[str, str]],
) -> List[BigQueryView]:
    """Returns the list of views that should be updated, built from builders in the |candidate_view_builders| list."""

    views_to_update = []
    for view_builder in candidate_view_builders:
        if view_builder.dataset_id in view_source_table_datasets:
            raise ValueError(
                f"Found view [{view_builder.view_id}] in source-table-only dataset [{view_builder.dataset_id}]"
            )

        # TODO(#6314): Remove this check once we have deleted the hack us_pa_supervision_period_TEMP view
        if (
            isinstance(
                view_builder, NormalizedDirectIngestPreProcessedIngestViewBuilder
            )
            and view_builder.ingest_view_name == "us_pa_supervision_period_TEMP"
            and dataset_overrides
        ):
            # Don't update the us_pa_supervision_period_TEMP view when loading sandbox
            # views with dataset_overrides
            continue

        try:
            view = view_builder.build(dataset_overrides=dataset_overrides)
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
    views_to_update: List[BigQueryView],
    set_temp_dataset_table_expiration: bool,
) -> None:
    """Creates all required datasets for the list of views, with a table timeout if necessary. Done up front to avoid
    conflicts during a run of the DagWalker.
    """
    new_dataset_table_expiration_ms = (
        TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
        if set_temp_dataset_table_expiration
        else None
    )
    dataset_ids = set()
    for view in views_to_update:
        views_dataset_ref = bq_client.dataset_ref_for_id(view.dataset_id)
        if view.dataset_id not in dataset_ids:
            bq_client.create_dataset_if_necessary(
                views_dataset_ref, new_dataset_table_expiration_ms
            )
            dataset_ids.add(view.dataset_id)


def _create_dataset_and_deploy_views(
    views_to_update: List[BigQueryView], set_temp_dataset_table_expiration: bool = False
) -> None:
    """Create and update the given views and their parent datasets.

    For each dataset key in the given dictionary, creates the dataset if it does not
    exist, and creates or updates the underlying views mapped to that dataset.

    If a view has a set materialized_location field, materializes the view into a
    table.

    Args:
        views_to_update: A list of view objects to be created or updated.
        set_temp_dataset_table_expiration: If True, new datasets will be created with an
         expiration of TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS.
    """

    bq_client = BigQueryClientImpl()
    _create_all_datasets_if_necessary(
        bq_client, views_to_update, set_temp_dataset_table_expiration
    )

    dag_walker = BigQueryViewDagWalker(views_to_update)

    def process_fn(v: BigQueryView, parent_results: Dict[BigQueryView, bool]) -> bool:
        """Returns True if this view or any of its parents were updated."""
        return _create_or_update_view_and_materialize_if_necessary(
            bq_client, v, parent_results
        )

    dag_walker.process_dag(process_fn)


def _create_or_update_view_and_materialize_if_necessary(
    bq_client: BigQueryClient,
    view: BigQueryView,
    parent_results: Dict[BigQueryView, bool],
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
    updated_view = bq_client.create_or_update_view(dataset_ref, view)

    if updated_view.schema != old_schema:
        # We also check for schema changes, just in case a parent view or table has added a column
        view_changed = True

    if view.materialized_location:
        materialized_view_dataset_ref = bq_client.dataset_ref_for_id(
            view.materialized_location.dataset_id
        )
        if (
            view_changed
            or parent_changed
            or not bq_client.table_exists(
                materialized_view_dataset_ref, view.materialized_location.table_id
            )
        ):
            bq_client.materialize_view_to_table(view)
        else:
            logging.info(
                "Skipping materialization of view [%s.%s] which has not changed.",
                view.dataset_id,
                view.view_id,
            )
    return view_changed or parent_changed
