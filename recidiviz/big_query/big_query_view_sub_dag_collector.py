# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Defines a BigQueryViewCollector implementation that collects view builders that make
up only a part of a full DAG of BigQuery views.
"""

from typing import List, Optional, Sequence, Set

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.view_update_manager import build_views_to_update
from recidiviz.view_registry.datasets import VIEW_SOURCE_TABLE_DATASETS


class BigQueryViewSubDagCollector(BigQueryViewCollector[BigQueryViewBuilder]):
    """A BigQueryViewCollector implementation that collects view builders that make
    up only a part of a full DAG of BigQuery views.
    """

    def __init__(
        self,
        view_builders_in_full_dag: Sequence[BigQueryViewBuilder],
        view_addresses_in_sub_dag: Optional[Set[BigQueryAddress]],
        dataset_ids_in_sub_dag: Optional[Set[str]],
        include_ancestors: bool,
        include_descendants: bool,
        datasets_to_exclude: Set[str],
    ):
        """Builds a new BigQueryViewSubDagCollector
        Args:
            view_builders_in_full_dag: The list of all view builders in the DAG of all
                views that we want to build a sub-DAG from.
            view_addresses_in_sub_dag: If specified, the sub-DAG will include all views
                at these addresses (and any views upstream / downstream of those views,
                as appropriate). Either this arg or |dataset_ids_in_sub_dag| must be
                nonnull.
            dataset_ids_in_sub_dag: If specified, the sub-DAG will include all views
                in these datasets (and any views upstream / downstream of those views,
                as appropriate). Either this arg or |view_addresses_in_sub_dag| must be
                nonnull.
            include_ancestors: If True, all ancestors of selected views will be included
                in the sub-DAG as well.
            include_descendants: If True, all descendants of selected views will be
                included in the sub-DAG as well.
            datasets_to_exclude: Views in these datasets will be excluded from the
                sub-DAG, even if they are upstream / downstream of views in
                |view_addresses_in_sub_dag| or |dataset_ids_in_sub_dag|.
        """

        if not (view_addresses_in_sub_dag or dataset_ids_in_sub_dag):
            raise ValueError(
                "Must define at least one of view_ids_to_load or dataset_ids_to_load."
            )

        self.view_builders_in_full_dag = view_builders_in_full_dag
        self.view_addresses_in_sub_dag = view_addresses_in_sub_dag
        self.dataset_ids_in_sub_dag = dataset_ids_in_sub_dag
        self.include_ancestors = include_ancestors
        self.include_descendants = include_descendants
        self.datasets_to_exclude = datasets_to_exclude

    def collect_view_builders(self) -> List[BigQueryViewBuilder]:
        # Get dag walker for *all* views
        full_dag_walker = BigQueryViewDagWalker(
            build_views_to_update(
                view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
                candidate_view_builders=self.view_builders_in_full_dag,
                address_overrides=None,
            )
        )
        full_dag_walker.populate_node_view_builders(self.view_builders_in_full_dag)

        root_views_in_sub_dag = self._get_root_views_in_sub_dag(
            all_views=full_dag_walker.views,
        )

        # Get the full subgraph
        sub_graph_dag_walker = full_dag_walker.get_sub_dag(
            views=root_views_in_sub_dag,
            include_ancestors=self.include_ancestors,
            include_descendants=self.include_descendants,
        )

        # Return the view builders in the sub-graph that aren't in datasets_to_exclude
        return [
            builder
            for builder in sub_graph_dag_walker.view_builders()
            if not self.datasets_to_exclude
            or builder.dataset_id not in self.datasets_to_exclude
        ]

    def _get_root_views_in_sub_dag(
        self, all_views: Sequence[BigQueryView]
    ) -> List[BigQueryView]:
        """Returns the list of view builders that match either the addresses in
        view_ids_to_load or dataset_ids in dataset_ids_to_load. These are the roots of
        the sub-DAG that we will expand from to build the full sub-DAG (including
        child/parent views as is appropriate).
        """

        views_in_sub_dag: List[BigQueryView] = []
        if self.dataset_ids_in_sub_dag:
            views_in_datasets = [
                view
                for view in all_views
                if view.dataset_id in self.dataset_ids_in_sub_dag
            ]

            found_datasets = {view.dataset_id for view in views_in_datasets}
            if found_datasets != set(self.dataset_ids_in_sub_dag):
                missing_datasets = set(self.dataset_ids_in_sub_dag) - found_datasets
                raise ValueError(
                    f"Did not find any views in the following datasets: {missing_datasets}"
                )
            views_in_sub_dag.extend(views_in_datasets)

        if self.view_addresses_in_sub_dag:
            if len(set(self.view_addresses_in_sub_dag)) != len(
                self.view_addresses_in_sub_dag
            ):
                raise ValueError(
                    f"Found duplicates in list of input views to load: {self.view_addresses_in_sub_dag}"
                )

            if self.dataset_ids_in_sub_dag:
                overlapping_addresses = [
                    a
                    for a in self.view_addresses_in_sub_dag
                    if a in self.dataset_ids_in_sub_dag
                ]
                if overlapping_addresses:
                    raise ValueError(
                        f"Found addresses in --view_ids_to_load with datasets already "
                        f"listed in --dataset_ids_to_load: {overlapping_addresses}"
                    )

            views_with_addresses = [
                view
                for view in all_views
                if view.address in self.view_addresses_in_sub_dag
            ]
            if len(views_with_addresses) != len(self.view_addresses_in_sub_dag):
                found_builders_set = {vb.address for vb in views_with_addresses}
                expected_builders_set = set(self.view_addresses_in_sub_dag)
                missing = expected_builders_set - found_builders_set
                raise ValueError(
                    f"Expected to find [{len(self.view_addresses_in_sub_dag)}], but only "
                    f"found [{len(views_with_addresses)}] that matched managed views. "
                    f"Did not find views that matched the following expected "
                    f"addresses: {missing}."
                )
            views_in_sub_dag.extend(views_with_addresses)
        return views_in_sub_dag
