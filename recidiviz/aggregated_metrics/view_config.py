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
"""Aggregated metrics view configuration."""

from typing import Sequence

from recidiviz.aggregated_metrics.aggregated_metric_collection_config import (
    AggregatedMetricsCollection,
)
from recidiviz.aggregated_metrics.aggregated_metrics_view_collector import (
    collect_aggregated_metric_view_builders_for_collection,
    collect_assignments_by_time_period_builders_for_collections,
)
from recidiviz.aggregated_metrics.assignment_sessions_view_collector import (
    collect_assignment_sessions_view_builders,
)
from recidiviz.aggregated_metrics.configuration import collections as collections_module
from recidiviz.aggregated_metrics.supervision_officer_caseload_count_spans import (
    SUPERVISION_OFFICER_CASELOAD_COUNT_SPANS_VIEW_BUILDER,
)
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.utils.immutable_key_dict import ImmutableKeyDict


def collect_aggregated_metrics_collection_configs() -> list[
    AggregatedMetricsCollection
]:
    collections = ModuleCollectorMixin.collect_top_level_attributes_in_module(
        attribute_type=AggregatedMetricsCollection,
        dir_module=collections_module,
        attribute_name_regex=r".*AGGREGATED_METRICS_COLLECTION_CONFIG",
        collect_from_callables=True,
        callable_name_regex=r"^get_aggregated_metrics_collections$",
        deduplicate_found_attributes=False,
    )
    if not collections:
        raise ValueError(
            f"Did not find any AggregatedMetricsCollection in [{collections_module}]"
        )
    return collections


def get_all_aggregated_metrics_collections_view_builders(
    all_collections: list[AggregatedMetricsCollection],
) -> Sequence[BigQueryViewBuilder]:
    builders: ImmutableKeyDict[
        BigQueryAddress, BigQueryViewBuilder
    ] = ImmutableKeyDict()
    for collection in all_collections:
        try:
            # Add to an immutable key dictionary to make sure there are no duplicate
            # addresses.
            builders.update(
                {
                    b.address: b
                    for b in collect_aggregated_metric_view_builders_for_collection(
                        collection
                    )
                }
            )
        except KeyError as e:
            raise ValueError(
                f"Found AggregatedMetricsCollection in dataset "
                f"[{collection.output_dataset_id}] that has views overlapping with "
                f"another AggregatedMetricsCollection defined in the "
                f"{collections_module.__name__} module. You might want to add a "
                f"collection_tag to one or more AggregatedMetricsCollection to avoid "
                f"this overlap."
            ) from e
    return list(builders.values())


def get_aggregated_metrics_view_builders() -> Sequence[BigQueryViewBuilder]:
    """
    Returns a list of builders for all views related to aggregated metrics
    """
    all_collections = collect_aggregated_metrics_collection_configs()
    return [
        SUPERVISION_OFFICER_CASELOAD_COUNT_SPANS_VIEW_BUILDER,
        *collect_assignment_sessions_view_builders(),
        *collect_assignments_by_time_period_builders_for_collections(all_collections),
        *get_all_aggregated_metrics_collections_view_builders(all_collections),
    ]
