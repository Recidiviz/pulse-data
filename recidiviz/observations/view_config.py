# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Observation view configuration."""
from typing import Sequence, Type

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.union_all_big_query_view_builder import (
    UnionAllBigQueryViewBuilder,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    convert_cols_to_json_string,
)
from recidiviz.observations.dataset_config import dataset_for_observation_type_cls
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.observations.observation_big_query_view_collector import (
    ObservationBigQueryViewCollector,
)
from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType


def _description_for_union_all_view(
    unit_of_observation: MetricUnitOfObservationType,
    observation_type_cls: Type[SpanType] | Type[EventType],
) -> str:
    dataset_id = dataset_for_observation_type_cls(
        unit_of_observation=unit_of_observation,
        observation_type_cls=SpanType,
    )
    observation_category = observation_type_cls.observation_type_category()

    return (
        f"Convenience view containing all results from {observation_category}-type "
        f"{unit_of_observation.short_name} observation views. Individual type-specific "
        f"views are defined in dataset {dataset_id}. This view should not be queried "
        f"directly in downstream views and should generally only be used for debugging "
        f"/ one-off local analysis."
    )


def _build_unioned_spans_builder(
    unit_of_observation_type: MetricUnitOfObservationType,
    parent_span_views: list[SpanObservationBigQueryViewBuilder],
) -> UnionAllBigQueryViewBuilder:
    """Returns a view builder for a view that unions together all the span observations
    for a given unit of observation.
    """
    unit_of_observation = MetricUnitOfObservation(type=unit_of_observation_type)
    non_attribute_columns_str = ", ".join(
        SpanObservationBigQueryViewBuilder.non_attribute_output_columns(
            unit_of_observation_type
        )
    )

    def _parent_view_to_select_statement(vb: SpanObservationBigQueryViewBuilder) -> str:
        return (
            f'SELECT "{vb.span_type.value}" AS span, {non_attribute_columns_str}, '
            f"{convert_cols_to_json_string(vb.attribute_cols)} AS {vb.SPAN_ATTRIBUTES_OUTPUT_COL_NAME}"
        )

    dataset_id = dataset_for_observation_type_cls(
        unit_of_observation=unit_of_observation_type,
        observation_type_cls=SpanType,
    )

    return UnionAllBigQueryViewBuilder(
        dataset_id=dataset_id,
        view_id=f"all_{unit_of_observation_type.short_name}_spans",
        description=_description_for_union_all_view(unit_of_observation_type, SpanType),
        parents=parent_span_views,
        clustering_fields=(unit_of_observation.primary_key_columns_ordered + ["span"]),
        parent_view_to_select_statement=_parent_view_to_select_statement,
    )


def _build_unioned_events_builder(
    unit_of_observation_type: MetricUnitOfObservationType,
    parent_event_views: list[EventObservationBigQueryViewBuilder],
) -> UnionAllBigQueryViewBuilder:
    """Returns a view builder for a view that unions together all the event observations
    for a given unit of observation.
    """
    unit_of_observation = MetricUnitOfObservation(type=unit_of_observation_type)
    non_attribute_columns_str = ", ".join(
        EventObservationBigQueryViewBuilder.non_attribute_output_columns(
            unit_of_observation_type
        )
    )

    def _parent_view_to_select_statement(
        vb: EventObservationBigQueryViewBuilder,
    ) -> str:
        return (
            f'SELECT "{vb.event_type.value}" AS event, {non_attribute_columns_str}, '
            f"{convert_cols_to_json_string(vb.attribute_cols)} AS {vb.EVENT_ATTRIBUTES_OUTPUT_COL_NAME}"
        )

    return UnionAllBigQueryViewBuilder(
        dataset_id=dataset_for_observation_type_cls(
            unit_of_observation=unit_of_observation_type,
            observation_type_cls=EventType,
        ),
        view_id=f"all_{unit_of_observation_type.short_name}_events",
        description=_description_for_union_all_view(
            unit_of_observation_type, EventType
        ),
        parents=parent_event_views,
        clustering_fields=(unit_of_observation.primary_key_columns_ordered + ["event"]),
        parent_view_to_select_statement=_parent_view_to_select_statement,
    )


def _get_unioned_observation_builders() -> list[UnionAllBigQueryViewBuilder]:
    """Returns views that union all observations of the same type together into a single
    materialized view.
    """
    collector = ObservationBigQueryViewCollector()

    builders = []
    span_builders_by_unit = collector.collect_span_builders_by_unit_of_observation()
    for unit_of_observation in MetricUnitOfObservationType:
        parent_span_views = span_builders_by_unit.get(unit_of_observation, None)
        if not parent_span_views:
            continue
        builders.append(
            _build_unioned_spans_builder(
                parent_span_views=parent_span_views,
                unit_of_observation_type=unit_of_observation,
            )
        )

    event_builders_by_unit = collector.collect_event_builders_by_unit_of_observation()
    for unit_of_observation in MetricUnitOfObservationType:
        parent_event_views = event_builders_by_unit.get(unit_of_observation, None)
        if not parent_event_views:
            continue

        builders.append(
            _build_unioned_events_builder(
                parent_event_views=parent_event_views,
                unit_of_observation_type=unit_of_observation,
            )
        )
    return builders


def get_view_builders_for_views_to_update() -> Sequence[BigQueryViewBuilder]:
    return [
        *ObservationBigQueryViewCollector().collect_view_builders(),
    ]
