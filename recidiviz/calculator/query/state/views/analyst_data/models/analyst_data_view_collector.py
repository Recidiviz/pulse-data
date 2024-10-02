# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Functions for collating span or event query builders into a view builder."""
from typing import List

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.observations.dataset_config import dataset_for_observation_type_cls
from recidiviz.observations.event_type import EventType
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.observations.span_type import SpanType
from recidiviz.utils.types import assert_type


# TODO(#32921): Delete this once we have migrated to use the new all_*_spans and
#  all_*_events views or their observation-specific parents.
def get_spans_and_events_view_builders() -> List[SimpleBigQueryViewBuilder]:
    """Returns all view builders for configured spans and events"""
    view_builders: List[SimpleBigQueryViewBuilder] = []

    observation_type_classes: set[type[EventType] | type[SpanType]] = {
        EventType,
        SpanType,
    }
    for observation_type_cls in observation_type_classes:
        valid_units_of_observation_types: set[MetricUnitOfObservationType] = set(
            (
                t.unit_of_observation_type
                if isinstance(t, EventType)
                else assert_type(t, SpanType).unit_of_observation_type
            )
            for t in observation_type_cls
        )
        for unit in valid_units_of_observation_types:
            category = observation_type_cls.observation_type_category()
            new_all_rows_materialized_address = BigQueryAddress(
                dataset_id=dataset_for_observation_type_cls(unit, observation_type_cls),
                table_id=f"all_{unit.short_name}_{category}s_materialized",
            )

            legacy_all_rows_view_address = BigQueryAddress(
                dataset_id=ANALYST_VIEWS_DATASET,
                table_id=f"{unit.short_name}_{category}s",
            )

            description = (
                f"Legacy view containing all {category} observations. For "
                f"information on individual {category} types, see individual "
                f"observation views in the {new_all_rows_materialized_address.dataset_id} "
                f"dataset."
            )
            view_builder = SimpleBigQueryViewBuilder(
                dataset_id=legacy_all_rows_view_address.dataset_id,
                view_id=legacy_all_rows_view_address.table_id,
                view_query_template=f"SELECT * FROM `{{project_id}}.{new_all_rows_materialized_address.to_str()}`",
                description=description,
                bq_description=description,
                should_materialize=True,
                clustering_fields=["state_code", category],
            )
            view_builders.append(view_builder)

    return view_builders
