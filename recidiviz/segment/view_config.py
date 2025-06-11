# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
from typing import Sequence

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.union_all_big_query_view_builder import (
    UnionAllBigQueryViewBuilder,
)
from recidiviz.segment.segment_event_big_query_view_builder import (
    SegmentEventBigQueryViewBuilder,
)
from recidiviz.segment.segment_event_big_query_view_collector import (
    SegmentEventBigQueryViewCollector,
)


def _get_unioned_segment_event_builders() -> list[UnionAllBigQueryViewBuilder]:
    def _select_statement(vb: SegmentEventBigQueryViewBuilder) -> str:
        return f"""
SELECT
    state_code,
    user_id,
    email,
    "{vb.segment_event_name}" AS event,
    event_ts,
    person_id,
"""

    product_union_builders = []
    for product_type, builders in (
        SegmentEventBigQueryViewCollector()
        .collect_segment_event_view_builders_by_product()
        .items()
    ):
        view_id = f"all_{product_type.pretty_name}_segment_events"
        product_dataset_id = builders[0].address.dataset_id
        unioned_builder = UnionAllBigQueryViewBuilder(
            dataset_id=product_dataset_id,
            view_id=view_id,
            description="Union of all segment events for a product.",
            parents=builders,
            clustering_fields=["state_code", "person_id"],
            parent_to_select_statement=_select_statement,
        )
        product_union_builders.append(unioned_builder)
    return product_union_builders


def get_view_builders_for_views_to_update() -> Sequence[BigQueryViewBuilder]:
    return [
        *SegmentEventBigQueryViewCollector().collect_view_builders(),
        *_get_unioned_segment_event_builders(),
    ]
