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
"""Transitions view configuration."""

from datetime import datetime
from typing import Sequence

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.union_all_big_query_view_builder import (
    UnionAllBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.dataset_config import TRANSITIONS_DATASET_ID
from recidiviz.outcome_metrics.impact_transitions_view_builder import (
    ImpactTransitionsBigQueryViewBuilder,
)
from recidiviz.outcome_metrics.impact_transitions_view_collector import (
    ImpactTransitionsBigQueryViewCollector,
)
from recidiviz.outcome_metrics.views.all_full_state_launch_dates import (
    ALL_FULL_STATE_LAUNCH_DATES_VIEW_BUILDER,
)
from recidiviz.outcome_metrics.views.transitions_metric_utils import (
    collect_view_builders_for_breadth_depth_metrics,
)


def get_unioned_transitions_view_builder() -> UnionAllBigQueryViewBuilder:
    def _parent_view_to_select_statement(
        vb: ImpactTransitionsBigQueryViewBuilder,
    ) -> str:
        return f"SELECT *, '{vb.product_transition_type.pretty_name}' AS product_transition_type"

    return UnionAllBigQueryViewBuilder(
        dataset_id=TRANSITIONS_DATASET_ID,
        view_id="all_impact_transitions",
        description="Convenience view combining all transitions metrics into a single view",
        parents=ImpactTransitionsBigQueryViewCollector().collect_view_builders(),
        clustering_fields=["state_code"],
        parent_view_to_select_statement=_parent_view_to_select_statement,
    )


def get_transitions_view_builders_for_views_to_update() -> Sequence[
    BigQueryViewBuilder
]:
    """Collects and returns a list of builders for all views related to
    orgwide transitions metrics
    """
    attribute_col_combinations = [
        ["state_code"],
        ["product_transition_type"],
        ["decarceral_impact_type", "has_mandatory_due_date", "is_jii_transition"],
    ]

    current_year = datetime.now().year
    breadth_depth_builders: list[BigQueryViewBuilder] = []
    for year in range(2024, current_year + 1):
        for attribute_cols in attribute_col_combinations:
            breadth_depth_builders.extend(
                collect_view_builders_for_breadth_depth_metrics(
                    metric_year=year, attribute_cols=attribute_cols
                )
            )

    return [
        *ImpactTransitionsBigQueryViewCollector().collect_view_builders(),
        get_unioned_transitions_view_builder(),
        ALL_FULL_STATE_LAUNCH_DATES_VIEW_BUILDER,
        *breadth_depth_builders,
    ]
