# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Shared schemas for outcome_metrics transition views."""

from recidiviz.big_query.big_query_view_column import (
    COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
    BigQueryViewColumn,
    Bool,
    Date,
    Float,
    Integer,
    String,
)


def impact_transitions_base_schema() -> list[BigQueryViewColumn]:
    """Base schema for individual impact transitions views."""
    return [
        Integer(
            name="person_id",
            description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
            mode="REQUIRED",
        ),
        Date(
            name="event_date",
            description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
            mode="NULLABLE",
        ),
        String(
            name="state_code",
            description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
            mode="REQUIRED",
        ),
        String(
            name="experiment_id",
            description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
            mode="NULLABLE",
        ),
        String(
            name="decarceral_impact_type",
            description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
            mode="NULLABLE",
        ),
        String(
            name="system_type",
            description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
            mode="NULLABLE",
        ),
        Bool(
            name="has_mandatory_due_date",
            description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
            mode="NULLABLE",
        ),
        Bool(
            name="is_jii_transition",
            description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
            mode="NULLABLE",
        ),
        Bool(
            name="is_during_or_after_full_state_launch_month",
            description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
            mode="NULLABLE",
        ),
        Bool(
            name="is_within_one_year_before_full_state_launch_month",
            description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
            mode="NULLABLE",
        ),
        Date(
            name="full_state_launch_date",
            description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
            mode="NULLABLE",
        ),
        Float(
            name="weight_factor",
            description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
            mode="NULLABLE",
        ),
        Integer(
            name="delta_direction_factor",
            description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
            mode="NULLABLE",
        ),
    ]


def all_impact_transitions_schema() -> list[BigQueryViewColumn]:
    """Schema for the all_impact_transitions union view."""
    return impact_transitions_base_schema() + [
        String(
            name="product_transition_type",
            description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
            mode="NULLABLE",
        ),
    ]


ALL_FULL_STATE_LAUNCH_DATES_SCHEMA = [
    String(
        name="state_code",
        description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
        mode="REQUIRED",
    ),
    String(
        name="experiment_id",
        description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
        mode="NULLABLE",
    ),
    Date(
        name="launch_date",
        description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
        mode="NULLABLE",
    ),
]


def transitions_breadth_metric_schema(
    attribute_cols: list[str] | None,
) -> list[BigQueryViewColumn]:
    """Schema for breadth metric views.

    Note: weight_factor and delta_direction_factor are STRING in breadth metric
    views because the source observation (impact_transition) stores them as strings.
    """
    base: list[BigQueryViewColumn] = [
        Date(
            name="metric_month",
            description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
            mode="NULLABLE",
        ),
    ]
    # TODO(#54941): thread BigQueryViewColumn through `attribute_col_combinations`
    # in `view_config.get_transitions_view_builders_for_views_to_update` so per-
    # attribute descriptions and types can be wired in here instead of defaulting
    # to undocumented String.
    if attribute_cols:
        for col in attribute_cols:
            base.append(
                String(
                    name=col,
                    description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                    mode="NULLABLE",
                )
            )
    base.extend(
        [
            String(
                name="weight_factor",
                description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                mode="NULLABLE",
            ),
            String(
                name="delta_direction_factor",
                description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                mode="NULLABLE",
            ),
            Integer(
                name="transitions",
                description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                mode="NULLABLE",
            ),
            Integer(
                name="new_transitions_added_via_launch",
                description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                mode="NULLABLE",
            ),
        ]
    )
    return base


def transitions_baseline_metric_schema(
    attribute_cols: list[str] | None,
) -> list[BigQueryViewColumn]:
    """Schema for baseline metric views."""
    base: list[BigQueryViewColumn] = [
        Date(
            name="metric_month",
            description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
            mode="NULLABLE",
        ),
    ]
    # TODO(#54941): thread BigQueryViewColumn through `attribute_col_combinations`
    # in `view_config.get_transitions_view_builders_for_views_to_update` so per-
    # attribute descriptions and types can be wired in here instead of defaulting
    # to undocumented String.
    if attribute_cols:
        for col in attribute_cols:
            base.append(
                String(
                    name=col,
                    description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                    mode="NULLABLE",
                )
            )
    base.extend(
        [
            String(
                name="weight_factor",
                description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                mode="NULLABLE",
            ),
            String(
                name="delta_direction_factor",
                description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                mode="NULLABLE",
            ),
            Integer(
                name="num_new_launches",
                description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                mode="NULLABLE",
            ),
            Float(
                name="monthly_baseline_old_tools",
                description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                mode="NULLABLE",
            ),
            Float(
                name="monthly_baseline_new_tools",
                description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                mode="NULLABLE",
            ),
            Float(
                name="total_baseline",
                description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                mode="NULLABLE",
            ),
        ]
    )
    return base


def transitions_depth_metric_schema(
    attribute_cols: list[str] | None,
) -> list[BigQueryViewColumn]:
    """Schema for depth metric views."""
    base: list[BigQueryViewColumn] = [
        Date(
            name="metric_month",
            description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
            mode="NULLABLE",
        ),
    ]
    # TODO(#54941): thread BigQueryViewColumn through `attribute_col_combinations`
    # in `view_config.get_transitions_view_builders_for_views_to_update` so per-
    # attribute descriptions and types can be wired in here instead of defaulting
    # to undocumented String.
    if attribute_cols:
        for col in attribute_cols:
            base.append(
                String(
                    name=col,
                    description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                    mode="NULLABLE",
                )
            )
    base.extend(
        [
            String(
                name="weight_factor",
                description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                mode="NULLABLE",
            ),
            String(
                name="delta_direction_factor",
                description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                mode="NULLABLE",
            ),
            Integer(
                name="transitions_month",
                description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                mode="NULLABLE",
            ),
            Integer(
                name="transitions_cumulative",
                description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                mode="NULLABLE",
            ),
            Float(
                name="total_baseline",
                description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                mode="NULLABLE",
            ),
            Float(
                name="total_baseline_cumulative",
                description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                mode="NULLABLE",
            ),
            Float(
                name="transitions_delta_month",
                description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                mode="NULLABLE",
            ),
            Float(
                name="transitions_delta_cumulative",
                description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
                mode="NULLABLE",
            ),
        ]
    )
    return base
