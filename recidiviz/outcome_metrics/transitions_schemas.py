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
    BigQueryViewColumn,
    Bool,
    Date,
    Float,
    Integer,
    String,
)

_WEIGHT_FACTOR_DESCRIPTION = (
    "How much this transition counts relative to others when "
    "transitions from different product types are combined."
)

_DELTA_DIRECTION_FACTOR_DESCRIPTION = (
    "1 if more of this transition should raise the depth metric, or -1 if it "
    "should lower it."
)

_METRIC_MONTH_DESCRIPTION = "The first day of the month the metric covers."

_TOTAL_BASELINE_DESCRIPTION = (
    "The expected number of transitions per month without the launches, "
    "monthly_baseline_old_tools plus monthly_baseline_new_tools."
)


_STATE_CODE_DESCRIPTION = "The state where the transition happened."

_PRODUCT_TRANSITION_TYPE_DESCRIPTION = (
    "The product the transition is attributed to, as the lowercase name of a "
    "ProductTransitionType value (e.g. workflows_discretionary)."
)

_DECARCERAL_IMPACT_TYPE_DESCRIPTION = (
    "The type of movement toward greater liberty that the transition facilitates,"
    " as a DecarceralImpactType value (e.g. DOWNGRADE_CUSTODY_LEVEL). "
    "NOT_APPLICABLE for supervisor outcomes module transitions."
)

_HAS_MANDATORY_DUE_DATE_DESCRIPTION = (
    "Whether the transition's task has a mandatory due date."
)

_IS_JII_TRANSITION_DESCRIPTION = (
    "Whether the transition is itself a movement toward greater liberty for the "
    "justice-impacted individual, such as a release from solitary confinement, "
    "rather than an administrative step that helps bring one about, such as a "
    "review hearing."
)


def impact_transitions_base_schema() -> list[BigQueryViewColumn]:
    """Base schema for individual impact transitions views."""
    return [
        Integer(
            name="person_id",
            description="The person who experienced the transition.",
            mode="REQUIRED",
        ),
        Date(
            name="event_date",
            description="The date the transition happened.",
            mode="NULLABLE",
        ),
        String(
            name="state_code",
            description=_STATE_CODE_DESCRIPTION,
            mode="REQUIRED",
        ),
        String(
            name="experiment_id",
            description=(
                "The experiment_id of the launch this transition is attributed to,"
                " which links the transition to its launch date in experiment_assignments."
            ),
            mode="NULLABLE",
        ),
        String(
            name="decarceral_impact_type",
            description=_DECARCERAL_IMPACT_TYPE_DESCRIPTION,
            mode="NULLABLE",
        ),
        String(
            name="system_type",
            description=(
                "The part of the system the transition happens in: INCARCERATION or "
                "SUPERVISION for Workflows and supervisor module transitions, PRE-SENTENCING "
                "for PSI case insights transitions."
            ),
            mode="NULLABLE",
        ),
        Bool(
            name="has_mandatory_due_date",
            description=_HAS_MANDATORY_DUE_DATE_DESCRIPTION,
            mode="NULLABLE",
        ),
        Bool(
            name="is_jii_transition",
            description=_IS_JII_TRANSITION_DESCRIPTION,
            mode="NULLABLE",
        ),
        Bool(
            name="is_during_or_after_full_state_launch_month",
            description=(
                "Whether the transition happened in or after the month of "
                "full_state_launch_date, or in or after the month of the pilot launch for TN "
                "compliant reporting 2025 policy transitions in pilot districts. Breadth counts only transitions where this "
                "is true."
            ),
            mode="NULLABLE",
        ),
        Bool(
            name="is_within_one_year_before_full_state_launch_month",
            description=(
                "Whether the transition happened in the 12 months before the month of "
                "full_state_launch_date. Depth uses these transitions as the baseline for a "
                "new launch."
            ),
            mode="NULLABLE",
        ),
        Date(
            name="full_state_launch_date",
            description=(
                "The date the tool for this transition launched to the full state. For "
                "the TN compliant reporting 2025 policy, transitions in a pilot district hold"
                " that district's pilot launch date instead."
            ),
            mode="NULLABLE",
        ),
        Float(
            name="weight_factor",
            description=_WEIGHT_FACTOR_DESCRIPTION,
            mode="NULLABLE",
        ),
        Integer(
            name="delta_direction_factor",
            description=_DELTA_DIRECTION_FACTOR_DESCRIPTION,
            mode="NULLABLE",
        ),
    ]


def all_impact_transitions_schema() -> list[BigQueryViewColumn]:
    """Schema for the all_impact_transitions union view."""
    return impact_transitions_base_schema() + [
        String(
            name="product_transition_type",
            description=_PRODUCT_TRANSITION_TYPE_DESCRIPTION,
            mode="NULLABLE",
        ),
    ]


ALL_FULL_STATE_LAUNCH_DATES_SCHEMA = [
    String(
        name="state_code",
        description="The state where the tool launched.",
        mode="REQUIRED",
    ),
    String(
        name="experiment_id",
        description="The experiment that represents the tool's launch.",
        mode="NULLABLE",
    ),
    Date(
        name="launch_date",
        description="The date the tool launched statewide.",
        mode="NULLABLE",
    ),
]


STATE_CODE_ATTRIBUTE = String(
    name="state_code",
    description=_STATE_CODE_DESCRIPTION,
    mode="NULLABLE",
)
PRODUCT_TRANSITION_TYPE_ATTRIBUTE = String(
    name="product_transition_type",
    description=_PRODUCT_TRANSITION_TYPE_DESCRIPTION,
    mode="NULLABLE",
)
DECARCERAL_IMPACT_TYPE_ATTRIBUTE = String(
    name="decarceral_impact_type",
    description=_DECARCERAL_IMPACT_TYPE_DESCRIPTION,
    mode="NULLABLE",
)
HAS_MANDATORY_DUE_DATE_ATTRIBUTE = String(
    name="has_mandatory_due_date",
    description=_HAS_MANDATORY_DUE_DATE_DESCRIPTION,
    mode="NULLABLE",
)
IS_JII_TRANSITION_ATTRIBUTE = String(
    name="is_jii_transition",
    description=_IS_JII_TRANSITION_DESCRIPTION,
    mode="NULLABLE",
)

TRANSITIONS_ATTRIBUTE_COL_COMBINATIONS: list[list[BigQueryViewColumn]] = [
    [STATE_CODE_ATTRIBUTE],
    [PRODUCT_TRANSITION_TYPE_ATTRIBUTE],
    [
        DECARCERAL_IMPACT_TYPE_ATTRIBUTE,
        HAS_MANDATORY_DUE_DATE_ATTRIBUTE,
        IS_JII_TRANSITION_ATTRIBUTE,
    ],
]
"""Sets of attribute columns used to disaggregate the breadth, baseline, and depth
metric views. One set of views is generated per combination per metric year. Every
attribute is a STRING because these views read from the impact_transition
observation view, which stores every attribute as a string."""


def transitions_breadth_metric_schema(
    attribute_cols: list[BigQueryViewColumn] | None,
) -> list[BigQueryViewColumn]:
    """Schema for breadth metric views.

    Note: weight_factor and delta_direction_factor are STRING in breadth metric
    views because the source observation (impact_transition) stores them as strings.
    """
    base: list[BigQueryViewColumn] = [
        Date(
            name="metric_month",
            description=_METRIC_MONTH_DESCRIPTION,
            mode="NULLABLE",
        ),
    ]
    base.extend(attribute_cols or [])
    base.extend(
        [
            String(
                name="weight_factor",
                description=_WEIGHT_FACTOR_DESCRIPTION,
                mode="NULLABLE",
            ),
            String(
                name="delta_direction_factor",
                description=_DELTA_DIRECTION_FACTOR_DESCRIPTION,
                mode="NULLABLE",
            ),
            Integer(
                name="transitions",
                description="The number of distinct transitions in the month from tools in or after their full-state launch month.",
                mode="NULLABLE",
            ),
            Integer(
                name="new_transitions_added_via_launch",
                description=(
                    "The number of those transitions from tools whose full-state launch was in this "
                    "month, excluding supervisor opportunities module launches."
                ),
                mode="NULLABLE",
            ),
        ]
    )
    return base


def transitions_baseline_metric_schema(
    attribute_cols: list[BigQueryViewColumn] | None,
) -> list[BigQueryViewColumn]:
    """Schema for baseline metric views."""
    base: list[BigQueryViewColumn] = [
        Date(
            name="metric_month",
            description=_METRIC_MONTH_DESCRIPTION,
            mode="NULLABLE",
        ),
    ]
    base.extend(attribute_cols or [])
    base.extend(
        [
            String(
                name="weight_factor",
                description=_WEIGHT_FACTOR_DESCRIPTION,
                mode="NULLABLE",
            ),
            String(
                name="delta_direction_factor",
                description=_DELTA_DIRECTION_FACTOR_DESCRIPTION,
                mode="NULLABLE",
            ),
            Integer(
                name="num_new_launches",
                description="The number of tools with a full-state launch in this month.",
                mode="NULLABLE",
            ),
            Float(
                name="monthly_baseline_old_tools",
                description=(
                    "For tools launched before the metric year, the average monthly transitions in "
                    "the year before the metric year."
                ),
                mode="NULLABLE",
            ),
            Float(
                name="monthly_baseline_new_tools",
                description=(
                    "For tools launched during the metric year, in or before this month, the average "
                    "monthly transitions in the 12 months before each tool's launch month."
                ),
                mode="NULLABLE",
            ),
            Float(
                name="total_baseline",
                description=_TOTAL_BASELINE_DESCRIPTION,
                mode="NULLABLE",
            ),
        ]
    )
    return base


def transitions_depth_metric_schema(
    attribute_cols: list[BigQueryViewColumn] | None,
) -> list[BigQueryViewColumn]:
    """Schema for depth metric views."""
    base: list[BigQueryViewColumn] = [
        Date(
            name="metric_month",
            description=_METRIC_MONTH_DESCRIPTION,
            mode="NULLABLE",
        ),
    ]
    base.extend(attribute_cols or [])
    base.extend(
        [
            String(
                name="weight_factor",
                description=_WEIGHT_FACTOR_DESCRIPTION,
                mode="NULLABLE",
            ),
            String(
                name="delta_direction_factor",
                description=_DELTA_DIRECTION_FACTOR_DESCRIPTION,
                mode="NULLABLE",
            ),
            Integer(
                name="transitions_month",
                description="The number of transitions in the month, from the breadth metric.",
                mode="NULLABLE",
            ),
            Integer(
                name="transitions_cumulative",
                description="The number of transitions from the start of the metric year through this month.",
                mode="NULLABLE",
            ),
            Float(
                name="total_baseline",
                description=_TOTAL_BASELINE_DESCRIPTION,
                mode="NULLABLE",
            ),
            Float(
                name="total_baseline_cumulative",
                description="The sum of total_baseline from the start of the metric year through this month.",
                mode="NULLABLE",
            ),
            Float(
                name="transitions_delta_month",
                description="The percent change from total_baseline to transitions_month, as a fraction (0.1 is 10% above baseline).",
                mode="NULLABLE",
            ),
            Float(
                name="transitions_delta_cumulative",
                description=(
                    "The percent change from total_baseline_cumulative to transitions_cumulative, as a "
                    "fraction (0.1 is 10% above baseline)."
                ),
                mode="NULLABLE",
            ),
        ]
    )
    return base
