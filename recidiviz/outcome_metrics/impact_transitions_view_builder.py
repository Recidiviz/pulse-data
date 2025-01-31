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
"""Defines TransitionsViewBuilder class, which represents a set of transition events and their attributes"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import TRANSITIONS_DATASET_ID
from recidiviz.outcome_metrics.product_transition_type import ProductTransitionType


class ImpactTransitionsBigQueryViewBuilder(SimpleBigQueryViewBuilder):
    """Represents a set of transitions that count toward org-wide KPI's,
    including the event observations along with additional attributes about the type
    of transition event and multiplying factors that can be used to aggregate across
    different event types."""

    def __init__(
        self,
        *,
        # Source query template containing an event_date along with other attributes of this transition
        query_template: str,
        # Description of the transitions represented by this view builder
        description: str,
        # Name of the numeric field in query template that represents a multiplying factor
        # (positive), which determines how this transition should be weighted
        # relative to other transitions when aggregating across transitions.
        weight_factor: float,
        # Name of the numeric field in query template that represents the directionality
        # of this transition, used to aggregate across multiple transition delta
        # metrics. Value can either be -1 or 1, where -1 indicates that an increase
        # in this transition should reduce the overall delta metric; +1 means that
        # an increase in this transition should contribute positively to the overall
        # metric. So for events that we hope to reduce in frequency (e.g., overdue
        # mandatory workflows transitions, incarcerations), this value should be -1.
        delta_direction_factor: int,
        # A string that indicates the kind of product for which this transitions
        # view builder counts events.
        product_transition_type: ProductTransitionType,
    ) -> None:
        view_id = f"transitions_{product_transition_type.pretty_name}"

        if delta_direction_factor not in [-1, 1]:
            raise ValueError(
                "`delta_direction_factor` must be either -1 (negative transition) or +1 (positive transition)"
            )

        if weight_factor <= 0:
            raise ValueError("`weight_factor` param must be strictly positive")

        query_template_with_select_columns = f"""
SELECT
    person_id,
    event_date,
    state_code,
    decarceral_impact_type,
    system_type,
    CAST(has_mandatory_due_date AS BOOL) AS has_mandatory_due_date,
    CAST(is_jii_transition AS BOOL) AS is_jii_transition,
    event_date >= DATE_TRUNC(full_state_launch_date, MONTH) AS is_after_full_state_launch_month,
    event_date
        BETWEEN DATE_SUB(DATE_TRUNC(full_state_launch_date, MONTH), INTERVAL 1 YEAR)
        AND DATE_SUB(DATE_TRUNC(full_state_launch_date, MONTH), INTERVAL 1 DAY)
    AS is_within_one_year_before_full_state_launch_month,
    full_state_launch_date AS full_state_launch_date,
    {weight_factor} AS weight_factor,
    {delta_direction_factor} AS delta_direction_factor,
FROM ({query_template})
        """
        super().__init__(
            dataset_id=TRANSITIONS_DATASET_ID,
            view_id=view_id,
            description=description,
            view_query_template=query_template_with_select_columns,
            should_materialize=True,
            materialized_address_override=None,
            projects_to_deploy=None,
            should_deploy_predicate=None,
            clustering_fields=None,
        )
        self.weight_factor = weight_factor
        self.delta_direction_factor = delta_direction_factor
        self.product_transition_type = product_transition_type
