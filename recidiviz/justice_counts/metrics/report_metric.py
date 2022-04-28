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
"""Base class for the reported value(s) for a Justice Counts metric."""

from typing import Any, Dict, List, Optional

import attr

from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.metrics.constants import ContextKey
from recidiviz.justice_counts.metrics.metric_definition import MetricDefinition
from recidiviz.justice_counts.metrics.metric_registry import METRIC_KEY_TO_METRIC


@attr.define()
class ReportedContext:
    """An agency's response to a `Context` field. The `key` should be a unique identifier
    that matches the `Context` object, and `value` should be what the agency reported.
    """

    key: ContextKey
    value: Any


@attr.define()
class ReportedAggregatedDimension:
    """Values entered by the agency for a given `AggregatedDimension`. The `dimension_to_value`
    dictionary should map `Dimension` enum values to numeric values.
    """

    dimension_to_value: Dict[DimensionBase, Optional[float]] = attr.field()

    @dimension_to_value.validator
    def validate(self, _attribute: attr.Attribute, value: Any) -> None:
        # Validate that all dimensions enum instances in the dictionary belong
        # to the same dimension enum class
        dimension_classes = [d.__class__ for d in value.keys()]
        if not all(d == dimension_classes[0] for d in dimension_classes):
            raise ValueError(
                "Cannot instantiate ReportedAggregated Dimension: "
                + "Not all dimension instances belong to the same class."
            )

        # Validate that all members of the dimension enum class are present
        # in the dictionary
        if not set(dimension_classes[0]) == set(value.keys()):
            raise ValueError(
                "Cannot instantiate ReportedAggregatedDimension: "
                + "Not all members of the dimension enum have a reported value.",
            )

    def dimension_identifier(self) -> str:
        # Identifier of the Dimension class that this breakdown corresponds to
        # e.g. if `dimension_to_value` is `{Gender.FEMALE: 10, Gender.MALE: 5}`
        # then this returns `Gender.FEMALE.__class__.dimensions_identifier()`
        return list(self.dimension_to_value.keys())[0].__class__.dimension_identifier()


@attr.define()
class ReportMetric:
    """Represents an agency's reported values for a Justice Counts metric.
    If the agency has not filled out a field yet, the value will be None.
    """

    # The key of the metric (i.e. `MetricDefinition.key`) that is being reported
    key: str
    # The value entered for the metric. If the metric has breakdowns, this is the
    # total, aggregate value summed across all dimensions.
    value: Optional[float] = attr.field()

    # Additional context that the agency reported on this metric
    contexts: Optional[List[ReportedContext]] = attr.field(default=None)
    # Values for aggregated dimensions
    aggregated_dimensions: Optional[List[ReportedAggregatedDimension]] = attr.field(
        default=None
    )

    # Whether or not to enforce that required fields are populated.
    # Typically we will validate when a report is published, but not before, because
    # we want to allow agencies to submit reports in an unfinished, draft state.
    # TODO(#12418) [Backend] Figure out when/when not to validate ReportMetrics
    enforce_required_fields: Optional[bool] = False

    @value.validator
    def validate_value(self, _attribute: attr.Attribute, value: Any) -> None:
        # Validate that for each reported aggregate dimension for which sum_to_total = True,
        # the reported values for this aggregate dimension sum to the total value metric
        dimension_identifier_to_reported_dimension = {
            dimension.dimension_identifier(): dimension
            for dimension in self.aggregated_dimensions or {}
        }
        for dimension_definition in self.metric_definition.aggregated_dimensions or []:
            dimension_identifier = dimension_definition.dimension_identifier()
            reported_dimension = dimension_identifier_to_reported_dimension.get(
                dimension_identifier
            )
            if not reported_dimension or not dimension_definition.should_sum_to_total:
                continue

            reported_dimension_values = reported_dimension.dimension_to_value.values()
            if len([value for value in reported_dimension_values if value is None]) > 0:
                # If any dimension values haven't been reported yet, skip validation
                return

            if sum(reported_dimension_values) != value:
                raise ValueError(
                    f"Sums across dimension {dimension_identifier} do not equal "
                    "the total metric value."
                )

    @contexts.validator
    def validate_contexts(self, _attribute: attr.Attribute, value: Any) -> None:
        # Validate that any reported context is of the right type, and that
        # all required contexts have been reported
        context_key_to_reported_context = {
            context.key: context for context in value or []
        }
        for context in self.metric_definition.contexts or []:
            reported_context = context_key_to_reported_context.get(context.key)

            if not reported_context or not reported_context.value:
                if context.required and self.enforce_required_fields:
                    raise ValueError(f"The required context {context.key} is missing.")
                continue

            if not isinstance(
                reported_context.value, context.context_type.python_type()
            ):
                raise ValueError(
                    f"The context {context.key} is reported as a {type(reported_context.value)} "
                    f"but typed as a {context.context_type.python_type()}."
                )

    @aggregated_dimensions.validator
    def validate_aggregate_dimensions(
        self, _attribute: attr.Attribute, value: Any
    ) -> None:
        if not self.enforce_required_fields:
            return

        # Validate that all required aggregated dimensions have been reported
        required_dimensions = {
            dimension.dimension_identifier()
            for dimension in self.metric_definition.aggregated_dimensions or []
            if dimension.required is True
        }
        reported_dimensions = {dimension.dimension_identifier() for dimension in value}
        missing_dimensions = required_dimensions.difference(reported_dimensions)
        if len(missing_dimensions) > 0:
            raise ValueError(
                f"The following required dimensions are missing: {missing_dimensions}"
            )

    @property
    def metric_definition(self) -> MetricDefinition:
        # MetricDefinition that this ReportMetric corresponds to
        return METRIC_KEY_TO_METRIC[self.key]
