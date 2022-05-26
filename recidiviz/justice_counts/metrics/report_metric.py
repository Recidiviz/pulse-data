# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.p
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

import re
from typing import Any, Dict, List, Optional, Type, TypeVar

import attr

from recidiviz.common.constants.justice_counts import ContextKey, ValueType
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.dimensions.dimension_registry import (
    DIMENSION_IDENTIFIER_TO_DIMENSION,
)
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
    Context,
    MetricDefinition,
)
from recidiviz.justice_counts.metrics.metric_registry import (
    METRIC_KEY_TO_METRIC,
    METRICS,
)

ReportedContextT = TypeVar("ReportedContextT", bound="ReportedContext")
ReportedAggregatedDimensionT = TypeVar(
    "ReportedAggregatedDimensionT", bound="ReportedAggregatedDimension"
)
ReportMetricT = TypeVar("ReportMetricT", bound="ReportMetric")


@attr.define()
class ReportedContext:
    """An agency's response to a `Context` field. The `key` should be a unique identifier
    that matches the `Context` object, and `value` should be what the agency reported.
    """

    key: ContextKey
    value: Any

    def to_json(self, context_definition: Context) -> Dict[str, Any]:
        value = self.value
        if context_definition.value_type == ValueType.BOOLEAN:
            value = "YES" if value is True else "NO"
        return {
            "key": self.key.value,
            "reporting_note": context_definition.reporting_note,
            "display_name": context_definition.label,
            "type": context_definition.value_type.value,
            "required": context_definition.required,
            "value": value,
        }

    @classmethod
    def from_json(
        cls: Type[ReportedContextT],
        json: Dict[str, Any],
        context_definition: Context,
    ) -> ReportedContextT:
        key = ContextKey[json["key"]]
        value = json["value"]
        if context_definition.value_type != ValueType.BOOLEAN:
            return cls(key=key, value=value)
        value = value == "YES"
        return cls(key=key, value=value)


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

    def dimension_to_json(self) -> List[Dict[str, Any]]:
        return [
            {
                "key": dimension.to_enum().value,
                "value": val,
                "label": re.sub(
                    "_", "-", dimension.dimension_value.title()
                ),  # replace underscore with dashes. Non_Emergency -> Non-Emergency
                "reporting_note": dimension.reporting_note,
            }
            for dimension, val in self.dimension_to_value.items()
        ]

    def to_json(self, dimension_definition: AggregatedDimension) -> Dict[str, Any]:
        return {
            "key": dimension_definition.dimension.dimension_identifier(),
            "helper_text": dimension_definition.helper_text,
            "required": dimension_definition.required,
            "should_sum_to_total": dimension_definition.should_sum_to_total,
            "display_name": dimension_definition.dimension.display_name(),
            "dimensions": self.dimension_to_json(),
        }

    @classmethod
    def from_json(
        cls: Type[ReportedAggregatedDimensionT], json: Dict[str, Any]
    ) -> ReportedAggregatedDimensionT:
        """
        - The input json is expected to be of the format {dimension name -> value}, e.g. {"BLACK": 50}
        - The input json does not need to include all dimension names, i.e. it can be partial/incomplete
        - This function will create a dimension_to_value dictionary that does include all dimension names
        - The dimensions that were reported in json will be copied over to dimension_to_value
        """
        # convert dimension name -> value mapping to dimension class -> value mapping
        # e.g "BLACK" : 50 -> RaceAndEthnicity().BLACK : 50
        dimension_class = DIMENSION_IDENTIFIER_TO_DIMENSION[
            json["key"]
        ]  # example: RaceAndEthnicity
        dimension_enum_value_to_value = {
            dim["key"]: dim["value"] for dim in json["dimensions"]
        }  # example: {"BLACK": 50, "WHITE": 20, ...}
        dimension_to_value = {
            dimension: dimension_enum_value_to_value.get(dimension.to_enum().value)
            for dimension in dimension_class
        }  # example: {RaceAndEthnicity.BLACK: 50, RaceAndEthnicity.WHITE: 20}
        return cls(dimension_to_value=dimension_to_value)


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

        if value is None:
            return

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

            # we know at this point that no values are None, but add the filter explicitly to make
            # mypy happy
            if sum(filter(None, reported_dimension_values)) != value:
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

            if not isinstance(reported_context.value, context.value_type.python_type()):
                raise ValueError(
                    f"The context {context.key} is reported as a {type(reported_context.value)} "
                    f"but typed as a {context.value_type.python_type()}."
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

    def to_json(self) -> Dict[str, Any]:
        dimension_id_to_dimension_definition = {
            d.dimension_identifier(): d
            for d in self.metric_definition.aggregated_dimensions or []
        }
        context_key_to_context_definition = {
            context.key: context for context in self.metric_definition.contexts or []
        }
        return {
            "key": self.key,
            "display_name": self.metric_definition.display_name,
            "description": self.metric_definition.description,
            "reporting_note": self.metric_definition.reporting_note,
            "value": self.value,
            "unit": self.metric_definition.metric_type.unit,
            "category": self.metric_definition.category.value,
            "label": self.metric_definition.display_name,
            "definitions": [
                d.to_json() for d in self.metric_definition.definitions or []
            ],
            "contexts": [
                c.to_json(context_definition=context_key_to_context_definition[c.key])
                for c in self.contexts or []
            ],
            "disaggregations": [
                d.to_json(
                    dimension_definition=dimension_id_to_dimension_definition[
                        d.dimension_identifier()
                    ]
                )
                for d in self.aggregated_dimensions or []
            ],
        }

    @classmethod
    def from_json(cls: Type[ReportMetricT], json: Dict[str, Any]) -> ReportMetricT:
        context_key_to_context = {}
        for metric in METRICS:
            for context in metric.contexts:
                if context.key.value not in context_key_to_context:
                    context_key_to_context[context.key.value] = context

        reported_contexts = [
            ReportedContext.from_json(
                json=context_json,
                context_definition=context_key_to_context[context_json["key"]],
            )
            for context_json in json.get("contexts", [])
        ]
        disaggregations = []
        for dimension_json in json.get("disaggregations", []):
            disaggregations.append(
                ReportedAggregatedDimension.from_json(json=dimension_json)
            )

        return cls(
            key=json["key"],
            value=json["value"],
            contexts=reported_contexts,
            aggregated_dimensions=disaggregations,
        )
