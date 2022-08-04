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

import enum
import itertools
from typing import Any, Dict, List, Optional, Set, Type, TypeVar

import attr

from recidiviz.common.constants.justice_counts import ContextKey, ValueType
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.dimensions.dimension_registry import (
    DIMENSION_IDENTIFIER_TO_DIMENSION,
)
from recidiviz.justice_counts.exceptions import (
    JusticeCountsBadRequestError,
    JusticeCountsDataError,
)
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
    Context,
    MetricDefinition,
)
from recidiviz.justice_counts.metrics.metric_registry import (
    METRIC_KEY_TO_METRIC,
    METRICS_BY_SYSTEM,
)
from recidiviz.persistence.database.schema.justice_counts import schema

MetricContextDataT = TypeVar("MetricContextDataT", bound="MetricContextData")
MetricAggregatedDimensionDataT = TypeVar(
    "MetricAggregatedDimensionDataT", bound="MetricAggregatedDimensionData"
)
MetricInterfaceT = TypeVar("MetricInterfaceT", bound="MetricInterface")


class DatapointGetRequestEntryPoint(enum.Enum):
    REPORT_PAGE = "REPORT_PAGE"
    METRICS_TAB = "METRICS_TAB"


@attr.define()
class MetricContextData:
    """An agency's response to a `Context` field. The `key` should be a unique identifier
    that matches the `Context` object, and `value` should be what the agency reported.
    """

    key: ContextKey
    value: Any

    def to_json(self, context_definition: Context) -> Dict[str, Any]:
        value = self.value
        multiple_choice_options = []
        if context_definition.value_type == ValueType.MULTIPLE_CHOICE:
            for elem in context_definition.multiple_choice_options or []:
                multiple_choice_options.append(elem.value)

        return {
            "key": self.key.value,
            "reporting_note": context_definition.reporting_note,
            "display_name": context_definition.label,
            "type": context_definition.value_type.value,
            "required": context_definition.required,
            "value": value,
            "multiple_choice_options": multiple_choice_options,
        }

    @classmethod
    def from_json(
        cls: Type[MetricContextDataT],
        json: Dict[str, Any],
    ) -> MetricContextDataT:
        key = ContextKey[json["key"]]
        value = json["value"]
        return cls(key=key, value=value)


@attr.define()
class MetricAggregatedDimensionData:
    """Values entered by the agency for a given `AggregatedDimension`. The `dimension_to_value`
    dictionary should map `Dimension` enum values to numeric values.
    """

    dimension_to_value: Optional[Dict[DimensionBase, Any]] = attr.field(default=None)
    dimension_to_enabled_status: Optional[Dict[DimensionBase, Any]] = attr.field(
        default=None
    )

    @dimension_to_value.validator
    def validate(self, _attribute: attr.Attribute, value: Any) -> None:
        # Validate that all dimensions enum instances in the dictionary belong
        # to the same dimension enum class
        if self.dimension_to_value is None:
            return
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
                "Cannot instantiate MetricAggregatedDimensionData: "
                + "Not all members of the dimension enum have a reported value.",
            )

    def dimension_identifier(self) -> str:
        # Identifier of the Dimension class that this breakdown corresponds to
        # e.g. if `dimension_to_value` is `{Gender.FEMALE: 10, Gender.MALE: 5}`
        # then this returns `Gender.FEMALE.__class__.dimensions_identifier()`
        dimension_dict = (
            self.dimension_to_enabled_status
            if self.dimension_to_enabled_status
            else self.dimension_to_value
        )
        if dimension_dict is not None:
            return list(dimension_dict.keys())[0].__class__.dimension_identifier()

        raise JusticeCountsBadRequestError(
            code="no_dimension_data",
            description="Metric has no dimension_to_enabled_status or dimension_to_value dictionary.",
        )

    def dimension_to_json(
        self, entry_point: DatapointGetRequestEntryPoint
    ) -> List[Dict[str, Any]]:
        """This method would be called in two scenarios: 1) We are getting the json of
        a report metric with will have both dimension_to_enabled_status and dimension_to_value
        populated or 2) We are getting the json of an agency metric with will only have
        dimension_to_enabled_status populated."""
        dimensions = []
        if self.dimension_to_enabled_status is not None:
            for dimension, status in self.dimension_to_enabled_status.items():
                json = {
                    "key": dimension.to_enum().value,
                    "label": dimension.dimension_value,
                    "enabled": status,
                }
                if (
                    self.dimension_to_value is not None
                    and entry_point == DatapointGetRequestEntryPoint.REPORT_PAGE
                ):
                    # if there is a non-null dimension_to_value dictionary, add dimension
                    # values into the json
                    json["value"] = self.dimension_to_value.get(dimension)

                elif (
                    self.dimension_to_value is None
                    and entry_point == DatapointGetRequestEntryPoint.REPORT_PAGE
                ):
                    raise JusticeCountsDataError(
                        code="no_dimension_values",
                        description=f"Metric {dimension.to_enum().value} has no dimension values",
                    )
                dimensions.append(json)
        return dimensions

    def to_json(
        self,
        dimension_definition: AggregatedDimension,
        entry_point: DatapointGetRequestEntryPoint,
    ) -> Dict[str, Any]:
        is_disaggregation_enabled = (
            self.dimension_to_enabled_status is not None
            and any(list(self.dimension_to_enabled_status.values()))
        )  # A disaggregation is enabled if at least one of it's dimensions is enabled.

        return {
            "key": dimension_definition.dimension.dimension_identifier(),
            "helper_text": dimension_definition.helper_text,
            "required": dimension_definition.required,
            "should_sum_to_total": dimension_definition.should_sum_to_total,
            "display_name": dimension_definition.display_name
            or dimension_definition.dimension.display_name(),
            "dimensions": self.dimension_to_json(entry_point=entry_point),
            "enabled": is_disaggregation_enabled,
        }

    @classmethod
    def from_json(
        cls: Type[MetricAggregatedDimensionDataT],
        json: Dict[str, Any],
        entry_point: DatapointGetRequestEntryPoint,
    ) -> MetricAggregatedDimensionDataT:
        """
        - The input json is expected to be of the format {dimension name -> value/enabled}, e.g. {"BLACK": 50} for report
          datapoints or {"BLACK": True} for agency datapoints.
        - The input json does not need to include all dimension names, i.e. it can be partial/incomplete. Values/enabled status'
          that are not reported will have values of None (e.g {"BLACK": None} for report datapoints or {"BLACK": None}). None values
          for report datapoints represent values that have not been reported. None values for agency datapoints represent metric configuration
          values that have not been changed.
        - This function will create a dimension_to_value or a dimension_to_enabled_status dictionary that
          does include all dimension names.
        - The dimensions that were reported in json will be copied over to dimension_to_value/dimension_to_enabled_status dict.
        """
        # default_dimension_enabled_status will be True or False if a disaggregation is being turned off/on,
        # and None otherwise.
        default_dimension_enabled_status = json.get("enabled")
        value_key = (
            "value"
            if entry_point == DatapointGetRequestEntryPoint.REPORT_PAGE
            else "enabled"
        )
        # convert dimension name -> value/enabled mapping to dimension class -> value/enabled mapping
        # e.g "BLACK" : 50 -> RaceAndEthnicity().BLACK : 50
        dimension_class = DIMENSION_IDENTIFIER_TO_DIMENSION[
            json["key"]
        ]  # example: RaceAndEthnicity
        dimensions = json.get("dimensions")
        dimension_enum_value_to_value = {
            dim["key"]: dim[value_key] for dim in dimensions or []
        }  # example: {"BLACK": 50, "WHITE": 20, ...} if a report metric
        # or {"BLACK": True, "WHITE": False, ...} if it is an agency metric
        dimension_to_value = {
            dimension: dimension_enum_value_to_value.get(
                dimension.to_enum().value,
                None
                if entry_point == DatapointGetRequestEntryPoint.REPORT_PAGE
                else default_dimension_enabled_status,
            )
            for dimension in dimension_class
        }  # example: {RaceAndEthnicity.BLACK: 50, RaceAndEthnicity.WHITE: 20}
        if entry_point == DatapointGetRequestEntryPoint.REPORT_PAGE:
            return cls(dimension_to_value=dimension_to_value)
        return cls(dimension_to_enabled_status=dimension_to_value)


@attr.define()
class MetricInterface:
    """Represents an agency's reported values for a Justice Counts metric.
    If the agency has not filled out a field yet, the value will be None.
    """

    # The key of the metric (i.e. `MetricDefinition.key`) that is being reported
    key: str
    # The value entered for the metric. If the metric has breakdowns, this is the
    # total, aggregate value summed across all dimensions.
    value: Optional[float] = attr.field(default=None)
    # Weather or not the metric is enabled for an agency.
    is_metric_enabled: bool = attr.field(default=True)
    # Additional context that the agency reported on this metric
    contexts: List[MetricContextData] = attr.field(factory=list)
    # Values for aggregated dimensions
    aggregated_dimensions: List[MetricAggregatedDimensionData] = attr.field(
        factory=list
    )

    # TODO(#12418) [Backend] Figure out when/when not to validate MetricInterfaces
    enforce_validation: Optional[bool] = False

    @value.validator
    def validate_value(self, _attribute: attr.Attribute, value: Any) -> None:
        """Validate that for each reported aggregate dimension for which sum_to_total = True,
        the reported values for this aggregate dimension sum to the total value metric"""

        if value is None or not self.enforce_validation:
            return

        dimension_identifier_to_reported_dimension = {
            dimension.dimension_identifier(): dimension
            for dimension in self.aggregated_dimensions
        }
        for dimension_definition in self.metric_definition.aggregated_dimensions or []:
            dimension_identifier = dimension_definition.dimension_identifier()
            reported_dimension = dimension_identifier_to_reported_dimension.get(
                dimension_identifier
            )
            if (
                not reported_dimension
                or not dimension_definition.should_sum_to_total
                or reported_dimension.dimension_to_value is None
            ):
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
        if not self.enforce_validation:
            return

        context_key_to_reported_context = {
            context.key: context for context in value or []
        }
        for context in self.metric_definition.contexts or []:
            reported_context = context_key_to_reported_context.get(context.key)

            if not reported_context or not reported_context.value:
                if context.required:
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
        if not self.enforce_validation:
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
        # MetricDefinition that this MetricInterface corresponds to
        return METRIC_KEY_TO_METRIC[self.key]

    def to_json(self, entry_point: DatapointGetRequestEntryPoint) -> Dict[str, Any]:
        dimension_id_to_dimension_definition = {
            d.dimension_identifier(): d
            for d in self.metric_definition.aggregated_dimensions or []
        }
        context_key_to_context_definition = {
            context.key: context for context in self.metric_definition.contexts or []
        }
        if len(self.metric_definition.reporting_frequencies) > 1:
            raise JusticeCountsDataError(
                code="more_than_one_frequency",
                description="Metric has more than one frequency associated with it",
            )

        frequency = self.metric_definition.reporting_frequencies[0].value
        return {
            "key": self.key,
            "system": self.metric_definition.system.value.replace("_", " ")
            .title()
            .replace("And", "and"),
            "display_name": self.metric_definition.display_name,
            "description": self.metric_definition.description,
            "reporting_note": self.metric_definition.reporting_note,
            "value": self.value,
            "unit": self.metric_definition.metric_type.unit,
            "category": self.metric_definition.category.value,
            "label": self.metric_definition.display_name,
            "enabled": self.is_metric_enabled,
            "frequency": frequency,
            "definitions": [
                d.to_json() for d in self.metric_definition.definitions or []
            ],
            "contexts": [
                c.to_json(context_definition=context_key_to_context_definition[c.key])
                for c in self.contexts
            ],
            "disaggregations": [
                d.to_json(
                    dimension_definition=dimension_id_to_dimension_definition[
                        d.dimension_identifier()
                    ],
                    entry_point=entry_point,
                )
                for d in self.aggregated_dimensions
            ],
        }

    @classmethod
    def from_json(
        cls: Type[MetricInterfaceT],
        json: Dict[str, Any],
        entry_point: DatapointGetRequestEntryPoint,
    ) -> MetricInterfaceT:
        """Creates a instance of a MetricInterface from a formatted json."""
        metric_context_data = [
            MetricContextData.from_json(
                json=context_json,
            )
            for context_json in json.get("contexts", [])
        ]

        disaggregations = []
        for dimension_json in json.get("disaggregations", []):
            disaggregations.append(
                MetricAggregatedDimensionData.from_json(
                    json=dimension_json, entry_point=entry_point
                )
            )

        if (
            "value" not in json
            and entry_point is DatapointGetRequestEntryPoint.REPORT_PAGE
        ):
            raise JusticeCountsBadRequestError(
                code="report_metric_no_value",
                description="No value field is included in request json",
            )

        return cls(
            key=json["key"],
            value=json.get("value"),
            contexts=metric_context_data,
            aggregated_dimensions=disaggregations,
            is_metric_enabled=json.get("enabled", True),
            # TODO(#13556) Backend validation needs to match new frontend validation
            # Right now, if you only publish a subset of the metrics, this will error
            # enforce_validation=report_status=ReportStatus.PUBLISHED
            enforce_validation=False,
        )

    @staticmethod
    def get_metric_definitions(
        systems: Set[schema.System],
        report_type: Optional[schema.ReportingFrequency] = None,
    ) -> List[MetricDefinition]:
        """Given a list of systems and report frequency, return all
        MetricDefinitions that belong to one of the systems and have
        the given frequency."""
        metrics = list(
            itertools.chain(
                *[
                    METRICS_BY_SYSTEM[system.value]
                    # Sort systems so that Supervision comes before Parole/Probation
                    # in the UI
                    for system in sorted(
                        list(systems), key=lambda x: x.value, reverse=True
                    )
                ]
            )
        )
        metric_definitions = []
        for metric in metrics:
            if report_type is not None and report_type not in {
                freq.value for freq in metric.reporting_frequencies
            }:
                continue
            if metric.disabled:
                continue
            metric_definitions.append(metric)

        if len(metric_definitions) == 0:
            raise JusticeCountsDataError(
                code="invalid_data",
                description="No metrics found for this report or agency.",
            )
        return metric_definitions
