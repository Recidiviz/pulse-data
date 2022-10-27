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

from recidiviz.justice_counts.exceptions import JusticeCountsServerError
from recidiviz.justice_counts.metrics.metric_context_data import MetricContextData
from recidiviz.justice_counts.metrics.metric_definition import (
    IncludesExcludesSetting,
    MetricCategory,
    MetricDefinition,
)
from recidiviz.justice_counts.metrics.metric_disaggregation_data import (
    MetricAggregatedDimensionData,
)
from recidiviz.justice_counts.metrics.metric_registry import (
    METRIC_KEY_TO_METRIC,
    METRICS_BY_SYSTEM,
)
from recidiviz.justice_counts.utils.constants import DatapointGetRequestEntryPoint
from recidiviz.persistence.database.schema.justice_counts import schema

MetricInterfaceT = TypeVar("MetricInterfaceT", bound="MetricInterface")


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
    # Values for includes_excludes settings at the metric level.
    includes_excludes_member_to_setting: Dict[
        enum.Enum, Optional[IncludesExcludesSetting]
    ] = attr.field(default={})

    # TODO(#12418) [Backend] Figure out when/when not to validate MetricInterfaces
    enforce_validation: Optional[bool] = False

    @property
    def metric_definition(self) -> MetricDefinition:
        # MetricDefinition that this MetricInterface corresponds to
        return METRIC_KEY_TO_METRIC[self.key]

    ### To/From JSON ###

    def to_json(self, entry_point: DatapointGetRequestEntryPoint) -> Dict[str, Any]:
        """Returns the json form of the MetricInterface object."""

        dimension_id_to_dimension_definition = {
            d.dimension_identifier(): d
            for d in self.metric_definition.aggregated_dimensions or []
        }
        context_key_to_context_definition = {
            context.key: context for context in self.metric_definition.contexts or []
        }

        frequency = self.metric_definition.reporting_frequency.value
        settings_json = []
        if (
            entry_point is DatapointGetRequestEntryPoint.METRICS_TAB
            and self.metric_definition.includes_excludes
        ):
            for (
                member,
                default_setting,
            ) in (
                self.metric_definition.includes_excludes.member_to_default_inclusion_setting.items()
            ):
                included = self.includes_excludes_member_to_setting.get(member)
                settings_json.append(
                    {
                        "key": member.name,
                        "label": member.value,
                        "included": included.value
                        if included is not None
                        else default_setting.value,
                        "default": default_setting.value,
                    }
                )

        return {
            "key": self.key,
            "system": self.metric_definition.system.value.replace("_", " ")
            .title()
            .replace("And", "and"),
            "display_name": self.metric_definition.display_name,
            "description": self.metric_definition.description,
            "reporting_note": self.metric_definition.reporting_note,
            "value": self.value,
            "settings": settings_json,
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
        metric_definition = METRIC_KEY_TO_METRIC[json["key"]]
        includes_excludes_member_to_setting = {}
        if (
            metric_definition.includes_excludes is not None
            and entry_point is DatapointGetRequestEntryPoint.METRICS_TAB
        ):
            actual_includes_excludes_list = json.get("settings", [])
            actual_includes_excludes_member_to_setting = {
                setting["key"]: setting["included"]
                for setting in actual_includes_excludes_list
            }
            for member in metric_definition.includes_excludes.members:
                setting = actual_includes_excludes_member_to_setting.get(member.name)
                includes_excludes_member_to_setting[member] = (
                    IncludesExcludesSetting(setting) if setting is not None else None
                )

        disaggregations = []
        dimension_id_to_definition = (
            {
                d.dimension.dimension_identifier(): d
                for d in metric_definition.aggregated_dimensions
            }
            if metric_definition.aggregated_dimensions is not None
            else {}
        )
        for dimension_json in json.get("disaggregations", []):
            dimension_definition = dimension_id_to_definition.get(dimension_json["key"])
            if dimension_definition is None:
                raise JusticeCountsServerError(
                    code="invalid_dimension_id",
                    description=f'Metric json contains an invalid dimension identifier: {dimension_json["key"]}',
                )
            disaggregations.append(
                MetricAggregatedDimensionData.from_json(
                    json=dimension_json,
                    entry_point=entry_point,
                    disaggregation_definition=dimension_definition,
                )
            )

        if (
            "value" not in json
            and entry_point is DatapointGetRequestEntryPoint.REPORT_PAGE
        ):
            raise JusticeCountsServerError(
                code="report_metric_no_value",
                description="No value field is included in request json",
            )

        return cls(
            key=json["key"],
            value=json.get("value"),
            contexts=metric_context_data,
            aggregated_dimensions=disaggregations,
            includes_excludes_member_to_setting=includes_excludes_member_to_setting,
            is_metric_enabled=json.get("enabled", True),
            # TODO(#13556) Backend validation needs to match new frontend validation
            # Right now, if you only publish a subset of the metrics, this will error
            # enforce_validation=report_status=ReportStatus.PUBLISHED
            enforce_validation=False,
        )

    ### Helpers ###

    @staticmethod
    def get_metric_definitions(
        systems: Set[schema.System],
        report_type: Optional[schema.ReportingFrequency] = None,
    ) -> List[MetricDefinition]:
        """Given a list of systems and report frequency, return all
        MetricDefinitions that belong to one of the systems and have
        the given frequency."""
        # Sort systems so that Supervision, Parole, Probation, and Post-Release
        # are always grouped together, in that order
        systems_ordered = schema.System.sort(systems=list(systems))

        metrics = list(
            itertools.chain(
                *[METRICS_BY_SYSTEM[system.value] for system in systems_ordered]
            )
        )

        if systems.intersection(
            {schema.System.PAROLE, schema.System.PROBATION, schema.System.POST_RELEASE}
        ):
            # This agency reports its metrics broken down by parole/probation/post-release,
            # so we should remove all of the generic "supervision" metrics except for the
            # cost and capacity ones (i.e. budget and staff), and we should remove the
            # cost and capacity ones from parole/probation/post-release.
            metrics = [
                metric
                for metric in metrics
                if not (
                    metric.system == schema.System.SUPERVISION
                    and metric.category != MetricCategory.CAPACITY_AND_COST
                )
                and not (
                    metric.system
                    in {
                        schema.System.PAROLE,
                        schema.System.PROBATION,
                        schema.System.POST_RELEASE,
                    }
                    and metric.category == MetricCategory.CAPACITY_AND_COST
                )
            ]

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
            raise JusticeCountsServerError(
                code="invalid_data",
                description="No metrics found for this report or agency.",
            )
        return metric_definitions

    ### Validations ###

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
