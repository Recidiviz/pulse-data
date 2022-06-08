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
"""Base class for official Justice Counts metrics."""

import enum
from typing import Dict, List, Optional, Type

import attr

from recidiviz.common.constants.justice_counts import ContextKey, ValueType
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.persistence.database.schema.justice_counts.schema import (
    MeasurementType,
    MetricType,
    ReportingFrequency,
    System,
)


class YesNoContext(enum.Enum):
    """Multiple choice options for contexts that are answered with Yes/No."""

    YES = "Yes"
    NO = "No"


class PopulationCountContextOptions(enum.Enum):
    """Multiple choice options for population count in prisons and jails."""

    WITHIN_TOTAL_POPULATION = "As a part of the total population"
    SEPARATE_FROM_POPULATION = "Separate from the total population"


class CallsRespondedOptions(enum.Enum):
    """Multiple choice options for calls for service context."""

    ALL_CALLS = "All calls"
    CALLS_RESPONDED = "Only calls responded"


class MetricCategory(enum.Enum):
    CAPACITY_AND_COST = "CAPACITY AND COST"
    OPERATIONS_AND_DYNAMICS = "OPERATIONS_AND_DYNAMICS"
    POPULATIONS = "POPULATIONS"
    PUBLIC_SAFETY = "PUBLIC_SAFETY"
    EQUITY = "EQUITY"
    FAIRNESS = "FAIRNESS"


@attr.define()
class Context:
    """Additional context that the agency is required to report on this metric.
    The `key` should be a unique identifier; `value_type` is the input type,
    `label` should be a human-readable explanation, and `required` indicates if
    this context is required or requested.
    """

    key: ContextKey
    value_type: ValueType
    required: bool
    label: str
    reporting_note: Optional[str] = None
    multiple_choice_options: Optional[Type[enum.Enum]] = None


@attr.define()
class FilteredDimension:
    """Dimension where the data to be reported should only account for certain value of
    that dimension. For instance, `FilteredDimension(dimension=PopulationType.PRISON)`
    indicates that the agency should report data for the prison population only, not those
    in jail or on population or parole.
    """

    dimension: DimensionBase


@attr.define()
class AggregatedDimension:
    """Dimension that this metric should be disaggregated by. For instance, if OffenseType
    is an AggegregatedDimension, then agencies should report a separate datapoint for
    each possible OffenseType.
    """

    dimension: Type[DimensionBase]
    # Whether this disaggregation is requested but not required.
    required: bool
    # Whether the disaggregated values should sum to the total metric value
    should_sum_to_total: bool = False
    # Text displayed above aggregated dimension breakdowns
    helper_text: Optional[str] = None
    # If disabled, don't send to the frontend to render
    # TODO(#13225) Remove SheriffBudgetType and get rid of this property
    disabled: bool = False

    def dimension_identifier(self) -> str:
        return self.dimension.dimension_identifier()


@attr.define()
class Definition:
    """A definition provided to the agency to help them report the metric."""

    term: str
    definition: str

    def to_json(self) -> Dict[str, str]:
        return {"term": self.term, "definition": self.definition}


@attr.define(frozen=True)
class MetricDefinition:
    """Represents an official Justice Counts metric. An instance
    of this class should be 1:1 with a cell in the Tier 1 chart.
    """

    # Agencies in this system are responsible for reporting this metric
    system: System
    # Metrics are unique by <system, metric_type, aggregated_dimensions>
    metric_type: MetricType
    # Each metric belongs to a particular category
    category: MetricCategory

    # Human-readable name for the metric
    display_name: str
    # Human-readable description of the metric
    description: str
    # How the metric over a time window is reduced to a single point
    measurement_type: MeasurementType
    # How often the metric should be reported
    reporting_frequencies: List[ReportingFrequency]
    # Note to agencies about how to report this metric (i.e. the ideal methodology)
    reporting_note: Optional[str] = None
    # Additional context that the agency is required to report on this metric
    specified_contexts: Optional[List[Context]] = None
    # Definitions provided to the agency to help them report this metric
    definitions: Optional[List[Definition]] = None
    # Dimensions that this metric should be disaggregated by in the reporting
    aggregated_dimensions: Optional[List[AggregatedDimension]] = None

    @property
    def key(self) -> str:
        """Returns a unique identifier across all Justice Counts metrics.
        Metrics are unique by <system, metric_type, aggregated_dimensions>
        """
        aggregated_dimension_key = ",".join(
            sorted(
                aggregation.dimension.dimension_identifier()
                for aggregation in self.aggregated_dimensions or []
            )
        )
        return "_".join(
            [
                self.system.value,
                self.metric_type.value,
                aggregated_dimension_key,
            ],
        )

    @property
    def contexts(self) -> List[Context]:
        """Returns the list of contexts associated with the metric.
        Appends an additional context to the list of required/requested contexts. Returns
        only a list containing the additional contexts if no contexts are associated with the metric.
        """
        additional_context: List[Context] = [
            Context(
                key=ContextKey.ADDITIONAL_CONTEXT,
                value_type=ValueType.TEXT,
                label="Please provide additional context.",
                required=False,
            )
        ]

        return (
            self.specified_contexts + additional_context
            if self.specified_contexts is not None
            else additional_context
        )
