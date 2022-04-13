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
from typing import List, Optional, Type

import attr

from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.metrics.constants import ContextKey
from recidiviz.persistence.database.schema.justice_counts.schema import (
    MeasurementType,
    MetricType,
    System,
)


class ReportingFrequency(enum.Enum):
    ANNUALLY = "ANNUALLY"
    MONTHLY = "MONTHLY"


@attr.define()
class Context:
    """Additional context that the agency is required to report on this metric.
    The `key` should be a unique identifier; the `label` should be a human-readable
    explanation; and `required` indicates if this context is required or requested.
    """

    key: ContextKey
    label: str
    required: bool


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

    def dimension_identifier(self) -> str:
        return self.dimension.dimension_identifier()


@attr.define(frozen=True)
class MetricDefinition:
    """Represents an official Justice Counts metric. An instance
    of this class should be 1:1 with a cell in the Tier 1 chart.
    """

    # Agencies in this system are responsible for reporting this metric
    system: System
    # Metrics are unique by <system, metric_type, filtered_dimensions, aggregated_dimensions>
    metric_type: MetricType

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
    contexts: Optional[List[Context]] = None

    # Dimensions where the data to be reported should only account for a certain dimension value
    filtered_dimensions: Optional[List[FilteredDimension]] = None

    # Dimensions that this metric should be disaggregated by in the reporting
    aggregated_dimensions: Optional[List[AggregatedDimension]] = None

    @property
    def key(self) -> str:
        """Returns a unique identifier across all Justice Counts metrics.
        Metrics are unique by <system, metric_type, filtered_dimensions, aggregated_dimensions>
        """
        filtered_dimension_key = ",".join(
            sorted(
                filter.dimension.dimension_identifier()
                + ":"
                + filter.dimension.dimension_value
                for filter in self.filtered_dimensions or []
            )
        )
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
                filtered_dimension_key,
                aggregated_dimension_key,
            ],
        )
