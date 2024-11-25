# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests functionality of MetricPopulationType functions"""

import re
import unittest

from recidiviz.aggregated_metrics.assignment_sessions_view_collector import (
    get_standard_population_selector_for_unit_of_observation,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.observations.observation_big_query_view_collector import (
    ObservationBigQueryViewCollector,
)
from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType


class MetricPopulationsByTypeTest(unittest.TestCase):
    """Tests functionality of MetricPopulationType functions"""

    # check that population_name_short only has valid character types
    def test_population_name_short_char_types(self) -> None:
        for e in MetricPopulationType:
            if not re.match(r"^\w+$", e.population_name_short):
                raise ValueError(
                    "All characters in MetricPopulationType value must be alphanumeric or underscores."
                )

    # Check that all attribute filters are compatible with person spans in configured
    # MetricPopulation
    def test_compatible_person_span_filters(self) -> None:
        collector = ObservationBigQueryViewCollector()
        span_builders_by_span_type: dict[
            SpanType, SpanObservationBigQueryViewBuilder
        ] = {b.span_type: b for b in collector.collect_span_builders()}

        for population_type in MetricPopulationType:
            if population_type is MetricPopulationType.CUSTOM:
                continue

            for unit_of_observation in MetricUnitOfObservationType:
                span_selector = (
                    get_standard_population_selector_for_unit_of_observation(
                        population_type, unit_of_observation
                    )
                )

                if not span_selector:
                    continue

                span_type = span_selector.span_type
                supported_attributes = span_builders_by_span_type[
                    span_type
                ].attribute_cols
                for attribute in span_selector.span_conditions_dict:
                    if attribute not in supported_attributes:
                        raise ValueError(
                            f"Span attribute `{attribute}` is not supported by "
                            f"{span_type.value} span. Supported attributes: "
                            f"{supported_attributes}"
                        )
