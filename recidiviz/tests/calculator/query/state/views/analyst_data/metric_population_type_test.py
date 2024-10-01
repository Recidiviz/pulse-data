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
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.spans import (
    SPANS_BY_TYPE,
)
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)


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

                for attribute in span_selector.span_conditions_dict:
                    if (
                        attribute
                        not in SPANS_BY_TYPE[span_selector.span_type].attribute_cols
                    ):
                        raise ValueError(
                            f"Span attribute `{attribute}` is not supported by {span_selector.span_type.value} span. "
                            f"Supported attributes: {SPANS_BY_TYPE[span_selector.span_type].attribute_cols}"
                        )
