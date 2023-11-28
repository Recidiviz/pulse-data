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

from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    POPULATION_TYPE_TO_SPAN_SELECTOR_LIST,
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.spans import (
    SPANS_BY_TYPE,
)


class MetricPopulationsByTypeTest(unittest.TestCase):
    # check that population_name_short only has valid character types
    def test_population_name_short_char_types(self) -> None:
        for e in MetricPopulationType:
            if not re.match(r"^\w+$", e.population_name_short):
                raise ValueError(
                    "All characters in MetricPopulationType value must be alphanumeric or underscores."
                )

    # check that all attribute filters are compatible with person spans in configured MetricPopulation
    def test_compatible_person_span_filters(self) -> None:
        for _, span_selectors in POPULATION_TYPE_TO_SPAN_SELECTOR_LIST.items():
            for span_selector in span_selectors:
                for attribute in span_selector.span_conditions_dict:
                    if (
                        attribute
                        not in SPANS_BY_TYPE[span_selector.span_type].attribute_cols
                    ):
                        raise ValueError(
                            f"Span attribute `{attribute}` is not supported by {span_selector.span_type.value} span. "
                            f"Supported attributes: {SPANS_BY_TYPE[span_selector.span_type].attribute_cols}"
                        )
