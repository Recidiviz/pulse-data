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
"""Tests validity of aggregated metric configurations"""

import itertools
import unittest

from recidiviz.aggregated_metrics.aggregated_metric_view_collector import (
    METRICS_BY_POPULATION_TYPE,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    EventMetricConditionsMixin,
    SpanMetricConditionsMixin,
)
from recidiviz.calculator.query.state.views.analyst_data.models.person_events import (
    PERSON_EVENTS_BY_TYPE,
)
from recidiviz.calculator.query.state.views.analyst_data.models.person_spans import (
    PERSON_SPANS_BY_TYPE,
)


class MetricsByPopulationTypeTest(unittest.TestCase):
    # check that span attribute filters are compatible with source spans for all configured span metrics
    def test_compatible_span_attribute_filters(self) -> None:
        for metric in itertools.chain.from_iterable(
            METRICS_BY_POPULATION_TYPE.values()
        ):
            if isinstance(metric, SpanMetricConditionsMixin):
                for span, attribute in itertools.product(
                    metric.span_types, metric.span_attribute_filters
                ):
                    if attribute not in PERSON_SPANS_BY_TYPE[span].attribute_cols:
                        raise ValueError(
                            f"Span attribute `{attribute}` is not supported by {span.value} span. "
                            f"Supported attributes: {PERSON_SPANS_BY_TYPE[span].attribute_cols}"
                        )

    # check that event attribute filters are compatible with source events for all configured event metrics
    def test_compatible_event_attribute_filters(self) -> None:
        for metric in itertools.chain.from_iterable(
            METRICS_BY_POPULATION_TYPE.values()
        ):
            if isinstance(metric, EventMetricConditionsMixin):
                for event, attribute in itertools.product(
                    metric.event_types, metric.event_attribute_filters
                ):
                    if attribute not in PERSON_EVENTS_BY_TYPE[event].attribute_cols:
                        raise ValueError(
                            f"Event attribute `{attribute}` is not supported by {event.value} event. "
                            f"Supported attributes: {PERSON_EVENTS_BY_TYPE[event].attribute_cols}"
                        )
