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
"""Method to build OTL views from our monitoring config"""
from typing import Optional, Sequence

import opentelemetry.sdk.metrics.view as opentelemetry_view
from opentelemetry.sdk.metrics.view import View

from recidiviz.monitoring.configs import AggregationConfig, MonitoringConfig


def build_monitoring_views() -> Sequence[View]:
    views = []

    monitoring_config = MonitoringConfig.build()

    for instrument in monitoring_config.instruments:
        for view_config in instrument.views:
            aggregation = None
            if view_config.aggregation:
                aggregation = _build_aggregation(
                    aggregation_config=view_config.aggregation
                )
            views.append(
                View(
                    instrument_name=instrument.instrument_key.value,
                    name=view_config.name,
                    attribute_keys=view_config.attribute_keys,
                    aggregation=aggregation,
                )
            )
    return views


def _build_aggregation(
    aggregation_config: AggregationConfig,
) -> Optional[opentelemetry_view.Aggregation]:
    if aggregation_kind := aggregation_config.kind:
        aggregation = getattr(opentelemetry_view, aggregation_kind)
        if aggregation_kind == "ExplicitBucketHistogramAggregation":
            if not aggregation_config.options:
                raise ValueError(
                    "Must specify options when using ExplicitBucketHistogramAggregation"
                )

            return opentelemetry_view.ExplicitBucketHistogramAggregation(
                boundaries=exponential_buckets(**aggregation_config.options)
            )

        return aggregation()
    return None


def exponential_buckets(start: float, factor: float, count: int) -> list[float]:
    """Creates `count` buckets where `start` is the first bucket's upper boundary, and each subsequent bucket has an
    upper boundary that is `factor` larger.

    E.g. start=10, factor=2, count=5 would create [0, 10, 20, 40, 80, 160]
    """
    buckets = []
    next_boundary = start
    for _ in range(count):
        buckets.append(next_boundary)
        next_boundary *= factor
    return buckets
