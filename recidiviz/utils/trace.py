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
"""Functionality to make it easier to create and export traces"""

from contextvars import ContextVar
from functools import wraps
import time
from typing import Callable, Dict, List

from flask import request
from opencensus.stats import measure, view, aggregation
from opencensus.trace import execution_context, samplers, span_context as span_ctx, tracer as tracer_module

from recidiviz.utils import monitoring

m_duration_s = measure.MeasureFloat("function_duration", "The time it took for this function to run", "s")

duration_distribution_view = view.View("recidiviz/function_durations",
                                       "The distribution of the function durations",
                                       [monitoring.TagKey.REGION, monitoring.TagKey.FUNCTION],
                                       m_duration_s,
                                       aggregation.DistributionAggregation(monitoring.exponential_buckets(0.1, 5, 10)))
monitoring.register_views([duration_distribution_view])

# Contains a list of all the addresses of all of the functions in our stack that are currently being timed. Used to
# detect recursion.
stack: ContextVar[List[int]] = ContextVar("stack", default=[])

def span(func: Callable) -> Callable:
    """Creates a new span for this function in the trace.

    This allows us to visualize how much of the processing time of a given request is spent inside of this function
    without relying on log entries. Additionally the duration of the function call is recorded as a metric.
    """

    @wraps(func)
    def run_inside_new_span(*args, **kwargs):
        tracer: tracer_module.Tracer = execution_context.get_opencensus_tracer()
        with tracer.span(name=func.__qualname__) as new_span:
            new_span.add_attribute('recidiviz.function.module', func.__module__)
            new_span.add_attribute('recidiviz.function.args', str(args))
            new_span.add_attribute('recidiviz.function.kwargs', str(kwargs))

            with monitoring.measurements({
                monitoring.TagKey.MODULE: func.__module__,
                monitoring.TagKey.FUNCTION: func.__qualname__,
                monitoring.TagKey.RECURSION_DEPTH: stack.get().count(id(func))
            }) as measurements:
                stack_token = stack.set(stack.get() + [id(func)])
                start = time.perf_counter()

                try:
                    return func(*args, **kwargs)
                finally:
                    measurements.measure_float_put(m_duration_s, time.perf_counter() - start)
                    stack.reset(stack_token)

    return run_inside_new_span

class CompositeSampler(samplers.Sampler):
    """Dispatches to a sampler based on the path of the currently active flask request, if any."""
    def __init__(
        self, path_prefix_to_sampler: Dict[str, samplers.Sampler], default_sampler: samplers.Sampler
    ) -> None:
        self.path_prefix_to_sampler = path_prefix_to_sampler
        self.default_sampler = default_sampler

    def should_sample(self, span_context: span_ctx.SpanContext) -> bool:
        sampler_to_use = self.default_sampler

        if request is not None:
            # Just uses the last entry that matches
            for path_prefix, sampler in self.path_prefix_to_sampler.items():
                if request.path.startswith(path_prefix):
                    sampler_to_use = sampler

        return sampler_to_use.should_sample(span_context)
