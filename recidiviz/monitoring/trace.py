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
"""Functionality to make it easier to create and export traces"""

import time
from contextvars import ContextVar
from functools import wraps
from typing import Any, Callable, Dict, List, Tuple, TypeVar, Union

from flask import has_request_context, request
from opentelemetry.baggage import get_baggage
from opentelemetry.sdk.trace.sampling import Sampler, SamplingResult
from typing_extensions import ParamSpec

from recidiviz.monitoring.instruments import get_monitoring_instrument
from recidiviz.monitoring.keys import AttributeKey, HistogramInstrumentKey
from recidiviz.monitoring.providers import get_global_tracer_provider

TRACER_NAME = "org.recidiviz"

# Contains a list of all the addresses of all of the functions in our stack that are currently being timed. Used to
# detect recursion.
stack: ContextVar[List[int]] = ContextVar("stack", default=[])

P = ParamSpec("P")
T = TypeVar("T")


def time_and_trace(func: Callable[P, T]) -> Callable[P, Tuple[float, T]]:
    """Wraps a function in a function that will emit a span to Cloud Trace covering the
    duration of the execution and then return both the result of the function and the
    execution duration.
    """

    @wraps(func)
    def _wrapper(*args: P.args, **kwargs: P.kwargs) -> Tuple[float, T]:
        start = time.perf_counter()
        res = span(func)(*args, **kwargs)
        end = time.perf_counter()
        return (end - start), res

    return _wrapper


def span(func: Callable[P, T]) -> Callable[P, T]:
    """Creates a new span for this function in the trace.

    This allows us to visualize how much of the processing time of a given request is spent inside of this function
    without relying on log entries. Additionally, the duration of the function call is recorded as a metric.
    """

    @wraps(func)
    def run_inside_new_span(*args: P.args, **kwargs: P.kwargs) -> T:
        tracer = get_global_tracer_provider().get_tracer(TRACER_NAME)
        with tracer.start_span(name=func.__qualname__) as new_span:
            new_span.set_attribute(AttributeKey.MODULE, func.__module__)
            new_span.set_attribute(AttributeKey.ARGS, str(args))
            new_span.set_attribute(AttributeKey.KWARGS, str(kwargs))

            stack_token = stack.set(stack.get() + [id(func)])
            start = time.perf_counter()

            try:
                return func(*args, **kwargs)
            finally:
                monitoring_attributes: dict[str, Union[str, int]] = {
                    AttributeKey.MODULE: func.__module__,
                    AttributeKey.FUNCTION: func.__qualname__,
                    AttributeKey.RECURSION_DEPTH: int(stack.get().count(id(func))),
                }
                if region := get_baggage(AttributeKey.REGION):
                    monitoring_attributes[AttributeKey.REGION] = str(region)

                duration = time.perf_counter() - start
                get_monitoring_instrument(
                    HistogramInstrumentKey.FUNCTION_DURATION
                ).record(
                    amount=duration,
                    attributes=monitoring_attributes,
                )
                stack.reset(stack_token)

    return run_inside_new_span


class CompositeSampler(Sampler):
    """Dispatches to a sampler based on the path of the currently active flask request, if any."""

    def get_description(self) -> str:
        return "Dispatches to a sampler based on the path of the currently active flask request, if any."

    def __init__(
        self,
        path_prefix_to_sampler: Dict[str, Sampler],
        default_sampler: Sampler,
    ) -> None:
        self.path_prefix_to_sampler = path_prefix_to_sampler
        self.default_sampler = default_sampler

    def should_sample(self, *args: Any, **kwargs: Any) -> SamplingResult:
        sampler_to_use = self.default_sampler

        if has_request_context():
            # Just uses the last entry that matches
            for path_prefix, sampler in self.path_prefix_to_sampler.items():
                if request.path.startswith(path_prefix):
                    sampler_to_use = sampler

        return sampler_to_use.should_sample(*args, **kwargs)
