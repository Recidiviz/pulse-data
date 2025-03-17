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

"""Creates monitoring client for measuring and recording stats."""
from collections import deque
from contextlib import contextmanager
from typing import Generator, Optional, Union

from opentelemetry import context, trace
from opentelemetry.baggage import set_baggage
from opentelemetry.context import Context
from opentelemetry.trace import INVALID_SPAN, format_trace_id

from recidiviz.monitoring.keys import AttributeKey
from recidiviz.utils import environment


@contextmanager
def push_monitoring_context(
    *args: Union[str, object, dict[str, object]],
    current_context: Optional[Context] = None
) -> Generator[None, None, None]:
    """
    Adds an attribute to the current Open Telemetry context that can be accessed elsewhere with a call to
    Open Telemetry's get_baggage function for the duration of this code block wrapped by this context manager.
    """

    name_value_pairs: list[tuple[str, object]] = []
    args_queue = deque(args)
    while args_queue:
        arg = args_queue.popleft()

        if isinstance(arg, str):
            name_value_pairs.append((arg, args_queue.popleft()))
        elif isinstance(arg, dict):
            name_value_pairs.extend(list(arg.items()))
        else:
            raise ValueError(
                "Incorrect call signature; pass evenly matched name/value pairs, or dicts"
            )

    tokens = [
        context.attach(set_baggage(name, value, context=current_context))
        for name, value in name_value_pairs
    ]

    try:
        yield
    finally:
        for token in reversed(tokens):
            context.detach(token)


@contextmanager
def push_region_context(
    region_code: str, ingest_instance: Optional[str]
) -> Generator[None, None, None]:
    with push_monitoring_context(
        {
            AttributeKey.REGION: region_code,
            AttributeKey.INGEST_INSTANCE: ingest_instance,
        }
    ):
        yield


def get_current_trace_id() -> str:
    """Returns the Trace ID representing the trace that the current span is a part of.
    Raises a KeyError if we are not currently in a span being traced by OpenTelemetry"""
    if not environment.in_gcp():
        return "local"
    current_span = trace.get_current_span()

    if current_span == INVALID_SPAN:
        raise KeyError("Cannot get the current trace id while not inside a trace")

    return format_trace_id(current_span.get_span_context().trace_id)
