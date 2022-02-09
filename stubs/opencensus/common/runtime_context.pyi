# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
from contextvars import ContextVar
from typing import Any, Callable

from opencensus.trace.tracers import ContextTracer

class _AsyncRuntimeContext:
    tracer: ContextTracer

    class Slot:
        contextvar: ContextVar
        def get(self) -> Any: ...
        def clear(self) -> None: ...
        def set(self, _value: Any) -> None: ...

    def with_current_context(self, func: Callable) -> Callable: ...

RuntimeContext: _AsyncRuntimeContext
