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
from abc import ABC

class AbstractBackoff(ABC):
    def reset(self) -> None: ...
    def compute(self, failures: float) -> float: ...

class ConstantBackoff(AbstractBackoff):
    def __init__(self, backoff: float) -> None: ...

class NoBackoff(ConstantBackoff): ...

class ExponentialBackoff(AbstractBackoff):
    def __init__(self, cap: float, base: float) -> None: ...

class FullJitterBackoff(AbstractBackoff):
    def __init__(self, cap: float, base: float) -> None: ...

class EqualJitterBackoff(AbstractBackoff):
    def __init__(self, cap: float, base: float) -> None: ...

class DecorrelatedJitterBackoff(AbstractBackoff):
    def __init__(self, cap: float, base: float) -> None: ...
