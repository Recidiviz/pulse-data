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
"""A series of classes to better encapsulate request results."""
from typing import Generic, List, TypeVar

import attr

Success = TypeVar("Success")
Failure = TypeVar("Failure")
Skipped = TypeVar("Skipped")


@attr.s
class MultiRequestResult(Generic[Success, Failure]):
    """For some endpoints, there are multiple operations that are performed based on a request that
    contains a list of some sort, so this class helps encapsulate partial results."""

    successes: List[Success] = attr.ib()
    failures: List[Failure] = attr.ib()


@attr.s
class MultiRequestResultWithSkipped(Generic[Success, Failure, Skipped]):
    """An augmented version of MultiRequestResult, but with the option of having some of the results
    be skipped."""

    successes: List[Success] = attr.ib()
    failures: List[Failure] = attr.ib()
    skipped: List[Skipped] = attr.ib(default=list)
