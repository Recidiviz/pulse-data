# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Matchers for convenient unit testing."""
import collections
from typing import Any

import callee


class UnorderedCollection(callee.Matcher):
    """An argument Matcher which can match against unordered collections.

    That is, if you want to assert that a mock
    was called with a collection and you don't care about, or can't guarantee, the order of elements, this will
    assert that the collection consists of the correct elements.
    """

    def __init__(self, comparison_collection: Any):
        self.comparison = comparison_collection

    def match(self, value: Any) -> bool:
        return collections.Counter(value) == collections.Counter(self.comparison)
