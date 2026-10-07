# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Fake Sentence subclass and offense enums for state-agnostic calculator tests.

Sentence, OffenseCategory and OffenseGroup are all abstract: each state supplies
its own concrete subclass. Tests that exercise generic engine or graph behavior
rather than one state's rules use the fakes here, so they do not have to borrow
a real state's types.
"""

import attr

from recidiviz.sentence_calculator.models.inputs import (
    OffenseCategory,
    OffenseGroup,
    Sentence,
)


class FakeOffenseCategory(OffenseCategory):
    """Offense severity categories for a fake state."""

    CATEGORY_A = "A"
    CATEGORY_B = "B"
    CATEGORY_C = "C"


class FakeOffenseGroup(OffenseGroup):
    """Offense type groupings for a fake state."""

    VIOLENCE = "VIOLENCE"
    NONE = "NONE"


@attr.define(frozen=True, kw_only=True)
class FakeSentence(Sentence):
    """Sentence implementation for a fake state, using the fake offense enums."""

    offense_category: FakeOffenseCategory
    offense_group: FakeOffenseGroup
