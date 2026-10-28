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
"""State-specific Sentence input models, and the factory that dispatches
sentence construction to the right one by state code."""

from datetime import date

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.sentence_calculator.models.inputs import (
    OffenseCategory,
    OffenseGroup,
    Sentence,
    SentenceType,
    Term,
)
from recidiviz.sentence_calculator.models.state_inputs.inputs_nv import (
    NevadaOffenseCategory,
    NevadaOffenseGroup,
    NevadaSentence,
)


@attr.define(frozen=True, kw_only=True)
class _StateSentenceTypes:
    """The Sentence subclass and offense enums one state's sentences use."""

    sentence_class: type[Sentence]
    """The Sentence subclass, which declares offense_category and offense_group
    with the enums below."""

    offense_category_type: type[OffenseCategory]
    """The enum for the sentence's offense_category."""

    offense_group_type: type[OffenseGroup]
    """The enum for the sentence's offense_group."""


# Supporting a new state means adding its types here.
_STATE_SENTENCE_TYPES: dict[StateCode, _StateSentenceTypes] = {
    StateCode.US_NV: _StateSentenceTypes(
        sentence_class=NevadaSentence,
        offense_category_type=NevadaOffenseCategory,
        offense_group_type=NevadaOffenseGroup,
    ),
}


def _state_sentence_types(state_code: StateCode) -> _StateSentenceTypes:
    if state_code not in _STATE_SENTENCE_TYPES:
        raise ValueError(
            f"Unsupported state code for sentences: [{state_code.value}]. Add its "
            f"sentence types to _STATE_SENTENCE_TYPES."
        )
    return _STATE_SENTENCE_TYPES[state_code]


def create_sentence(
    *,
    state_code: StateCode,
    sentence_id: str,
    min_term: Term,
    max_term: Term,
    raw_offense_category: str,
    raw_offense_group: str,
    sentence_type: SentenceType | None = None,
    start_date: date | None = None,
    parent_sentence_id: str | None = None,
    initial_time_served: Term | None = None,
) -> Sentence:
    """Builds the state-specific Sentence subclass for state_code.

    Fields common to every state are filled in directly here. Raw values for
    fields whose concrete type varies by state (offense_category,
    offense_group) are parsed into that state's enums.
    """
    sentence_types = _state_sentence_types(state_code)
    # Sentence declares offense_category and offense_group as abstract
    # properties, so mypy can't see that every subclass takes them as fields.
    return sentence_types.sentence_class(  # type: ignore[call-arg]
        sentence_id=sentence_id,
        min_term=min_term,
        max_term=max_term,
        sentence_type=sentence_type,
        start_date=start_date,
        parent_sentence_id=parent_sentence_id,
        initial_time_served=initial_time_served or Term(),
        offense_category=sentence_types.offense_category_type(raw_offense_category),
        offense_group=sentence_types.offense_group_type(raw_offense_group),
    )


def sentence_class(state_code: StateCode) -> type[Sentence]:
    """Returns the Sentence subclass state_code's sentences use."""
    return _state_sentence_types(state_code).sentence_class
