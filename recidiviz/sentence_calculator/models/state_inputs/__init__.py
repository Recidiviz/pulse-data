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

from recidiviz.common.constants.states import StateCode
from recidiviz.sentence_calculator.models.inputs import Sentence, SentenceType, Term
from recidiviz.sentence_calculator.models.state_inputs.inputs_nv import (
    NevadaOffenseCategory,
    NevadaOffenseGroup,
    NevadaSentence,
)


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
    offense_group) are parsed by the state-specific subclass, since only that
    subclass knows its own enum types.
    """
    if state_code is StateCode.US_NV:
        # Pylint known false positive
        return NevadaSentence(  # pylint: disable=abstract-class-instantiated
            sentence_id=sentence_id,
            min_term=min_term,
            max_term=max_term,
            sentence_type=sentence_type,
            start_date=start_date,
            parent_sentence_id=parent_sentence_id,
            initial_time_served=initial_time_served or Term(),
            offense_category=NevadaOffenseCategory(raw_offense_category),
            offense_group=NevadaOffenseGroup(raw_offense_group),
        )
    raise ValueError(
        f"Unsupported state code for Sentence construction: [{state_code}]"
    )
