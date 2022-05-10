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
"""Helpers for determining supervision types at different points in time."""
from typing import Optional, Set

from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)

SUPERVISION_TYPE_PRECEDENCE_ORDER = [
    StateSupervisionPeriodSupervisionType.PROBATION,
    StateSupervisionPeriodSupervisionType.PAROLE,
    StateSupervisionPeriodSupervisionType.EXTERNAL_UNKNOWN,
    StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN,
]


def sentence_supervision_types_to_supervision_period_supervision_type(
    supervision_types: Set[Optional[StateSupervisionSentenceSupervisionType]],
) -> Optional[StateSupervisionPeriodSupervisionType]:

    supervision_period_supervision_types = [
        sentence_supervision_type_to_supervision_periods_supervision_type(
            supervision_type
        )
        for supervision_type in supervision_types
    ]
    if (
        StateSupervisionPeriodSupervisionType.PROBATION
        in supervision_period_supervision_types
        and StateSupervisionPeriodSupervisionType.PAROLE
        in supervision_period_supervision_types
    ):
        return StateSupervisionPeriodSupervisionType.DUAL

    # Return the supervision type that takes highest precedence
    return next(
        (
            supervision_period_supervision_type
            for supervision_period_supervision_type in SUPERVISION_TYPE_PRECEDENCE_ORDER
            if supervision_period_supervision_type
            in supervision_period_supervision_types
        ),
        None,
    )


def sentence_supervision_type_to_supervision_periods_supervision_type(
    sentence_supervision_type: Optional[StateSupervisionSentenceSupervisionType],
) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Returns the StateSupervisionPeriodSupervisionType value that most closely
    matches the provided StateSupervisionSentenceSupervisionType."""
    if not sentence_supervision_type:
        return None

    if sentence_supervision_type == StateSupervisionSentenceSupervisionType.PROBATION:
        return StateSupervisionPeriodSupervisionType.PROBATION
    if sentence_supervision_type == StateSupervisionSentenceSupervisionType.PAROLE:
        return StateSupervisionPeriodSupervisionType.PAROLE
    if (
        sentence_supervision_type
        == StateSupervisionSentenceSupervisionType.COMMUNITY_CORRECTIONS
    ):
        # TODO(#9421): update mapping, when community centers are standardized.
        return StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT
    if (
        sentence_supervision_type
        == StateSupervisionSentenceSupervisionType.EXTERNAL_UNKNOWN
    ):
        return StateSupervisionPeriodSupervisionType.EXTERNAL_UNKNOWN

    if (
        sentence_supervision_type
        == StateSupervisionSentenceSupervisionType.INTERNAL_UNKNOWN
    ):
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN

    raise ValueError(f"Unexpected supervision_type {sentence_supervision_type}.")
