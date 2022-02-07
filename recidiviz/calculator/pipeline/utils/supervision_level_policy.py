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
"""Defines a date-bounded version of a supervision level policy."""
from datetime import date
from typing import Dict, Optional, Tuple

import attr

from recidiviz.common.constants.shared_enums.person_characteristics import Gender
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)
from recidiviz.persistence.entity.state.entities import StatePerson


@attr.s
class SupervisionLevelPolicy:
    """Defines the supervision level policies for a given state over a given time
    period. The main method is `recommended_supervision_level_from_score` which takes
    in the person and their last assessment score and outputs what the policy would
    recommend."""

    # Mapping from gender -> supervision level -> risk assessment score range bounds
    # A value of None in the second field of the tuple implies there is no upper bound.
    level_mapping: Dict[
        Gender, Dict[StateSupervisionLevel, Tuple[int, Optional[int]]]
    ] = attr.ib()

    # start_date is inclusive
    start_date: Optional[date] = attr.ib(default=None)
    end_date_exclusive: Optional[date] = attr.ib(default=None)
    # states may or may not have a policy about this
    pre_assessment_level: Optional[StateSupervisionLevel] = attr.ib(default=None)

    def recommended_supervision_level_from_score(
        self, person: StatePerson, last_assessment_score: int
    ) -> Optional[StateSupervisionLevel]:
        if not (gender := person.gender) or not (
            policy := self.level_mapping.get(gender)
        ):
            return None

        for supervision_level, assessment_score_bounds in policy.items():
            if assessment_score_bounds[0] <= last_assessment_score and (
                assessment_score_bounds[1] is None
                or assessment_score_bounds[1] >= last_assessment_score
            ):
                # The person's score is within the specified bounds.
                return supervision_level

        raise ValueError(
            "Could not find supervision level for inputted assessment score"
        )
