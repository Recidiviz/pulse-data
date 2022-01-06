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
# ============================================================================
"""Specific entity matching utils for StateIncarcerationPeriod/StateSupervisionPeriod entities."""
from typing import List

from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    is_placeholder,
)
from recidiviz.persistence.entity_matching.state.state_matching_utils import (
    get_all_entities_of_cls,
)


def add_supervising_officer_to_open_supervision_periods(
    persons: List[schema.StatePerson], field_index: CoreEntityFieldIndex
) -> None:
    """For each person in the provided |persons|, adds the supervising_officer from the person entity onto all open
    StateSupervisionPeriods.
    """
    for person in persons:
        if not person.supervising_officer:
            continue

        supervision_periods: List[
            schema.StateSupervisionPeriod
        ] = get_all_entities_of_cls(
            [person], schema.StateSupervisionPeriod, field_index=field_index
        )  # type: ignore
        for supervision_period in supervision_periods:
            # Skip placeholders
            if is_placeholder(supervision_period, field_index):
                continue

            if not supervision_period.termination_date:
                supervision_period.supervising_officer = person.supervising_officer
