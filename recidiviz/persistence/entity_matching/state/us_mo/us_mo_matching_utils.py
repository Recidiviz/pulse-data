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
"""Contains util methods for UsMoMatchingDelegate."""
import datetime
from typing import List

from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    is_placeholder,
)
from recidiviz.persistence.entity_matching.state.state_matching_utils import (
    get_all_entities_of_cls,
)


def set_current_supervising_officer_from_supervision_periods(
    matched_persons: List[schema.StatePerson], field_index: CoreEntityFieldIndex
):
    """For every matched person, update the supervising_officer field to pull in the supervising_officer from the latest
    supervision period (sorted by termination date).
    """
    for person in matched_persons:

        sps = get_all_entities_of_cls(
            person.sentence_groups,
            schema.StateSupervisionPeriod,
            field_index=field_index,
        )

        non_placeholder_sps = [sp for sp in sps if not is_placeholder(sp, field_index)]

        if not non_placeholder_sps:
            continue

        non_placeholder_sps.sort(
            key=lambda sp: sp.termination_date
            if sp.termination_date
            else datetime.date.max
        )

        latest_supervision_period = non_placeholder_sps[-1]
        person.supervising_officer = latest_supervision_period.supervising_officer
