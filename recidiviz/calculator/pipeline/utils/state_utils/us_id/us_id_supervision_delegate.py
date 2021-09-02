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
"""US_ID implementation of the supervision delegate"""
from typing import List, Optional, Tuple

from recidiviz.calculator.pipeline.supervision.events import SupervisionPopulationEvent
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)

_OUT_OF_STATE_EXTERNAL_ID_IDENTIFIERS: List[str] = [
    "INTERSTATE PROBATION",
    "PAROLE COMMISSION OFFICE",
]


class UsIdSupervisionDelegate(StateSpecificSupervisionDelegate):
    """US_ID implementation of the supervision delegate"""

    def supervision_types_mutually_exclusive(self) -> bool:
        """In US_ID, people on DUAL supervision are tracked as mutually exclusive from groups of people
        on PAROLE or PROBATION."""
        return True

    def supervision_location_from_supervision_site(
        self, supervision_site: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        """In US_ID, supervision_site follows format {supervision district}|{location/office within district}"""
        # TODO(#3829): Remove this helper once we've once we've built level 1/level 2 supervision
        #  location distinction directly into our schema.
        level_1_supervision_location = None
        level_2_supervision_location = None
        if supervision_site:
            (
                level_2_supervision_location,
                level_1_supervision_location,
            ) = supervision_site.split("|")
        return (
            level_1_supervision_location or None,
            level_2_supervision_location or None,
        )

    def is_supervision_location_out_of_state(
        self, supervision_population_event: SupervisionPopulationEvent
    ) -> bool:
        """For Idaho, we look at the supervision district identifier to see if it's a non-Idaho
        entity/jurisdiction."""
        # TODO(#4713): Rely on level_2_supervising_district_external_id, once it is populated.
        external_id = supervision_population_event.supervising_district_external_id
        return external_id is not None and external_id.startswith(
            tuple(_OUT_OF_STATE_EXTERNAL_ID_IDENTIFIERS)
        )
