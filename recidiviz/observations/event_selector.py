# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Defines EventSelector object used to filter rows from an events table"""

from typing import Dict, List, Union

import attr

from recidiviz.observations.event_type import EventType
from recidiviz.observations.observation_selector import ObservationSelector


@attr.define(frozen=True, kw_only=True)
class EventSelector(ObservationSelector[EventType]):
    """
    Class that stores information that can be used to generate query fragments that
    allow us to select a targeted set of events.
    """

    # The EventTypes to select
    event_type: EventType

    # Dictionary mapping event attributes to their associated conditions. Only events
    # with these attribute values will be selected.
    event_conditions_dict: Dict[str, Union[List[str], str]] = attr.ib()

    @property
    def observation_type(self) -> EventType:
        return self.event_type

    @property
    def observation_conditions_dict(self) -> dict[str, list[str] | str]:
        return self.event_conditions_dict
