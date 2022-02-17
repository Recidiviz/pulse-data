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
"""Base class for identifying various instances of events for a metric calculation pipeline."""
import abc
from typing import Any, Dict, Generic, Type, TypeVar

import attr

from recidiviz.calculator.pipeline.utils.event_utils import IdentifierEvent
from recidiviz.persistence.entity.state.entities import StatePerson

IdentifierEventResultT = TypeVar("IdentifierEventResultT")
IdentifierContextT = TypeVar("IdentifierContextT", bound=Dict[str, Any])


@attr.s
class BaseIdentifier(abc.ABC, Generic[IdentifierEventResultT]):
    """A base class for identifying various instances of events for a metric calculation pipeline."""

    identifier_event_class: Type[IdentifierEvent] = attr.ib()

    @abc.abstractmethod
    def find_events(
        self, person: StatePerson, identifier_context: IdentifierContextT
    ) -> IdentifierEventResultT:
        """Define the function to identify the events."""
