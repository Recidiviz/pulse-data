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
# ============================================================================

"""Converts ingested IngestInfo data to the persistence layer entities."""

from recidiviz.persistence import entities
from recidiviz.persistence.ingest_info_converter.base_converter import \
    BaseConverter
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import \
    state_person


class StateConverter(BaseConverter[entities.StatePerson]):
    """Converts between ingest_info objects and persistence layer entities
    for state-level entities."""

    def __init__(self, ingest_info, metadata):
        super().__init__(ingest_info, metadata)

    def _is_complete(self) -> bool:
        if self.ingest_info.state_people:
            return False
        return True

    def _convert_and_pop(self) -> entities.StatePerson:
        return self._convert_state_person(self.ingest_info.state_people.pop())

    def _convert_state_person(self, ingest_state_person) \
            -> entities.StatePerson:
        """Converts an ingest_info proto StatePerson to a persistence entity."""
        state_person_builder = entities.StatePerson.builder()

        state_person.copy_fields_to_builder(
            state_person_builder, ingest_state_person, self.metadata)

        # TODO(1625): Add conversion of child entities, see county_converter

        return state_person_builder.build()
