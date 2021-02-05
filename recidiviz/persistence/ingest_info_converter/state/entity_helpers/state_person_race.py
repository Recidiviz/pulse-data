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

"""Converts an ingest_info proto StatePersonRace to a persistence entity."""
from recidiviz.common.constants.enum_parser import EnumParser
from recidiviz.common.constants.person_characteristics import Race
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StatePersonRace
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.deserialize_entity_factories import StatePersonRaceFactory


def convert(proto: StatePersonRace,
            metadata: IngestMetadata) -> entities.StatePersonRace:
    """Converts an ingest_info proto Hold to a persistence entity."""
    new = entities.StatePersonRace.builder()

    # Enum mappings
    new.race = EnumParser(getattr(proto, 'race'), Race, metadata.enum_overrides)
    new.race_raw_text = getattr(proto, 'race')

    # 1-to-1 mappings
    new.state_code = metadata.region

    return new.build(StatePersonRaceFactory.deserialize)
