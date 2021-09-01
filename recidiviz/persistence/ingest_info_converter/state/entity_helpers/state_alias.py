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

"""Converts an ingest_info proto StateAlias to a persistence entity."""
from recidiviz.common.constants.defaulting_and_normalizing_enum_parser import (
    DefaultingAndNormalizingEnumParser,
)
from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateAlias
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.deserialize_entity_factories import (
    StatePersonAliasFactory,
)
from recidiviz.persistence.ingest_info_converter.utils.names import parse_name


# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
def convert(proto: StateAlias, metadata: IngestMetadata) -> entities.StatePersonAlias:
    """Converts an ingest_info proto StateAlias to a persistence entity."""
    new = entities.StatePersonAlias.builder()

    # enum values
    new.alias_type = DefaultingAndNormalizingEnumParser(
        getattr(proto, "alias_type"), StatePersonAliasType, metadata.enum_overrides
    )
    new.alias_type_raw_text = getattr(proto, "alias_type")

    # 1-to-1 mappings
    new.state_code = metadata.region
    new.full_name = parse_name(proto)

    return new.build(StatePersonAliasFactory.deserialize)
