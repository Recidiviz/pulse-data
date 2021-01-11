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

"""Converts an ingest_info proto StatePersonExternalId to a persistence
entity."""
from recidiviz.common.common_utils import get_external_id
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StatePersonExternalId
from recidiviz.persistence.entity.state import entities


def convert(proto: StatePersonExternalId,
            metadata: IngestMetadata) -> entities.StatePersonExternalId:
    """Converts an ingest_info proto Hold to a persistence entity."""
    new = entities.StatePersonExternalId.builder()

    # The ingest info -> proto converter appends the id_type to the external id to keep external ids globally unique.
    new.external_id = get_external_id(synthetic_id=getattr(proto, 'state_person_external_id_id'))
    new.id_type = getattr(proto, 'id_type')
    new.state_code = metadata.region

    return new.build(entities.StatePersonExternalId.deserialize)  # type: ignore[attr-defined]
