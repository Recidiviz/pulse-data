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
"""Converts an ingest_info proto Hold to a persistence entity."""
from recidiviz.common.constants.county.hold import HoldStatus
from recidiviz.common.ingest_metadata import LegacyStateAndJailsIngestMetadata
from recidiviz.common.str_field_utils import normalize
from recidiviz.persistence.entity.county import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    fn,
    parse_external_id,
)
from recidiviz.persistence.ingest_info_converter.utils.ingest_info_proto_enum_mapper import (
    IngestInfoProtoEnumMapper,
)


def convert(proto, metadata: LegacyStateAndJailsIngestMetadata) -> entities.Hold:
    """Converts an ingest_info proto Hold to a persistence entity."""
    new = entities.Hold.builder()

    enum_fields = {
        "status": HoldStatus,
    }
    proto_enum_mapper = IngestInfoProtoEnumMapper(
        proto, enum_fields, metadata.enum_overrides
    )

    new.status = proto_enum_mapper.get(
        HoldStatus, default=HoldStatus.PRESENT_WITHOUT_INFO
    )
    new.status_raw_text = fn(normalize, "status", proto)

    new.external_id = fn(parse_external_id, "hold_id", proto)
    new.jurisdiction_name = fn(normalize, "jurisdiction_name", proto)

    return new.build()
