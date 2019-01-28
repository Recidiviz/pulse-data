# Recidiviz - a platform for tracking granular recidivism metrics in real time
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
from recidiviz.common.constants.hold import HoldStatus
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence import entities
from recidiviz.persistence.converter.converter_utils import normalize, fn, \
    parse_external_id


def convert(proto, metadata: IngestMetadata) -> entities.Hold:
    """Converts an ingest_info proto Hold to a persistence entity."""
    new = entities.Hold.builder()

    new.external_id = fn(parse_external_id, 'hold_id', proto)
    new.jurisdiction_name = fn(normalize, 'jurisdiction_name', proto)
    new.status = fn(HoldStatus.parse, 'status', proto,
                    metadata.enum_overrides,
                    default=HoldStatus.UNKNOWN_FOUND_IN_SOURCE)
    new.status_raw_text = fn(normalize, 'status', proto)

    return new.build()
