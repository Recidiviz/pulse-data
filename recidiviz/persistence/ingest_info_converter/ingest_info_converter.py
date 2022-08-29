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
"""Converts scraped IngestInfo data to the persistence layer entity.

TODO(#8905): Delete this file once all states have been migrated to v2 ingest
 mappings.
"""

from recidiviz.common.ingest_metadata import LegacyStateAndJailsIngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import IngestInfo
from recidiviz.persistence.ingest_info_converter.base_converter import (
    EntityDeserializationResult,
)
from recidiviz.persistence.ingest_info_converter.state.state_converter import (
    StateConverter,
)
from recidiviz.utils import trace


@trace.span
def convert_to_persistence_entities(
    ingest_info: IngestInfo, metadata: LegacyStateAndJailsIngestMetadata
) -> EntityDeserializationResult:
    converter = StateConverter(ingest_info, metadata)
    return converter.run_convert()
