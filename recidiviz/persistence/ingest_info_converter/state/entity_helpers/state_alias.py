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
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateAlias
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import \
    parse_region_code_with_override
from recidiviz.persistence.ingest_info_converter.utils.names import parse_name


def convert(proto: StateAlias, metadata: IngestMetadata) \
        -> entities.StatePersonAlias:
    """Converts an ingest_info proto StateAlias to a persistence entity."""
    new = entities.StatePersonAlias.builder()

    new.state_code = parse_region_code_with_override(
        proto, 'state_code', metadata)
    new.full_name = parse_name(proto)

    return new.build()
