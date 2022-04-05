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
# =============================================================================

"""Converts an ingest_info proto StateFine to a persistence entity."""
from recidiviz.common.constants.state.state_fine import StateFineStatus
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.common.str_field_utils import parse_date, normalize, \
    parse_dollars

from recidiviz.ingest.models.ingest_info_pb2 import StateFine
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import \
    parse_region_code_with_override, parse_external_id, fn
from recidiviz.persistence.ingest_info_converter.utils.enum_mappings import \
    EnumMappings


def copy_fields_to_builder(
        fine_builder: entities.StateFine.Builder,
        proto: StateFine,
        metadata: IngestMetadata) -> None:
    """Mutates the provided |fine_builder| by converting an ingest_info proto
    StateFine.

    Note: This will not copy children into the Builder!
    """
    new = fine_builder

    enum_fields = {
        'status': StateFineStatus,
    }
    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    # enum values
    new.status = enum_mappings.get(StateFineStatus)
    new.status_raw_text = fn(normalize, 'status', proto)

    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, 'state_fine_id', proto)
    new.date_paid = fn(parse_date, 'date_paid', proto)
    new.state_code = parse_region_code_with_override(
        proto, 'state_code', metadata)
    new.county_code = fn(normalize, 'county_code', proto)
    new.fine_dollars = fn(parse_dollars, 'fine_dollars', proto)
