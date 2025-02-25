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
"""Converts an ingest_info proto StateCharge to a persistence entity."""

from recidiviz.common.constants.charge import (ChargeStatus)
from recidiviz.common.constants.state.state_charge import \
    StateChargeClassificationType
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateCharge
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    fn, parse_external_id, parse_region_code_with_override)
from recidiviz.common.str_field_utils import parse_bool, normalize, \
    parse_date, parse_int
from recidiviz.persistence.ingest_info_converter.utils.enum_mappings \
    import EnumMappings


def copy_fields_to_builder(
        new: entities.StateCharge.Builder,
        proto: StateCharge,
        metadata: IngestMetadata) -> None:
    """Mutates the provided |charge_builder| by converting an ingest_info proto
     StateCharge.

     Note: This will not copy children into the Builder!
     """

    enum_fields = {
        'status': ChargeStatus,
        'classification_type': StateChargeClassificationType,
    }
    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    # Enum values
    new.status = enum_mappings.get(ChargeStatus,
                                   default=ChargeStatus.PRESENT_WITHOUT_INFO)
    new.status_raw_text = fn(normalize, 'status', proto)
    new.classification_type = \
        enum_mappings.get(StateChargeClassificationType)
    new.classification_type_raw_text = \
        fn(normalize, 'classification_type', proto)

    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, 'state_charge_id', proto)
    new.offense_date = fn(parse_date, 'offense_date', proto)
    new.date_charged = fn(parse_date, 'date_charged', proto)
    new.state_code = parse_region_code_with_override(
        proto, 'state_code', metadata)
    new.county_code = fn(normalize, 'county_code', proto)
    new.statute = fn(normalize, 'statute', proto)
    new.description = fn(normalize, 'description', proto)
    new.attempted = fn(parse_bool, 'attempted', proto)
    if new.classification_type is None:
        new.classification_type = \
            StateChargeClassificationType.find_in_string(new.description)
    new.classification_subtype = \
        fn(normalize, 'classification_subtype', proto)
    new.counts = fn(parse_int, 'counts', proto)
    new.charge_notes = fn(normalize, 'charge_notes', proto)
    new.is_controlling = fn(parse_bool, 'is_controlling', proto)
    new.charging_entity = fn(normalize, 'charging_entity', proto)
