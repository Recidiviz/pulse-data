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
from recidiviz.common import ncic
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.state.state_charge import StateChargeClassificationType
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.common.str_field_utils import (
    normalize,
    parse_bool,
    parse_date,
    parse_int,
)
from recidiviz.ingest.models.ingest_info_pb2 import StateCharge
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    fn,
    parse_external_id,
    parse_region_code_with_override,
)
from recidiviz.persistence.ingest_info_converter.utils.enum_mappings import EnumMappings


# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
def copy_fields_to_builder(
    new: entities.StateCharge.Builder, proto: StateCharge, metadata: IngestMetadata
) -> None:
    """Mutates the provided |charge_builder| by converting an ingest_info proto
    StateCharge.

    Note: This will not copy children into the Builder!
    """

    enum_fields = {
        "status": ChargeStatus,
        "classification_type": StateChargeClassificationType,
    }
    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    # Enum values
    new.status = enum_mappings.get(
        ChargeStatus, default=ChargeStatus.PRESENT_WITHOUT_INFO
    )
    new.status_raw_text = fn(normalize, "status", proto)
    new.classification_type = enum_mappings.get(StateChargeClassificationType)
    new.classification_type_raw_text = fn(normalize, "classification_type", proto)

    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, "state_charge_id", proto)
    new.offense_date = fn(parse_date, "offense_date", proto)
    new.date_charged = fn(parse_date, "date_charged", proto)
    new.state_code = parse_region_code_with_override(proto, "state_code", metadata)
    new.county_code = fn(normalize, "county_code", proto)
    new.statute = fn(normalize, "statute", proto)

    new.ncic_code = fn(normalize, "ncic_code", proto)
    new.description = fn(normalize, "description", proto)
    if new.description is None and new.ncic_code is not None:
        ncic_description = ncic.get_description(new.ncic_code)
        if ncic_description:
            new.description = normalize(ncic_description)

    new.attempted = fn(parse_bool, "attempted", proto)
    if new.classification_type is None:
        new.classification_type = StateChargeClassificationType.find_in_string(
            new.description
        )
    new.classification_subtype = fn(normalize, "classification_subtype", proto)
    new.offense_type = fn(normalize, "offense_type", proto)
    new.is_violent = fn(parse_bool, "is_violent", proto)
    new.is_sex_offense = fn(parse_bool, "is_sex_offense", proto)
    new.counts = fn(parse_int, "counts", proto)
    new.charge_notes = fn(normalize, "charge_notes", proto)
    new.is_controlling = fn(parse_bool, "is_controlling", proto)
    new.charging_entity = fn(normalize, "charging_entity", proto)
