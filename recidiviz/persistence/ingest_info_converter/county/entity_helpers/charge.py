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
"""Converts an ingest_info proto Charge to a persistence entity."""
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.county.charge import ChargeClass, ChargeDegree
from recidiviz.common.ingest_metadata import LegacyStateAndJailsIngestMetadata
from recidiviz.common.str_field_utils import (
    normalize,
    parse_bool,
    parse_date,
    parse_dollars,
)
from recidiviz.persistence.entity.county import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    fn,
    parse_external_id,
)
from recidiviz.persistence.ingest_info_converter.utils.ingest_info_proto_enum_mapper import (
    IngestInfoProtoEnumMapper,
)


def copy_fields_to_builder(
    new: entities.Charge.Builder, proto, metadata: LegacyStateAndJailsIngestMetadata
) -> None:
    """Mutates the provided |charge_builder| by converting an ingest_info proto
    Charge.

    Note: This will not copy children into the Builder!
    """

    enum_fields = {
        "degree": ChargeDegree,
        "charge_class": ChargeClass,
        "status": ChargeStatus,
    }
    proto_enum_mapper = IngestInfoProtoEnumMapper(
        proto, enum_fields, metadata.enum_overrides
    )

    # Enum values
    new.degree = proto_enum_mapper.get(ChargeDegree)
    new.degree_raw_text = fn(normalize, "degree", proto)
    new.charge_class = proto_enum_mapper.get(ChargeClass)
    new.class_raw_text = fn(normalize, "charge_class", proto)
    new.status = proto_enum_mapper.get(
        ChargeStatus, default=ChargeStatus.PRESENT_WITHOUT_INFO
    )
    new.status_raw_text = fn(normalize, "status", proto)

    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, "charge_id", proto)
    new.offense_date = fn(parse_date, "offense_date", proto)
    new.statute = fn(normalize, "statute", proto)
    new.name = fn(normalize, "name", proto)
    if new.charge_class is None:
        new.charge_class = ChargeClass.find_in_string(new.name)
    new.attempted = fn(parse_bool, "attempted", proto)
    new.level = fn(normalize, "level", proto)
    new.fee_dollars = fn(parse_dollars, "fee_dollars", proto)
    new.charging_entity = fn(normalize, "charging_entity", proto)
    new.case_number = fn(normalize, "case_number", proto)
    new.court_type = fn(normalize, "court_type", proto)
    new.next_court_date = fn(parse_date, "next_court_date", proto)
    new.judge_name = fn(normalize, "judge_name", proto)
    new.charge_notes = fn(normalize, "charge_notes", proto)
