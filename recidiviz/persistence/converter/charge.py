# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
"""Converts an ingest_info proto Arrest to a persistence entity."""
from recidiviz.common.common_utils import normalize
from recidiviz.common.constants.charge import (ChargeClass, ChargeDegree,
                                               ChargeStatus, CourtType)
from recidiviz.common.date import parse_date
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence import entities
from recidiviz.persistence.converter.converter_utils import (
    fn, parse_bool, parse_dollars, parse_external_id)
from recidiviz.persistence.converter.enum_mappings import EnumMappings


def copy_fields_to_builder(
        new: entities.Charge.Builder, proto, metadata: IngestMetadata) -> None:
    """Mutates the provided |booking_builder| by converting an ingest_info proto
     Booking.

     Note: This will not copy children into the Builder!
     """

    enum_fields = {
        'degree': ChargeDegree.parse,
        'charge_class': ChargeClass.parse,
        'status': ChargeStatus.parse,
        'court_type': CourtType.parse
    }
    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    # Enum values
    new.degree = enum_mappings.get(ChargeDegree)
    new.degree_raw_text = fn(normalize, 'degree', proto)
    new.charge_class = enum_mappings.get(ChargeClass)
    new.class_raw_text = fn(normalize, 'charge_class', proto)
    new.status = enum_mappings.get(ChargeStatus,
                                   default=ChargeStatus.PRESENT_WITHOUT_INFO)
    new.status_raw_text = fn(normalize, 'status', proto)
    new.court_type = enum_mappings.get(CourtType)
    new.court_type_raw_text = fn(normalize, 'court_type', proto)

    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, 'charge_id', proto)
    new.offense_date = fn(parse_date, 'offense_date', proto)
    new.statute = fn(normalize, 'statute', proto)
    new.name = fn(normalize, 'name', proto)
    if new.charge_class is None:
        new.charge_class = ChargeClass.find_in_string(new.name)
    new.attempted = fn(parse_bool, 'attempted', proto)
    new.level = fn(normalize, 'level', proto)
    new.fee_dollars = fn(parse_dollars, 'fee_dollars', proto)
    new.charging_entity = fn(normalize, 'charging_entity', proto)
    new.case_number = fn(normalize, 'case_number', proto)
    new.next_court_date = fn(parse_date, 'next_court_date', proto)
    new.judge_name = fn(normalize, 'judge_name', proto)
    new.charge_notes = fn(normalize, 'charge_notes', proto)
