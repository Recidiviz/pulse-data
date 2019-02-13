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
from recidiviz.common.constants.charge import ChargeDegree, ChargeClass, \
    ChargeStatus, CourtType
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence import entities
from recidiviz.persistence.converter.converter_utils import normalize, fn, \
    parse_date, parse_bool, parse_dollars, parse_external_id


def copy_fields_to_builder(
        new: entities.Charge.Builder, proto, metadata: IngestMetadata) -> None:
    """Mutates the provided |booking_builder| by converting an ingest_info proto
     Booking.

     Note: This will not copy children into the Builder!
     """
    new.external_id = fn(parse_external_id, 'charge_id', proto)
    new.offense_date = fn(parse_date, 'offense_date', proto)
    new.statute = fn(normalize, 'statute', proto)
    new.name = charge_name = fn(normalize, 'name', proto)
    new.attempted = fn(parse_bool, 'attempted', proto)
    new.degree = fn(ChargeDegree.parse, 'degree', proto,
                    metadata.enum_overrides)
    new.degree_raw_text = fn(normalize, 'degree', proto)

    charge_class_from_name = ChargeClass.find_in_string(charge_name)
    new.charge_class = fn(ChargeClass.parse, 'charge_class', proto,
                          metadata.enum_overrides,
                          default=charge_class_from_name or None)
    new.class_raw_text = fn(normalize, 'charge_class', proto)
    new.level = fn(normalize, 'level', proto)
    new.fee_dollars = fn(parse_dollars, 'fee_dollars', proto)
    new.charging_entity = fn(normalize, 'charging_entity', proto)
    new.status = fn(ChargeStatus.parse, 'status', proto,
                    metadata.enum_overrides,
                    default=ChargeStatus.UNKNOWN_FOUND_IN_SOURCE)
    new.status_raw_text = fn(normalize, 'status', proto)
    new.court_type = fn(CourtType.parse, 'court_type', proto,
                        metadata.enum_overrides)
    new.court_type_raw_text = fn(normalize, 'court_type', proto)
    new.case_number = fn(normalize, 'case_number', proto)
    new.next_court_date = fn(parse_date, 'next_court_date', proto)
    new.judge_name = fn(normalize, 'judge_name', proto)
    new.charge_notes = fn(normalize, 'charge_notes', proto)
