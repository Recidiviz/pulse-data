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

"""Converts an ingest_info proto StateCourtCase to a persistence entity."""

from recidiviz.common.constants.state.state_court_case import (
    StateCourtCaseStatus,
    StateCourtType,
)
from recidiviz.common.str_field_utils import normalize, parse_date, parse_dollars
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateCourtCase
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    fn,
    parse_external_id,
    parse_region_code_with_override,
)
from recidiviz.persistence.ingest_info_converter.utils.enum_mappings import EnumMappings


def copy_fields_to_builder(
    court_case_builder: entities.StateCourtCase.Builder,
    proto: StateCourtCase,
    metadata: IngestMetadata,
) -> None:

    """Mutates the provided |court_case_builder| by converting an ingest_info
    proto StateCourtCase.

    Note: This will not copy children into the Builder!
    """
    new = court_case_builder

    enum_fields = {
        "status": StateCourtCaseStatus,
        "court_type": StateCourtType,
    }
    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    # enum values
    new.status = enum_mappings.get(
        StateCourtCaseStatus, default=StateCourtCaseStatus.PRESENT_WITHOUT_INFO
    )
    new.status_raw_text = fn(normalize, "status", proto)

    new.court_type = enum_mappings.get(
        StateCourtType, default=StateCourtType.PRESENT_WITHOUT_INFO
    )
    new.court_type_raw_text = fn(normalize, "court_type", proto)

    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, "state_court_case_id", proto)
    new.date_convicted = fn(parse_date, "date_convicted", proto)
    new.next_court_date = fn(parse_date, "next_court_date", proto)
    new.state_code = parse_region_code_with_override(proto, "state_code", metadata)
    new.county_code = fn(normalize, "county_code", proto)
    new.judicial_district_code = fn(normalize, "judicial_district_code", proto)
    new.court_fee_dollars = fn(parse_dollars, "court_fee_dollars", proto)
