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

from recidiviz.common.constants.state.state_court_case import \
    StateCourtCaseStatus
from recidiviz.common.str_field_utils import normalize, parse_date, \
    parse_dollars
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.entities import StateCourtCase
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    fn,
    parse_external_id,
    parse_region_code_with_override)
from recidiviz.persistence.ingest_info_converter.utils.enum_mappings \
    import EnumMappings


def convert(proto: StateCourtCase, metadata: IngestMetadata) \
        -> entities.StateCourtCase:
    """Converts an ingest_info proto StateCourtCase to a persistence entity."""
    new = entities.StateCourtCase.builder()

    enum_fields = {
        'status': StateCourtCaseStatus,
    }
    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    # enum values
    new.status = enum_mappings.get(StateCourtCaseStatus)
    new.status_raw_text = fn(normalize, 'status', proto)

    # TODO(1625) - Bring back once we know values for court type
    # new.court_type = enum_mappings.get(StateCourtType)
    # new.court_type_raw_text = fn(normalize, 'court_type', proto)

    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, 'state_court_case_id', proto)
    new.date_convicted = fn(parse_date, 'date_convicted', proto)
    new.next_court_date = fn(parse_date, 'next_court_date', proto)
    new.state_code = parse_region_code_with_override(
        proto, 'state_code', metadata)
    new.county_code = fn(normalize, 'county_code', proto)
    new.court_fee_dollars = fn(parse_dollars, 'court_fee_dollars', proto)
    new.judge_name = fn(normalize, 'judge_name', proto)

    return new.build()
