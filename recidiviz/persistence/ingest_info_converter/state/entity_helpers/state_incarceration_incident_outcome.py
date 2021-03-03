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
"""Converts an ingest_info proto StateIncarcerationIncidentOutcome to a
persistence entity.
"""

from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentOutcomeType,
)
from recidiviz.common.str_field_utils import normalize, parse_int, parse_date
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateIncarcerationIncidentOutcome
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    fn,
    parse_external_id,
    parse_region_code_with_override,
)
from recidiviz.persistence.ingest_info_converter.utils.enum_mappings import EnumMappings


def convert(
    proto: StateIncarcerationIncidentOutcome, metadata: IngestMetadata
) -> entities.StateIncarcerationIncidentOutcome:
    """Converts an ingest_info proto StateIncarcerationIncidentOutcome to a
    persistence entity.
    """
    new = entities.StateIncarcerationIncidentOutcome.builder()

    enum_fields = {
        "outcome_type": StateIncarcerationIncidentOutcomeType,
    }
    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    # enum values
    new.outcome_type = enum_mappings.get(StateIncarcerationIncidentOutcomeType)
    new.outcome_type_raw_text = fn(normalize, "outcome_type", proto)

    # 1-to-1 mappings
    new.external_id = fn(
        parse_external_id, "state_incarceration_incident_outcome_id", proto
    )
    new.date_effective = fn(parse_date, "date_effective", proto)
    new.hearing_date = fn(parse_date, "hearing_date", proto)
    new.report_date = fn(parse_date, "report_date", proto)
    new.state_code = parse_region_code_with_override(proto, "state_code", metadata)
    new.outcome_description = fn(normalize, "outcome_description", proto)
    new.punishment_length_days = fn(parse_int, "punishment_length_days", proto)

    return new.build()
