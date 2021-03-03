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

"""Converts an ingest_info proto StateIncarcerationSentence to a
persistence entity."""

from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateIncarcerationSentence
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    fn,
    parse_external_id,
    parse_region_code_with_override,
)
from recidiviz.common.str_field_utils import (
    parse_days,
    normalize,
    parse_date,
    parse_bool,
)
from recidiviz.persistence.ingest_info_converter.utils.enum_mappings import EnumMappings


def copy_fields_to_builder(
    incarceration_sentence_builder: entities.StateIncarcerationSentence.Builder,
    proto: StateIncarcerationSentence,
    metadata: IngestMetadata,
) -> None:
    """Mutates the provided |incarceration_sentence_builder| by converting an
    ingest_info proto StateIncarcerationSentence.

    Note: This will not copy children into the Builder!
    """
    new = incarceration_sentence_builder

    enum_fields = {
        "status": StateSentenceStatus,
        "incarceration_type": StateIncarcerationType,
    }
    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    # Enum mappings
    new.status = enum_mappings.get(
        StateSentenceStatus, default=StateSentenceStatus.PRESENT_WITHOUT_INFO
    )
    new.status_raw_text = fn(normalize, "status", proto)

    new.incarceration_type = enum_mappings.get(
        StateIncarcerationType, default=StateIncarcerationType.STATE_PRISON
    )
    new.incarceration_type_raw_text = fn(normalize, "incarceration_type", proto)

    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, "state_incarceration_sentence_id", proto)
    new.date_imposed = fn(parse_date, "date_imposed", proto)
    new.start_date = fn(parse_date, "start_date", proto)
    new.projected_min_release_date = fn(parse_date, "projected_min_release_date", proto)
    new.projected_max_release_date = fn(parse_date, "projected_max_release_date", proto)
    new.completion_date = fn(parse_date, "completion_date", proto)
    new.parole_eligibility_date = fn(parse_date, "parole_eligibility_date", proto)
    new.state_code = parse_region_code_with_override(proto, "state_code", metadata)
    new.county_code = fn(normalize, "county_code", proto)
    new.min_length_days = fn(parse_days, "min_length", proto)
    new.max_length_days = fn(parse_days, "max_length", proto)
    new.is_life = fn(parse_bool, "is_life", proto)
    new.is_capital_punishment = fn(parse_bool, "is_capital_punishment", proto)
    new.parole_possible = fn(parse_bool, "parole_possible", proto)
    new.initial_time_served_days = fn(parse_days, "initial_time_served", proto)
    new.good_time_days = fn(parse_days, "good_time", proto)
    new.earned_time_days = fn(parse_days, "earned_time", proto)
