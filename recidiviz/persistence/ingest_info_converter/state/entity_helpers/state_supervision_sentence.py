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

"""Converts an ingest_info proto StateSupervisionSentence to a
persistence entity."""
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.common.str_field_utils import normalize, parse_date, parse_days
from recidiviz.ingest.models.ingest_info_pb2 import StateSupervisionSentence
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    fn,
    parse_completion_date,
    parse_external_id,
    parse_region_code_with_override,
)
from recidiviz.persistence.ingest_info_converter.utils.ingest_info_proto_enum_mapper import (
    IngestInfoProtoEnumMapper,
)


# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
def copy_fields_to_builder(
    supervision_sentence_builder: entities.StateSupervisionSentence.Builder,
    proto: StateSupervisionSentence,
    metadata: IngestMetadata,
) -> None:
    """Mutates the provided |supervision_sentence_builder| by converting an
    ingest_info proto StateSupervisionSentence.

    Note: This will not copy children into the Builder!
    """
    new = supervision_sentence_builder

    enum_fields = {
        "status": StateSentenceStatus,
        "supervision_type": StateSupervisionType,
    }
    proto_enum_mapper = IngestInfoProtoEnumMapper(
        proto, enum_fields, metadata.enum_overrides
    )

    # Enum mappings
    new.status = proto_enum_mapper.get(
        StateSentenceStatus, default=StateSentenceStatus.PRESENT_WITHOUT_INFO
    )
    new.status_raw_text = fn(normalize, "status", proto)
    new.supervision_type = proto_enum_mapper.get(StateSupervisionType)
    new.supervision_type_raw_text = fn(normalize, "supervision_type", proto)

    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, "state_supervision_sentence_id", proto)
    new.date_imposed = fn(parse_date, "date_imposed", proto)
    new.start_date = fn(parse_date, "start_date", proto)
    new.completion_date, new.projected_completion_date = parse_completion_date(
        proto, metadata
    )
    new.state_code = parse_region_code_with_override(proto, "state_code", metadata)
    new.county_code = fn(normalize, "county_code", proto)
    new.min_length_days = fn(parse_days, "min_length", proto)
    new.max_length_days = fn(parse_days, "max_length", proto)

    set_status_if_needed(new)


def set_status_if_needed(builder):
    # completion_date is guaranteed to be in the past by parse_completion_date
    if (
        builder.completion_date
        and builder.status is StateSentenceStatus.PRESENT_WITHOUT_INFO
    ):
        builder.status = StateSentenceStatus.COMPLETED
