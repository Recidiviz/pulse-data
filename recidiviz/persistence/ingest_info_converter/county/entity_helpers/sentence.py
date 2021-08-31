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
"""Converts an ingest_info proto Sentence to a persistence entity."""

from recidiviz.common.constants.county.sentence import SentenceStatus
from recidiviz.common.str_field_utils import (
    normalize,
    parse_bool,
    parse_date,
    parse_days,
    parse_dollars,
)
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    fn,
    parse_completion_date,
    parse_external_id,
)
from recidiviz.persistence.ingest_info_converter.utils.ingest_info_proto_enum_mapper import (
    IngestInfoProtoEnumMapper,
)


def copy_fields_to_builder(sentence_builder, proto, metadata) -> None:
    """Mutates the provided |sentence_builder| by converting an ingest_info
    proto Sentence.

    Note: This will not copy children into the Builder!
    """
    new = sentence_builder

    enum_fields = {
        "status": SentenceStatus,
    }
    proto_enum_mapper = IngestInfoProtoEnumMapper(
        proto, enum_fields, metadata.enum_overrides
    )

    # Enum mappings
    new.status = proto_enum_mapper.get(
        SentenceStatus, default=SentenceStatus.PRESENT_WITHOUT_INFO
    )
    new.status_raw_text = fn(normalize, "status", proto)

    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, "sentence_id", proto)
    new.sentencing_region = fn(normalize, "sentencing_region", proto)
    new.min_length_days = fn(parse_days, "min_length", proto)
    new.max_length_days = fn(parse_days, "max_length", proto)
    new.is_life = fn(parse_bool, "is_life", proto)
    new.is_probation = fn(parse_bool, "is_probation", proto)
    new.is_suspended = fn(parse_bool, "is_suspended", proto)
    new.fine_dollars = fn(parse_dollars, "fine_dollars", proto)
    new.parole_possible = fn(parse_bool, "parole_possible", proto)
    new.post_release_supervision_length_days = fn(
        parse_days, "post_release_supervision_length", proto
    )
    new.date_imposed = fn(parse_date, "date_imposed", proto)
    new.completion_date, new.projected_completion_date = parse_completion_date(
        proto, metadata
    )

    set_status_if_needed(new)


def set_status_if_needed(builder):
    # completion_date is guaranteed to be in the past by parse_completion_date
    if (
        builder.completion_date
        and builder.status is SentenceStatus.PRESENT_WITHOUT_INFO
    ):
        builder.status = SentenceStatus.COMPLETED
