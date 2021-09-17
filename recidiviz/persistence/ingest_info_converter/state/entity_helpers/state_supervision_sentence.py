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
from recidiviz.common import common_utils
from recidiviz.common.constants.defaulting_and_normalizing_enum_parser import (
    DefaultingAndNormalizingEnumParser,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateSupervisionSentence
from recidiviz.persistence.entity.state import entities


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

    # Enum mappings
    new.status = DefaultingAndNormalizingEnumParser(
        getattr(proto, "status"),
        StateSentenceStatus,
        metadata.enum_overrides,
    )
    new.status_raw_text = getattr(proto, "status")
    new.supervision_type = DefaultingAndNormalizingEnumParser(
        getattr(proto, "supervision_type"),
        StateSupervisionType,
        metadata.enum_overrides,
    )
    new.supervision_type_raw_text = getattr(proto, "supervision_type")

    # 1-to-1 mappings

    state_supervision_sentence_id = getattr(proto, "state_supervision_sentence_id")
    new.external_id = (
        None
        if common_utils.is_generated_id(state_supervision_sentence_id)
        else state_supervision_sentence_id
    )
    new.date_imposed = getattr(proto, "date_imposed")
    new.start_date = getattr(proto, "start_date")
    new.completion_date = getattr(proto, "completion_date")
    new.projected_completion_date = getattr(proto, "projected_completion_date")
    new.state_code = metadata.region
    new.county_code = getattr(proto, "county_code")
    new.min_length_days = getattr(proto, "min_length")
    new.max_length_days = getattr(proto, "max_length")
